package configledger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// JournalFile is the append-only JSONL journal inside the ledger directory.
	JournalFile    = "ledger.jsonl"
	snapshotPrefix = "snapshot."
	snapshotSuffix = ".yaml"
	tempPrefix     = ".configledger-"
	journalSchema  = 1
)

type journalLine struct {
	Schema int `json:"schema"`
	Record
}

// Open replays the journal, truncating a torn final line. On recovery only the newest
// generation may stay pending, and only if its hash matches opts.LoadedHash.
func Open(dir string, opts Options) (*Ledger, error) {
	l := &Ledger{
		dir:          dir,
		retain:       opts.Retain,
		now:          opts.Now,
		records:      map[int64]*Record{},
		latestByHash: map[string]int64{},
	}
	if l.retain <= 0 {
		l.retain = DefaultRetain
	}
	if l.now == nil {
		l.now = func() time.Time { return time.Now().UTC() }
	}
	lines, err := l.readJournal()
	if err != nil {
		return nil, err
	}
	for _, line := range lines {
		l.apply(line)
	}
	sort.Slice(l.order, func(i, j int) bool { return l.order[i] < l.order[j] })
	if err := l.recover(opts.LoadedHash); err != nil {
		return nil, err
	}
	l.removeOrphans()
	return l, nil
}

// Dir returns the ledger directory.
func (l *Ledger) Dir() string { return l.dir }

// Snapshot returns the exact document bytes recorded for generation.
func (l *Ledger) Snapshot(generation int64) ([]byte, error) {
	l.mu.RLock()
	_, ok := l.records[generation]
	l.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrUnknownGeneration, generation)
	}
	return os.ReadFile(l.snapshotPath(generation))
}

func (l *Ledger) journalPath() string { return filepath.Join(l.dir, JournalFile) }

func (l *Ledger) snapshotPath(generation int64) string {
	return filepath.Join(l.dir, snapshotPrefix+strconv.FormatInt(generation, 10)+snapshotSuffix)
}

func (l *Ledger) readJournal() ([]Record, error) {
	data, err := os.ReadFile(l.journalPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read config ledger journal: %w", err)
	}
	complete := data
	if cut := bytes.LastIndexByte(data, '\n'); cut+1 < len(data) {
		// An unterminated tail is an interrupted append; later appends must not extend it.
		complete = data[:cut+1]
		if err := truncateFile(l.journalPath(), int64(len(complete))); err != nil {
			return nil, fmt.Errorf("truncate torn config ledger journal: %w", err)
		}
	}
	var records []Record
	for number, raw := range bytes.Split(bytes.TrimSuffix(complete, []byte("\n")), []byte("\n")) {
		if len(raw) == 0 {
			continue
		}
		var line journalLine
		if err := json.Unmarshal(raw, &line); err != nil {
			return nil, fmt.Errorf("%w: line %d: %w", ErrCorruptJournal, number+1, err)
		}
		if line.Schema != journalSchema || line.Generation <= 0 || line.Hash == "" || !line.State.valid() {
			return nil, fmt.Errorf("%w: line %d is not a schema %d record", ErrCorruptJournal, number+1, journalSchema)
		}
		records = append(records, line.Record)
	}
	return records, nil
}

// recover repairs states a crash can leave behind and journals the repairs.
func (l *Ledger) recover(loadedHash string) error {
	at := l.now()
	var batch []Record
	for _, generation := range l.order {
		record := l.records[generation]
		switch {
		case record.State == StatePending:
			if generation == l.last && loadedHash != "" && record.Hash == loadedHash {
				continue
			}
			next, err := l.transitioned(record, StateSuperseded, ReasonLostOnRestart, at)
			if err != nil {
				return err
			}
			batch = append(batch, next)
		case (record.State == StateActive || record.State == StateKnownGood) && generation != l.active:
			next, err := l.transitioned(record, StateSuperseded, ReasonReplaced, at)
			if err != nil {
				return err
			}
			batch = append(batch, next)
		}
	}
	if len(batch) == 0 {
		return nil
	}
	if err := l.appendJournal(batch); err != nil {
		return err
	}
	l.apply(batch...)
	return nil
}

func (l *Ledger) ensureDir() error {
	if err := os.MkdirAll(l.dir, 0o700); err != nil {
		return fmt.Errorf("create config ledger directory: %w", err)
	}
	return nil
}

// writeSnapshot publishes a 0600 snapshot via temp file, fsync, and rename.
func (l *Ledger) writeSnapshot(generation int64, document []byte) error {
	if err := writeFileAtomically(l.dir, l.snapshotPath(generation), document); err != nil {
		return fmt.Errorf("write config ledger snapshot %d: %w", generation, err)
	}
	return nil
}

// appendJournal writes the whole batch with one write and one fsync.
func (l *Ledger) appendJournal(records []Record) error {
	var buf bytes.Buffer
	for _, record := range records {
		encoded, err := json.Marshal(journalLine{Schema: journalSchema, Record: record})
		if err != nil {
			return fmt.Errorf("encode config ledger record: %w", err)
		}
		buf.Write(encoded)
		buf.WriteByte('\n')
	}
	if err := l.ensureDir(); err != nil {
		return err
	}
	file, err := os.OpenFile(l.journalPath(), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("open config ledger journal: %w", err)
	}
	_, writeErr := file.Write(buf.Bytes())
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if closeErr := file.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return fmt.Errorf("append config ledger journal: %w", writeErr)
	}
	if !l.dirSynced {
		syncDir(l.dir)
		l.dirSynced = true
	}
	return nil
}

// compactIfNeeded keeps the newest retain generations plus the active and
// known-good pins, and runs only past 2*retain+2 so each O(retain) rewrite is O(1) amortized.
func (l *Ledger) compactIfNeeded() error {
	if len(l.order) <= 2*l.retain+2 {
		return nil
	}
	cutoff := len(l.order) - l.retain
	keep := make([]int64, 0, l.retain+2)
	var pruned []int64
	for index, generation := range l.order {
		if index >= cutoff || generation == l.active || generation == l.knownGood {
			keep = append(keep, generation)
		} else {
			pruned = append(pruned, generation)
		}
	}
	var buf bytes.Buffer
	for _, generation := range keep {
		encoded, err := json.Marshal(journalLine{Schema: journalSchema, Record: *l.records[generation]})
		if err != nil {
			return err
		}
		buf.Write(encoded)
		buf.WriteByte('\n')
	}
	// Rewrite before deleting snapshots, so a crash never leaves a record without one.
	if err := writeFileAtomically(l.dir, l.journalPath(), buf.Bytes()); err != nil {
		return err
	}
	for _, generation := range pruned {
		delete(l.records, generation)
		_ = os.Remove(l.snapshotPath(generation))
	}
	l.order = keep
	l.latestByHash = make(map[string]int64, len(keep))
	for _, generation := range keep {
		l.latestByHash[l.records[generation].Hash] = generation
	}
	return nil
}

// removeOrphans deletes snapshots without a record and abandoned temp files.
func (l *Ledger) removeOrphans() {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			continue
		}
		if strings.HasPrefix(name, tempPrefix) {
			_ = os.Remove(filepath.Join(l.dir, name))
			continue
		}
		if !strings.HasPrefix(name, snapshotPrefix) || !strings.HasSuffix(name, snapshotSuffix) {
			continue
		}
		generation, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(name, snapshotPrefix), snapshotSuffix), 10, 64)
		if err != nil {
			continue
		}
		if _, ok := l.records[generation]; !ok {
			_ = os.Remove(filepath.Join(l.dir, name))
		}
	}
}

func writeFileAtomically(dir, path string, data []byte) error {
	tmp, err := os.CreateTemp(dir, tempPrefix+"*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	// CreateTemp opens the file 0600, so the snapshot is private before any byte lands.
	_, writeErr := tmp.Write(data)
	if writeErr == nil {
		writeErr = tmp.Sync()
	}
	if closeErr := tmp.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr == nil {
		writeErr = os.Rename(tmpPath, path)
	}
	if writeErr != nil {
		_ = os.Remove(tmpPath)
		return writeErr
	}
	syncDir(dir)
	return nil
}

func truncateFile(path string, size int64) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	truncErr := file.Truncate(size)
	if truncErr == nil {
		truncErr = file.Sync()
	}
	if closeErr := file.Close(); truncErr == nil {
		truncErr = closeErr
	}
	return truncErr
}

// syncDir makes a rename durable; best-effort because some filesystems reject directory fsync.
func syncDir(dir string) {
	if handle, err := os.Open(dir); err == nil {
		_ = handle.Sync()
		_ = handle.Close()
	}
}
