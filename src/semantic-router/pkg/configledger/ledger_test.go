package configledger

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func openTestLedger(t *testing.T, dir string, opts Options) *Ledger {
	t.Helper()
	if opts.Now == nil {
		fixed := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
		opts.Now = func() time.Time { return fixed }
	}
	ledger, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return ledger
}

func mustAppend(t *testing.T, ledger *Ledger, hash, source string) Record {
	t.Helper()
	record, err := ledger.Append(Entry{Hash: hash, RuntimeHash: "runtime-" + hash, Source: source, Document: []byte("doc: " + hash + "\n")})
	if err != nil {
		t.Fatalf("Append(%s) error = %v", hash, err)
	}
	return record
}

func mustState(t *testing.T, ledger *Ledger, generation int64, want State, reason string) {
	t.Helper()
	record, ok := ledger.Get(generation)
	if !ok {
		t.Fatalf("generation %d is missing", generation)
	}
	if record.State != want || record.Reason != reason {
		t.Fatalf("generation %d = %s/%q, want %s/%q", generation, record.State, record.Reason, want, reason)
	}
}

func TestTransitionTable(t *testing.T) {
	states := []State{StatePending, StateActive, StateFailed, StateSuperseded, StateKnownGood}
	legal := map[[2]State]bool{
		{StatePending, StateActive}:       true,
		{StatePending, StateFailed}:       true,
		{StatePending, StateSuperseded}:   true,
		{StateActive, StateKnownGood}:     true,
		{StateActive, StateSuperseded}:    true,
		{StateKnownGood, StateSuperseded}: true,
	}
	for _, from := range states {
		for _, to := range states {
			if got, want := canTransition(from, to), legal[[2]State{from, to}]; got != want {
				t.Errorf("canTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestIllegalTransitionsAreRejectedAndNotJournaled(t *testing.T) {
	cases := []struct {
		name  string
		setup func(*Ledger) int64
		act   func(*Ledger, int64) error
	}{
		{
			"pending to known_good", func(l *Ledger) int64 { return mustAppend(t, l, "a", SourceAPI).Generation },
			func(l *Ledger, g int64) error { _, err := l.MarkKnownGood(g); return err },
		},
		{"active to failed", func(l *Ledger) int64 {
			g := mustAppend(t, l, "a", SourceAPI).Generation
			_, _ = l.MarkActive(g)
			return g
		}, func(l *Ledger, g int64) error { _, err := l.MarkFailed(g, "late"); return err }},
		{"active to active", func(l *Ledger) int64 {
			g := mustAppend(t, l, "a", SourceAPI).Generation
			_, _ = l.MarkActive(g)
			return g
		}, func(l *Ledger, g int64) error { _, err := l.MarkActive(g); return err }},
		{"failed to active", func(l *Ledger) int64 {
			g := mustAppend(t, l, "a", SourceAPI).Generation
			_, _ = l.MarkFailed(g, "boom")
			return g
		}, func(l *Ledger, g int64) error { _, err := l.MarkActive(g); return err }},
		{"superseded to active", func(l *Ledger) int64 {
			g := mustAppend(t, l, "a", SourceAPI).Generation
			mustAppend(t, l, "b", SourceAPI)
			return g
		}, func(l *Ledger, g int64) error { _, err := l.MarkActive(g); return err }},
		{"known_good to known_good", func(l *Ledger) int64 {
			g := mustAppend(t, l, "a", SourceAPI).Generation
			_, _ = l.MarkActive(g)
			_, _ = l.MarkKnownGood(g)
			return g
		}, func(l *Ledger, g int64) error { _, err := l.MarkKnownGood(g); return err }},
		{"older pending over newer active", func(l *Ledger) int64 {
			older := mustAppend(t, l, "a", SourceAPI).Generation
			newer := mustAppend(t, l, "b", SourceAPI).Generation
			_, _ = l.MarkActive(newer)
			// Force an ordering the journal can never produce, to exercise the guard.
			l.records[older].State = StatePending
			return older
		}, func(l *Ledger, g int64) error { _, err := l.MarkActive(g); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ledger := openTestLedger(t, dir, Options{})
			generation := tc.setup(ledger)
			before, _ := os.ReadFile(filepath.Join(dir, JournalFile))
			err := tc.act(ledger, generation)
			if !errors.Is(err, ErrIllegalTransition) {
				t.Fatalf("error = %v, want ErrIllegalTransition", err)
			}
			after, _ := os.ReadFile(filepath.Join(dir, JournalFile))
			if !bytes.Equal(before, after) {
				t.Fatal("a rejected transition must not append to the journal")
			}
		})
	}
	ledger := openTestLedger(t, t.TempDir(), Options{})
	if _, err := ledger.MarkActive(42); !errors.Is(err, ErrUnknownGeneration) {
		t.Fatalf("unknown generation error = %v", err)
	}
	if _, err := ledger.Append(Entry{Hash: "a"}); !errors.Is(err, ErrInvalidEntry) {
		t.Fatalf("empty document error = %v", err)
	}
}

func TestLifecycleSupersedesAndKeepsKnownGoodPointer(t *testing.T) {
	ledger := openTestLedger(t, t.TempDir(), Options{})
	first := mustAppend(t, ledger, "a", SourceAPI)
	if first.Generation != 1 || first.State != StatePending {
		t.Fatalf("first record = %+v", first)
	}
	if _, err := ledger.MarkActive(1); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.MarkKnownGood(1); err != nil {
		t.Fatal(err)
	}

	mustAppend(t, ledger, "b", SourceAPI)
	mustAppend(t, ledger, "c", SourceAPI)
	mustState(t, ledger, 2, StateSuperseded, ReasonSuperseded)
	mustState(t, ledger, 1, StateKnownGood, "")

	if _, err := ledger.MarkFailed(3, "model download failed"); err != nil {
		t.Fatal(err)
	}
	if active, _ := ledger.Active(); active.Generation != 1 {
		t.Fatalf("a failed candidate must not displace the active generation, got %+v", active)
	}

	mustAppend(t, ledger, "d", SourceAPI)
	if _, err := ledger.MarkActive(4); err != nil {
		t.Fatal(err)
	}
	mustState(t, ledger, 1, StateSuperseded, ReasonReplaced)
	mustState(t, ledger, 4, StateActive, "")
	knownGood, ok := ledger.KnownGood()
	if !ok || knownGood.Generation != 1 || knownGood.KnownGoodAt == nil {
		t.Fatalf("known-good pointer = %+v, %v; want generation 1 kept after supersede", knownGood, ok)
	}
}

func TestRollbackGetsNewGeneration(t *testing.T) {
	ledger := openTestLedger(t, t.TempDir(), Options{})
	mustAppend(t, ledger, "a", SourceAPI)
	mustAppend(t, ledger, "b", SourceAPI)
	rollback := mustAppend(t, ledger, "a", SourceRollback)
	if rollback.Generation != 3 || rollback.RestoresGeneration != 1 {
		t.Fatalf("rollback record = %+v, want generation 3 restoring 1", rollback)
	}
	if latest, _ := ledger.LatestByHash("a"); latest.Generation != 3 {
		t.Fatalf("LatestByHash(a) = %d, want 3", latest.Generation)
	}
	if again := mustAppend(t, ledger, "a", SourceRollback); again.Generation != 4 || again.RestoresGeneration != 3 {
		t.Fatalf("second rollback = %+v", again)
	}
}

func TestSnapshotsArePrivateAndByteIdentical(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "backups")
	ledger := openTestLedger(t, dir, Options{})
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("Open must not create the directory before the first write")
	}
	document := []byte("# comment kept\nproviders: {api_key: sk-secret}\n")
	record, err := ledger.Append(Entry{Hash: "a", Source: SourceAPI, Document: document})
	if err != nil {
		t.Fatal(err)
	}
	got, err := ledger.Snapshot(record.Generation)
	if err != nil || !bytes.Equal(got, document) {
		t.Fatalf("Snapshot() = %q, %v", got, err)
	}
	for _, name := range []string{JournalFile, "snapshot.1.yaml"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode = %v, want 0600", name, info.Mode().Perm())
		}
	}
	journal, _ := os.ReadFile(filepath.Join(dir, JournalFile))
	if bytes.Contains(journal, []byte("sk-secret")) {
		t.Fatal("the journal must not contain document content")
	}
}

func TestRetentionNeverPrunesActiveOrKnownGood(t *testing.T) {
	dir := t.TempDir()
	ledger := openTestLedger(t, dir, Options{Retain: 2})
	mustAppend(t, ledger, "good", SourceAPI)
	if _, err := ledger.MarkActive(1); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.MarkKnownGood(1); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, ledger, "live", SourceAPI)
	if _, err := ledger.MarkActive(2); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		record := mustAppend(t, ledger, fmt.Sprintf("candidate-%d", i), SourceAPI)
		if _, err := ledger.MarkFailed(record.Generation, "rejected"); err != nil {
			t.Fatal(err)
		}
	}

	records := ledger.Records()
	if len(records) > 2*2+2 {
		t.Fatalf("retention kept %d records, want at most 6", len(records))
	}
	for _, generation := range []int64{1, 2} {
		if _, ok := ledger.Get(generation); !ok {
			t.Fatalf("generation %d was pruned", generation)
		}
		if _, err := ledger.Snapshot(generation); err != nil {
			t.Fatalf("snapshot %d was pruned: %v", generation, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "snapshot.3.yaml")); !os.IsNotExist(err) {
		t.Fatal("an old failed candidate snapshot should be pruned")
	}

	reopened := openTestLedger(t, dir, Options{Retain: 2})
	if !reflect.DeepEqual(reopened.Records(), ledger.Records()) {
		t.Fatalf("compacted journal replays differently:\n%+v\n%+v", reopened.Records(), ledger.Records())
	}
	if active, _ := reopened.Active(); active.Generation != 2 {
		t.Fatalf("active after reopen = %d", active.Generation)
	}
	if knownGood, _ := reopened.KnownGood(); knownGood.Generation != 1 {
		t.Fatalf("known-good after reopen = %d", knownGood.Generation)
	}
	if next := mustAppend(t, reopened, "after", SourceAPI); next.Generation != 23 {
		t.Fatalf("generation after compaction and reopen = %d, want 23", next.Generation)
	}
}

func TestRestartRecoveryResumesOnlyMatchingNewestPending(t *testing.T) {
	for _, tc := range []struct {
		name       string
		loadedHash string
		wantState  State
		wantReason string
	}{
		{"matching document resumes", "b", StatePending, ""},
		{"other document is lost", "a", StateSuperseded, ReasonLostOnRestart},
		{"unknown document is lost", "", StateSuperseded, ReasonLostOnRestart},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ledger := openTestLedger(t, dir, Options{})
			mustAppend(t, ledger, "a", SourceAPI)
			if _, err := ledger.MarkActive(1); err != nil {
				t.Fatal(err)
			}
			mustAppend(t, ledger, "b", SourceAPI)

			reopened := openTestLedger(t, dir, Options{LoadedHash: tc.loadedHash})
			mustState(t, reopened, 2, tc.wantState, tc.wantReason)
			mustState(t, reopened, 1, StateActive, "")
			again := openTestLedger(t, dir, Options{LoadedHash: tc.loadedHash})
			if !reflect.DeepEqual(again.Records(), reopened.Records()) {
				t.Fatal("recovery must be journaled and idempotent")
			}
		})
	}
}

func TestRestartRecoverySupersedesStalePendingFromTornBatch(t *testing.T) {
	dir := t.TempDir()
	ledger := openTestLedger(t, dir, Options{})
	mustAppend(t, ledger, "a", SourceAPI)
	// Simulate a crash that kept the new pending line but lost its supersede line.
	next := Record{Generation: 2, Hash: "b", Source: SourceAPI, State: StatePending}
	if err := ledger.appendJournal([]Record{next}); err != nil {
		t.Fatal(err)
	}
	reopened := openTestLedger(t, dir, Options{LoadedHash: "b"})
	mustState(t, reopened, 1, StateSuperseded, ReasonLostOnRestart)
	mustState(t, reopened, 2, StatePending, "")
}

func TestRecoveryTruncatesTornJournalLine(t *testing.T) {
	dir := t.TempDir()
	ledger := openTestLedger(t, dir, Options{})
	mustAppend(t, ledger, "a", SourceAPI)
	if _, err := ledger.MarkActive(1); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(dir, JournalFile)
	intact, _ := os.ReadFile(journal)
	file, err := os.OpenFile(journal, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(`{"schema":1,"generation":2,"hash":"b","sta`); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()

	reopened := openTestLedger(t, dir, Options{LoadedHash: "a"})
	if records := reopened.Records(); len(records) != 1 || records[0].State != StateActive {
		t.Fatalf("records after torn tail = %+v", records)
	}
	if got, _ := os.ReadFile(journal); !bytes.Equal(got, intact) {
		t.Fatal("the torn tail must be truncated before the next append")
	}
	if record := mustAppend(t, reopened, "b", SourceAPI); record.Generation != 2 {
		t.Fatalf("generation after torn tail = %d", record.Generation)
	}
	if final := openTestLedger(t, dir, Options{LoadedHash: "b"}); len(final.Records()) != 2 {
		t.Fatalf("journal after repair = %+v", final.Records())
	}
}

func TestCorruptCompleteLineIsRejected(t *testing.T) {
	dir := t.TempDir()
	ledger := openTestLedger(t, dir, Options{})
	mustAppend(t, ledger, "a", SourceAPI)
	file, err := os.OpenFile(filepath.Join(dir, JournalFile), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString("not json\n")
	_ = file.Close()
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrCorruptJournal) {
		t.Fatalf("Open() error = %v, want ErrCorruptJournal", err)
	}
}

func TestOpenRemovesOrphanSnapshotsAndTempFiles(t *testing.T) {
	dir := t.TempDir()
	ledger := openTestLedger(t, dir, Options{})
	mustAppend(t, ledger, "a", SourceAPI)
	for _, name := range []string{"snapshot.9.yaml", tempPrefix + "123", "config.20260101-000000.yaml"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	openTestLedger(t, dir, Options{LoadedHash: "a"})
	for name, wantExists := range map[string]bool{
		"snapshot.1.yaml": true, "snapshot.9.yaml": false, tempPrefix + "123": false,
		"config.20260101-000000.yaml": true,
	} {
		_, err := os.Stat(filepath.Join(dir, name))
		if exists := err == nil; exists != wantExists {
			t.Errorf("%s exists = %v, want %v", name, exists, wantExists)
		}
	}
}

func TestConcurrentAppendsAssignUniqueGenerations(t *testing.T) {
	dir := t.TempDir()
	ledger := openTestLedger(t, dir, Options{Retain: 100})
	const writers = 32
	var wg sync.WaitGroup
	generations := make(chan int64, writers)
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			record, err := ledger.Append(Entry{Hash: fmt.Sprintf("h%d", i), Source: SourceAPI, Document: []byte("x")})
			if err != nil {
				errs <- err
				return
			}
			generations <- record.Generation
			_, _ = ledger.MarkActive(record.Generation)
			_, _ = ledger.Active()
			_ = ledger.Records()
		}(i)
	}
	wg.Wait()
	close(generations)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	for generation := range generations {
		if seen[generation] {
			t.Fatalf("generation %d assigned twice", generation)
		}
		seen[generation] = true
	}
	for generation := int64(1); generation <= writers; generation++ {
		if !seen[generation] {
			t.Fatalf("generation %d missing from %v", generation, seen)
		}
	}
	reopened := openTestLedger(t, dir, Options{Retain: 100})
	if !reflect.DeepEqual(reopened.Records(), ledger.Records()) {
		t.Fatal("journal replay differs from the in-memory index after concurrent writes")
	}
	active := 0
	for _, record := range reopened.Records() {
		if record.State == StateActive {
			active++
		}
	}
	if active > 1 {
		t.Fatalf("%d generations are active at once", active)
	}
}
