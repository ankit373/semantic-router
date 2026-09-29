// Package configledger records every accepted config mutation as a durable,
// monotonically numbered generation with an activation state.
package configledger

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// State is the activation state of one generation.
type State string

const (
	StatePending    State = "pending"
	StateActive     State = "active"
	StateFailed     State = "failed"
	StateSuperseded State = "superseded"
	StateKnownGood  State = "known_good"
)

// Transition reasons recorded with a state change.
const (
	ReasonSuperseded    = "Superseded"
	ReasonReplaced      = "Replaced"
	ReasonLostOnRestart = "LostOnRestart"
)

// Mutation sources.
const (
	SourceAPI      = "api"
	SourceRollback = "rollback"
)

// DefaultRetain is the number of newest generations every compaction keeps.
const DefaultRetain = 10

var (
	ErrIllegalTransition = errors.New("illegal config ledger transition")
	ErrUnknownGeneration = errors.New("unknown config ledger generation")
	ErrCorruptJournal    = errors.New("corrupt config ledger journal")
	ErrInvalidEntry      = errors.New("invalid config ledger entry")
)

// legalTransitions is the complete state machine; anything absent is rejected.
var legalTransitions = map[State][]State{
	StatePending:   {StateActive, StateFailed, StateSuperseded},
	StateActive:    {StateKnownGood, StateSuperseded},
	StateKnownGood: {StateSuperseded},
}

func canTransition(from, to State) bool {
	for _, allowed := range legalTransitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

func (s State) valid() bool {
	switch s {
	case StatePending, StateActive, StateFailed, StateSuperseded, StateKnownGood:
		return true
	}
	return false
}

// Record is the latest known state of one generation.
type Record struct {
	Generation int64 `json:"generation"`
	// Hash is the canonical config hash; a rollback repeats an older hash.
	Hash string `json:"hash"`
	// RuntimeHash is the exact runtime document hash the Router publishes on activation.
	RuntimeHash string `json:"runtime_hash,omitempty"`
	// Version is the timestamp ID the mutation response returned.
	Version string `json:"version,omitempty"`
	Source  string `json:"source"`
	// RestoresGeneration is the newest earlier generation with the same hash.
	RestoresGeneration int64      `json:"restores_generation,omitempty"`
	State              State      `json:"state"`
	Reason             string     `json:"reason,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
	KnownGoodAt        *time.Time `json:"known_good_at,omitempty"`
}

// Entry describes a persisted document to record as a new pending generation.
type Entry struct {
	Hash        string
	RuntimeHash string
	Version     string
	Source      string
	Document    []byte
}

// Options configures Open.
type Options struct {
	// Retain is the minimum number of newest generations kept; 0 means DefaultRetain.
	Retain int
	// LoadedHash is the canonical hash of the document this process loaded at startup.
	LoadedHash string
	Now        func() time.Time
}

// Ledger is a single-writer, crash-safe journal of config generations.
// Lookups by generation and by hash are O(1); history is bounded by compaction.
type Ledger struct {
	mu     sync.RWMutex
	dir    string
	retain int
	now    func() time.Time

	records      map[int64]*Record
	order        []int64 // ascending; generations are only ever appended
	latestByHash map[string]int64
	last         int64
	active       int64
	knownGood    int64
	dirSynced    bool
}

func (l *Ledger) lookup(generation int64) (*Record, error) {
	record, ok := l.records[generation]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrUnknownGeneration, generation)
	}
	return record, nil
}

// transitioned returns a copy of record in state to, or ErrIllegalTransition.
func (l *Ledger) transitioned(record *Record, to State, reason string, at time.Time) (Record, error) {
	if !canTransition(record.State, to) {
		return Record{}, fmt.Errorf("%w: generation %d %s -> %s", ErrIllegalTransition, record.Generation, record.State, to)
	}
	next := *record
	next.State = to
	next.Reason = reason
	next.UpdatedAt = at
	if to == StateKnownGood {
		stamp := at
		next.KnownGoodAt = &stamp
	}
	return next, nil
}

// apply updates the in-memory index after the journal accepted the records.
func (l *Ledger) apply(records ...Record) {
	for i := range records {
		record := records[i]
		if _, exists := l.records[record.Generation]; !exists {
			l.order = append(l.order, record.Generation)
		}
		l.records[record.Generation] = &record
		if record.Generation > l.last {
			l.last = record.Generation
		}
		if record.Generation >= l.latestByHash[record.Hash] {
			l.latestByHash[record.Hash] = record.Generation
		}
		switch {
		case record.State == StateActive || record.State == StateKnownGood:
			l.active = record.Generation
		case record.Generation == l.active:
			l.active = 0
		}
		if record.KnownGoodAt != nil && record.Generation > l.knownGood {
			l.knownGood = record.Generation
		}
	}
}

// Append writes the snapshot, then journals a new pending generation and
// supersedes older pending ones in the same fsynced write.
func (l *Ledger) Append(entry Entry) (Record, error) {
	if entry.Hash == "" || len(entry.Document) == 0 {
		return Record{}, fmt.Errorf("%w: hash and document are required", ErrInvalidEntry)
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	at := l.now()
	record := Record{
		Generation: l.last + 1, Hash: entry.Hash, RuntimeHash: entry.RuntimeHash,
		Version: entry.Version, Source: entry.Source, State: StatePending,
		CreatedAt: at, UpdatedAt: at,
	}
	if entry.Source == SourceRollback {
		record.RestoresGeneration = l.latestByHash[entry.Hash]
	}
	batch := []Record{record}
	for _, generation := range l.order {
		if existing := l.records[generation]; existing.State == StatePending {
			superseded, err := l.transitioned(existing, StateSuperseded, ReasonSuperseded, at)
			if err != nil {
				return Record{}, err
			}
			batch = append(batch, superseded)
		}
	}
	if err := l.ensureDir(); err != nil {
		return Record{}, err
	}
	if err := l.writeSnapshot(record.Generation, entry.Document); err != nil {
		return Record{}, err
	}
	if err := l.appendJournal(batch); err != nil {
		return Record{}, err
	}
	l.apply(batch...)
	if err := l.compactIfNeeded(); err != nil {
		return record, fmt.Errorf("compact config ledger: %w", err)
	}
	return record, nil
}

// MarkActive records that the Router published generation; the previous
// active generation becomes superseded while the known-good pointer is kept.
func (l *Ledger) MarkActive(generation int64) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	record, err := l.lookup(generation)
	if err != nil {
		return Record{}, err
	}
	at := l.now()
	next, err := l.transitioned(record, StateActive, "", at)
	if err != nil {
		return Record{}, err
	}
	var batch []Record
	if l.active != 0 {
		if l.active > generation {
			return Record{}, fmt.Errorf("%w: generation %d is older than active generation %d", ErrIllegalTransition, generation, l.active)
		}
		previous, err := l.transitioned(l.records[l.active], StateSuperseded, ReasonReplaced, at)
		if err != nil {
			return Record{}, err
		}
		batch = append(batch, previous)
	}
	// The new active line goes last, so a torn batch leaves it pending and recoverable.
	batch = append(batch, next)
	if err := l.appendJournal(batch); err != nil {
		return Record{}, err
	}
	l.apply(batch...)
	return next, nil
}

// MarkFailed records that preparing generation failed.
func (l *Ledger) MarkFailed(generation int64, reason string) (Record, error) {
	return l.transition(generation, StateFailed, reason)
}

// MarkKnownGood promotes the active generation to known-good.
func (l *Ledger) MarkKnownGood(generation int64) (Record, error) {
	return l.transition(generation, StateKnownGood, "")
}

func (l *Ledger) transition(generation int64, to State, reason string) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	record, err := l.lookup(generation)
	if err != nil {
		return Record{}, err
	}
	next, err := l.transitioned(record, to, reason, l.now())
	if err != nil {
		return Record{}, err
	}
	if err := l.appendJournal([]Record{next}); err != nil {
		return Record{}, err
	}
	l.apply(next)
	return next, nil
}

// Get returns the record for generation.
func (l *Ledger) Get(generation int64) (Record, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.copyOf(generation)
}

// LatestByHash returns the newest retained generation with the canonical hash.
func (l *Ledger) LatestByHash(hash string) (Record, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.copyOf(l.latestByHash[hash])
}

// Active returns the generation the Router currently serves, if recorded.
func (l *Ledger) Active() (Record, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.copyOf(l.active)
}

// KnownGood returns the newest generation ever promoted to known-good.
func (l *Ledger) KnownGood() (Record, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.copyOf(l.knownGood)
}

// Records returns every retained record in ascending generation order.
func (l *Ledger) Records() []Record {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]Record, 0, len(l.order))
	for _, generation := range l.order {
		out = append(out, *l.records[generation])
	}
	return out
}

func (l *Ledger) copyOf(generation int64) (Record, bool) {
	record, ok := l.records[generation]
	if !ok {
		return Record{}, false
	}
	return *record, true
}
