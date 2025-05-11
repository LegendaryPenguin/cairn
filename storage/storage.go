package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/LegendaryPenguin/cairn/raft"
)

// Store is a disk-backed raft.Storage.
//
// Entries live in memory as well as on disk. Raft reads the log far more often
// than it writes it — every heartbeat to a lagging follower is a read — and the
// consensus layer calls Term() while holding its own lock, so a read that waits
// on a disk seek would stall the whole node. The log is bounded by snapshotting,
// which is what makes keeping it resident affordable.
type Store struct {
	mu  sync.RWMutex
	dir string
	wal *wal

	// ents holds every entry above the snapshot point, in index order.
	// offsets[i] is where ents[i] starts in the log file.
	ents    []raft.LogEntry
	offsets []int64

	snap raft.SnapshotMeta

	term     raft.Term
	votedFor raft.NodeID

	truncatedBytes int64
	closed         bool
}

var _ raft.Storage = (*Store)(nil)

// Open recovers a Store from dir, creating it if needed.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir}

	term, votedFor, err := loadStateFile(s.statePath())
	if err != nil {
		return nil, err
	}
	s.term, s.votedFor = term, votedFor

	snap, err := loadNewestSnapshot(dir)
	if err != nil {
		return nil, fmt.Errorf("cairn/storage: reading snapshots: %w", err)
	}
	s.snap = snap.Meta

	res, err := replayWAL(s.walPath())
	if err != nil {
		return nil, err
	}
	w, err := openWAL(s.walPath())
	if err != nil {
		return nil, err
	}
	s.wal = w
	s.ents, s.offsets = res.entries, res.offsets
	if err := s.repairTail(res); err != nil {
		s.wal.close()
		return nil, err
	}

	if err := s.reconcileSnapshotLocked(); err != nil {
		s.wal.close()
		return nil, err
	}
	return s, nil
}

// reconcileSnapshotLocked resolves the overlap a crash between SaveSnapshot's
// two durable steps can leave: a snapshot covering indexes the log still holds.
func (s *Store) reconcileSnapshotLocked() error {
	if s.snap.Index == 0 {
		return nil
	}
	// A log that starts above snapIndex+1 has a hole no peer can fill from here,
	// so those entries are unusable and go away. This is not a crash artifact of
	// this implementation; it would take an out-of-order snapshot install to
	// produce, and dropping is the only safe reading.
	if len(s.ents) > 0 && s.ents[0].Index > s.snap.Index+1 {
		s.ents, s.offsets = nil, nil
		return s.wal.truncate(0)
	}
	return s.compactLocked()
}

func (s *Store) walPath() string { return filepath.Join(s.dir, "wal") }

// TruncatedBytes reports how many bytes recovery discarded from the end of the
// log because they were torn or corrupt. Zero after a clean shutdown.
func (s *Store) TruncatedBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.truncatedBytes
}

func (s *Store) firstIndexLocked() raft.Index { return s.snap.Index + 1 }

func (s *Store) lastIndexLocked() raft.Index {
	if len(s.ents) == 0 {
		return s.snap.Index
	}
	return s.ents[len(s.ents)-1].Index
}

func (s *Store) FirstIndex() raft.Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.firstIndexLocked()
}

func (s *Store) LastIndex() raft.Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndexLocked()
}

func (s *Store) Append(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return raft.ErrShutdown
	}

	for i := 1; i < len(entries); i++ {
		if entries[i].Index != entries[i-1].Index+1 {
			return fmt.Errorf("cairn/storage: append batch is not contiguous at %d after %d", entries[i].Index, entries[i-1].Index)
		}
	}

	first := entries[0].Index
	if first <= s.snap.Index {
		return raft.ErrCompacted
	}
	last := s.lastIndexLocked()
	if first > last+1 {
		return fmt.Errorf("cairn/storage: append at %d leaves a gap after %d", first, last)
	}
	if first <= last {
		// Overwriting an existing range: the old tail has to be physically gone
		// before the new one is written, or recovery would read the stale
		// records (they still checksum) as the authoritative log.
		if err := s.truncateSuffixLocked(first); err != nil {
			return err
		}
	}

	offs, err := s.wal.append(entries)
	if err != nil {
		return err
	}
	s.ents = append(s.ents, entries...)
	s.offsets = append(s.offsets, offs...)
	return nil
}

func (s *Store) TruncateSuffix(from raft.Index) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return raft.ErrShutdown
	}
	if from <= s.snap.Index {
		return raft.ErrCompacted
	}
	return s.truncateSuffixLocked(from)
}

func (s *Store) truncateSuffixLocked(from raft.Index) error {
	if from > s.lastIndexLocked() {
		return nil
	}
	pos := int(from - s.firstIndexLocked())
	if err := s.wal.truncate(s.offsets[pos]); err != nil {
		return err
	}
	s.ents = s.ents[:pos]
	s.offsets = s.offsets[:pos]
	return nil
}

// Entries returns [lo, hi). The returned slice is fresh, but entry Data is
// shared with the Store; callers must not write to it.
func (s *Store) Entries(lo, hi raft.Index) ([]raft.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if lo > hi {
		return nil, fmt.Errorf("cairn/storage: inverted range [%d, %d)", lo, hi)
	}
	if lo <= s.snap.Index {
		return nil, raft.ErrCompacted
	}
	if hi > s.lastIndexLocked()+1 {
		return nil, raft.ErrUnavailable
	}
	first := s.firstIndexLocked()
	out := make([]raft.LogEntry, hi-lo)
	copy(out, s.ents[lo-first:hi-first])
	return out, nil
}

func (s *Store) Term(i raft.Index) (raft.Term, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// The snapshot point's term is kept precisely so AppendEntries' consistency
	// check still has an answer for the last index the snapshot absorbed.
	if i == s.snap.Index {
		return s.snap.Term, nil
	}
	if i < s.snap.Index {
		return 0, raft.ErrCompacted
	}
	if i > s.lastIndexLocked() {
		return 0, raft.ErrUnavailable
	}
	return s.ents[i-s.firstIndexLocked()].Term, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.wal.close()
}
