package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/LegendaryPenguin/cairn/raft"
)

func entry(i raft.Index, term raft.Term) raft.LogEntry {
	return raft.LogEntry{
		Index: i,
		Term:  term,
		Kind:  raft.EntryCommand,
		Data:  []byte(fmt.Sprintf("cmd-%d-%d", i, term)),
	}
}

func mustOpen(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	return s
}

func mustAppend(t *testing.T, s *Store, entries ...raft.LogEntry) {
	t.Helper()
	if err := s.Append(entries); err != nil {
		t.Fatalf("Append(%v): %v", entries, err)
	}
}

func wantEntries(t *testing.T, s *Store, lo, hi raft.Index, want []raft.LogEntry) {
	t.Helper()
	got, err := s.Entries(lo, hi)
	if err != nil {
		t.Fatalf("Entries(%d, %d): %v", lo, hi, err)
	}
	if len(got) != len(want) {
		t.Fatalf("Entries(%d, %d) returned %d entries, want %d", lo, hi, len(got), len(want))
	}
	for i := range got {
		if got[i].Index != want[i].Index || got[i].Term != want[i].Term || got[i].Kind != want[i].Kind || !bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("entry %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestEmptyStore(t *testing.T) {
	s := mustOpen(t, t.TempDir())
	defer s.Close()

	if got := s.FirstIndex(); got != 1 {
		t.Errorf("FirstIndex = %d, want 1", got)
	}
	if got := s.LastIndex(); got != 0 {
		t.Errorf("LastIndex = %d, want 0", got)
	}
	term, vote, err := s.LoadState()
	if err != nil || term != 0 || vote != "" {
		t.Errorf("LoadState = (%d, %q, %v), want (0, \"\", nil)", term, vote, err)
	}
	snap, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap.Meta.Index != 0 {
		t.Errorf("LoadSnapshot index = %d, want 0", snap.Meta.Index)
	}
	if _, err := s.Term(5); !errors.Is(err, raft.ErrUnavailable) {
		t.Errorf("Term(5) err = %v, want ErrUnavailable", err)
	}
}

func TestAppendReopenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	var want []raft.LogEntry
	for i := raft.Index(1); i <= 7; i++ {
		want = append(want, entry(i, raft.Term(1+i/3)))
	}

	s := mustOpen(t, dir)
	mustAppend(t, s, want[:3]...)
	mustAppend(t, s, want[3:]...)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := mustOpen(t, dir)
	defer s2.Close()
	if got := s2.TruncatedBytes(); got != 0 {
		t.Errorf("TruncatedBytes after clean close = %d, want 0", got)
	}
	if got := s2.LastIndex(); got != 7 {
		t.Errorf("LastIndex = %d, want 7", got)
	}
	wantEntries(t, s2, 1, 8, want)
	for _, e := range want {
		got, err := s2.Term(e.Index)
		if err != nil || got != e.Term {
			t.Errorf("Term(%d) = (%d, %v), want (%d, nil)", e.Index, got, err, e.Term)
		}
	}
}

func TestEmptyPayloadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	mustAppend(t, s, raft.LogEntry{Index: 1, Term: 4, Kind: raft.EntryNoop})
	s.Close()

	s2 := mustOpen(t, dir)
	defer s2.Close()
	got, err := s2.Entries(1, 2)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(got) != 1 || got[0].Kind != raft.EntryNoop || len(got[0].Data) != 0 {
		t.Fatalf("got %+v, want a single zero-length noop", got)
	}
}

func TestStatePersists(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	if err := s.SaveState(9, "node-b"); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	term, vote, _ := s.LoadState()
	if term != 9 || vote != "node-b" {
		t.Fatalf("LoadState before reopen = (%d, %q)", term, vote)
	}
	s.Close()

	s2 := mustOpen(t, dir)
	defer s2.Close()
	term, vote, err := s2.LoadState()
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if term != 9 || vote != "node-b" {
		t.Fatalf("LoadState after reopen = (%d, %q), want (9, \"node-b\")", term, vote)
	}

	if err := s2.SaveState(10, ""); err != nil {
		t.Fatalf("SaveState with no vote: %v", err)
	}
	term, vote, _ = s2.LoadState()
	if term != 10 || vote != "" {
		t.Fatalf("LoadState = (%d, %q), want (10, \"\")", term, vote)
	}
}

func TestTruncateSuffixThenDivergentAppend(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)

	for i := raft.Index(1); i <= 6; i++ {
		mustAppend(t, s, entry(i, 1))
	}
	// The follower-conflict case: the leader says index 4 onward is wrong.
	if err := s.TruncateSuffix(4); err != nil {
		t.Fatalf("TruncateSuffix: %v", err)
	}
	if got := s.LastIndex(); got != 3 {
		t.Fatalf("LastIndex after truncate = %d, want 3", got)
	}
	if _, err := s.Entries(4, 5); !errors.Is(err, raft.ErrUnavailable) {
		t.Errorf("Entries past truncation point err = %v, want ErrUnavailable", err)
	}

	divergent := []raft.LogEntry{entry(4, 5), entry(5, 5)}
	mustAppend(t, s, divergent...)
	s.Close()

	s2 := mustOpen(t, dir)
	defer s2.Close()
	if got := s2.LastIndex(); got != 5 {
		t.Fatalf("LastIndex after reopen = %d, want 5", got)
	}
	wantEntries(t, s2, 4, 6, divergent)
	if got, _ := s2.Term(4); got != 5 {
		t.Errorf("Term(4) = %d, want 5 (the divergent term)", got)
	}
	if got := s2.TruncatedBytes(); got != 0 {
		t.Errorf("TruncatedBytes = %d; a clean truncate must not look like a torn tail", got)
	}
}

func TestAppendOverwritesExistingTail(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()

	for i := raft.Index(1); i <= 5; i++ {
		mustAppend(t, s, entry(i, 2))
	}
	// A leader resending from an index the follower already has must replace it.
	mustAppend(t, s, entry(3, 7), entry(4, 7))
	if got := s.LastIndex(); got != 4 {
		t.Fatalf("LastIndex = %d, want 4", got)
	}
	if got, _ := s.Term(3); got != 7 {
		t.Errorf("Term(3) = %d, want 7", got)
	}
	if got, _ := s.Term(2); got != 2 {
		t.Errorf("Term(2) = %d, want 2", got)
	}
}

func TestAppendRejectsGapAndCompacted(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()

	mustAppend(t, s, entry(1, 1))
	if err := s.Append([]raft.LogEntry{entry(3, 1)}); err == nil {
		t.Error("Append leaving a gap succeeded, want error")
	}
	if err := s.Append([]raft.LogEntry{entry(2, 1), entry(4, 1)}); err == nil {
		t.Error("Append of a non-contiguous batch succeeded, want error")
	}
	if got := s.LastIndex(); got != 1 {
		t.Fatalf("LastIndex = %d; rejected appends must not have landed", got)
	}

	mustAppend(t, s, entry(2, 1), entry(3, 1))
	if err := s.SaveSnapshot(raft.Snapshot{Meta: raft.SnapshotMeta{Index: 2, Term: 1}, Data: []byte("x")}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := s.Append([]raft.LogEntry{entry(2, 9)}); !errors.Is(err, raft.ErrCompacted) {
		t.Errorf("Append below snapshot err = %v, want ErrCompacted", err)
	}
	if err := s.TruncateSuffix(2); !errors.Is(err, raft.ErrCompacted) {
		t.Errorf("TruncateSuffix below snapshot err = %v, want ErrCompacted", err)
	}
}

func TestSnapshotCompactsAndSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)

	for i := raft.Index(1); i <= 10; i++ {
		mustAppend(t, s, entry(i, raft.Term(1+i/4)))
	}
	snapTerm, err := s.Term(6)
	if err != nil {
		t.Fatalf("Term(6): %v", err)
	}
	payload := bytes.Repeat([]byte("state"), 100)
	if err := s.SaveSnapshot(raft.Snapshot{Meta: raft.SnapshotMeta{Index: 6, Term: snapTerm}, Data: payload}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	check := func(label string, s *Store) {
		if got := s.FirstIndex(); got != 7 {
			t.Errorf("%s: FirstIndex = %d, want 7", label, got)
		}
		if got := s.LastIndex(); got != 10 {
			t.Errorf("%s: LastIndex = %d, want 10", label, got)
		}
		if got, err := s.Term(6); err != nil || got != snapTerm {
			t.Errorf("%s: Term(snapshotIndex) = (%d, %v), want (%d, nil)", label, got, err, snapTerm)
		}
		if _, err := s.Term(5); !errors.Is(err, raft.ErrCompacted) {
			t.Errorf("%s: Term(5) err = %v, want ErrCompacted", label, err)
		}
		if _, err := s.Entries(6, 8); !errors.Is(err, raft.ErrCompacted) {
			t.Errorf("%s: Entries(6, 8) err = %v, want ErrCompacted", label, err)
		}
		if _, err := s.Entries(5, 8); !errors.Is(err, raft.ErrCompacted) {
			t.Errorf("%s: Entries(5, 8) err = %v, want ErrCompacted", label, err)
		}
		wantEntries(t, s, 7, 11, []raft.LogEntry{entry(7, 2), entry(8, 3), entry(9, 3), entry(10, 3)})
		snap, err := s.LoadSnapshot()
		if err != nil {
			t.Fatalf("%s: LoadSnapshot: %v", label, err)
		}
		if snap.Meta.Index != 6 || snap.Meta.Term != snapTerm || !bytes.Equal(snap.Data, payload) {
			t.Errorf("%s: LoadSnapshot = %+v with %d data bytes", label, snap.Meta, len(snap.Data))
		}
	}

	check("before reopen", s)
	s.Close()
	s2 := mustOpen(t, dir)
	defer s2.Close()
	check("after reopen", s2)
}

func TestSnapshotAheadOfLogDropsEverything(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)

	for i := raft.Index(1); i <= 4; i++ {
		mustAppend(t, s, entry(i, 1))
	}
	// InstallSnapshot from a leader that is far ahead.
	if err := s.SaveSnapshot(raft.Snapshot{Meta: raft.SnapshotMeta{Index: 40, Term: 9}, Data: []byte("far")}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if got, want := s.FirstIndex(), raft.Index(41); got != want {
		t.Errorf("FirstIndex = %d, want %d", got, want)
	}
	if got, want := s.LastIndex(), raft.Index(40); got != want {
		t.Errorf("LastIndex = %d, want %d", got, want)
	}
	if got, err := s.Term(40); err != nil || got != 9 {
		t.Errorf("Term(40) = (%d, %v), want (9, nil)", got, err)
	}
	mustAppend(t, s, entry(41, 9))
	s.Close()

	s2 := mustOpen(t, dir)
	defer s2.Close()
	if got := s2.LastIndex(); got != 41 {
		t.Errorf("LastIndex after reopen = %d, want 41", got)
	}
	wantEntries(t, s2, 41, 42, []raft.LogEntry{entry(41, 9)})
}

func TestOlderSnapshotRejectedAndOldFilesRemoved(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()

	for i := raft.Index(1); i <= 8; i++ {
		mustAppend(t, s, entry(i, 1))
	}
	for _, idx := range []raft.Index{3, 6} {
		if err := s.SaveSnapshot(raft.Snapshot{Meta: raft.SnapshotMeta{Index: idx, Term: 1}, Data: []byte{byte(idx)}}); err != nil {
			t.Fatalf("SaveSnapshot(%d): %v", idx, err)
		}
	}
	if err := s.SaveSnapshot(raft.Snapshot{Meta: raft.SnapshotMeta{Index: 4, Term: 1}}); err == nil {
		t.Error("SaveSnapshot going backwards succeeded, want error")
	}

	des, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var snaps []string
	for _, de := range des {
		if _, ok := parseSnapName(de.Name()); ok {
			snaps = append(snaps, de.Name())
		}
	}
	if len(snaps) != 1 {
		t.Errorf("snapshot files on disk = %v, want exactly the newest", snaps)
	}
}

func TestClosedStoreRejectsWrites(t *testing.T) {
	s := mustOpen(t, t.TempDir())
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if err := s.Append([]raft.LogEntry{entry(1, 1)}); !errors.Is(err, raft.ErrShutdown) {
		t.Errorf("Append after Close = %v, want ErrShutdown", err)
	}
	if err := s.SaveState(1, "a"); !errors.Is(err, raft.ErrShutdown) {
		t.Errorf("SaveState after Close = %v, want ErrShutdown", err)
	}
}

func TestConcurrentReadersAndWriter(t *testing.T) {
	s := mustOpen(t, t.TempDir())
	defer s.Close()
	mustAppend(t, s, entry(1, 1))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				last := s.LastIndex()
				if _, err := s.Entries(s.FirstIndex(), last+1); err != nil && !errors.Is(err, raft.ErrUnavailable) && !errors.Is(err, raft.ErrCompacted) {
					t.Errorf("Entries: %v", err)
					return
				}
				if _, err := s.Term(last); err != nil && !errors.Is(err, raft.ErrUnavailable) && !errors.Is(err, raft.ErrCompacted) {
					t.Errorf("Term: %v", err)
					return
				}
			}
		}()
	}
	for i := raft.Index(2); i <= 200; i++ {
		mustAppend(t, s, entry(i, 1))
		if i%50 == 0 {
			if err := s.SaveState(raft.Term(i), "node-a"); err != nil {
				t.Fatalf("SaveState: %v", err)
			}
		}
	}
	close(stop)
	wg.Wait()
}

// refModel is the obvious in-memory implementation the real Store is compared
// against under random operation sequences.
type refModel struct {
	ents      []raft.LogEntry
	snapIndex raft.Index
	snapTerm  raft.Term
}

func (m *refModel) first() raft.Index { return m.snapIndex + 1 }

func (m *refModel) last() raft.Index {
	if len(m.ents) == 0 {
		return m.snapIndex
	}
	return m.ents[len(m.ents)-1].Index
}

func (m *refModel) append(entries []raft.LogEntry) {
	if len(entries) == 0 {
		return
	}
	pos := int(entries[0].Index - m.first())
	m.ents = append(m.ents[:pos:pos], entries...)
}

func (m *refModel) truncate(from raft.Index) {
	if from > m.last() {
		return
	}
	m.ents = m.ents[:from-m.first()]
}

func (m *refModel) snapshot(idx raft.Index, term raft.Term) {
	keep := 0
	for keep < len(m.ents) && m.ents[keep].Index <= idx {
		keep++
	}
	m.ents = m.ents[keep:]
	m.snapIndex, m.snapTerm = idx, term
}

func TestRandomizedAgainstModel(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer func() { s.Close() }()

	m := &refModel{}
	rng := rand.New(rand.NewSource(20251004))

	for step := 0; step < 400; step++ {
		switch n := rng.Intn(100); {
		case n < 55: // append a batch
			count := 1 + rng.Intn(4)
			start := m.last() + 1
			if m.last() > m.first() && rng.Intn(4) == 0 {
				// Sometimes rewrite from inside the existing log.
				start = m.first() + raft.Index(rng.Intn(int(m.last()-m.first()+1)))
			}
			batch := make([]raft.LogEntry, count)
			for i := range batch {
				batch[i] = entry(start+raft.Index(i), raft.Term(step/10+1))
			}
			if err := s.Append(batch); err != nil {
				t.Fatalf("step %d: Append(%d..): %v", step, start, err)
			}
			m.append(batch)

		case n < 70: // truncate a suffix
			if m.last() < m.first() {
				continue
			}
			from := m.first() + raft.Index(rng.Intn(int(m.last()-m.first()+1)))
			if err := s.TruncateSuffix(from); err != nil {
				t.Fatalf("step %d: TruncateSuffix(%d): %v", step, from, err)
			}
			m.truncate(from)

		case n < 80: // snapshot and compact
			if m.last() < m.first() {
				continue
			}
			idx := m.first() + raft.Index(rng.Intn(int(m.last()-m.first()+1)))
			term, err := s.Term(idx)
			if err != nil {
				t.Fatalf("step %d: Term(%d): %v", step, idx, err)
			}
			if err := s.SaveSnapshot(raft.Snapshot{Meta: raft.SnapshotMeta{Index: idx, Term: term}, Data: []byte(fmt.Sprint(step))}); err != nil {
				t.Fatalf("step %d: SaveSnapshot(%d): %v", step, idx, err)
			}
			m.snapshot(idx, term)

		default: // reopen, exercising recovery against the same model
			if err := s.Close(); err != nil {
				t.Fatalf("step %d: Close: %v", step, err)
			}
			s = mustOpen(t, dir)
			if got := s.TruncatedBytes(); got != 0 {
				t.Fatalf("step %d: reopen after clean close truncated %d bytes", step, got)
			}
		}

		if got, want := s.FirstIndex(), m.first(); got != want {
			t.Fatalf("step %d: FirstIndex = %d, want %d", step, got, want)
		}
		if got, want := s.LastIndex(), m.last(); got != want {
			t.Fatalf("step %d: LastIndex = %d, want %d", step, got, want)
		}
		if m.snapIndex > 0 {
			if got, err := s.Term(m.snapIndex); err != nil || got != m.snapTerm {
				t.Fatalf("step %d: Term(snapIndex=%d) = (%d, %v), want (%d, nil)", step, m.snapIndex, got, err, m.snapTerm)
			}
		}
		if m.last() >= m.first() {
			wantEntries(t, s, m.first(), m.last()+1, m.ents)
			for _, e := range m.ents {
				got, err := s.Term(e.Index)
				if err != nil || got != e.Term {
					t.Fatalf("step %d: Term(%d) = (%d, %v), want (%d, nil)", step, e.Index, got, err, e.Term)
				}
			}
		}
	}

	// A final reopen must still agree after the whole sequence.
	if err := s.Close(); err != nil {
		t.Fatalf("final Close: %v", err)
	}
	s = mustOpen(t, dir)
	if got, want := s.LastIndex(), m.last(); got != want {
		t.Fatalf("final LastIndex = %d, want %d", got, want)
	}
	if m.last() >= m.first() {
		wantEntries(t, s, m.first(), m.last()+1, m.ents)
	}
	if _, err := os.Stat(filepath.Join(dir, "wal")); err != nil {
		t.Fatalf("wal file missing: %v", err)
	}
}
