package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/LegendaryPenguin/cairn/raft"
)

func walFile(dir string) string { return filepath.Join(dir, "wal") }

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return st.Size()
}

// seedLog writes n entries and returns the log file size after each one, so a
// test can cut the file at a known record boundary or inside a known record.
func seedLog(t *testing.T, dir string, n int) []int64 {
	t.Helper()
	s := mustOpen(t, dir)
	sizes := make([]int64, 0, n)
	for i := 1; i <= n; i++ {
		mustAppend(t, s, entry(raft.Index(i), 3))
		sizes = append(sizes, fileSize(t, walFile(dir)))
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return sizes
}

func TestTornTailDropsPartialRecord(t *testing.T) {
	dir := t.TempDir()
	sizes := seedLog(t, dir, 5)

	// Cut three bytes into the fifth record: the length prefix is there but the
	// payload and CRC are not, which is exactly a crash mid-write.
	cut := sizes[3] + 3
	if err := os.Truncate(walFile(dir), cut); err != nil {
		t.Fatal(err)
	}
	lost := sizes[4] - cut

	s := mustOpen(t, dir)
	defer s.Close()
	if got := s.LastIndex(); got != 4 {
		t.Fatalf("LastIndex = %d, want 4: the partial record must be dropped", got)
	}
	if got := s.TruncatedBytes(); got != 3 {
		t.Errorf("TruncatedBytes = %d, want 3 (the partial bytes), lost payload was %d", got, lost)
	}
	if got := fileSize(t, walFile(dir)); got != sizes[3] {
		t.Errorf("log size after recovery = %d, want %d: the file must be cut at the last valid record", got, sizes[3])
	}
	wantEntries(t, s, 1, 5, []raft.LogEntry{entry(1, 3), entry(2, 3), entry(3, 3), entry(4, 3)})

	// The repaired log must accept new writes at the recovered boundary.
	mustAppend(t, s, entry(5, 4))
	s.Close()
	s2 := mustOpen(t, dir)
	defer s2.Close()
	if got := s2.LastIndex(); got != 5 {
		t.Fatalf("LastIndex after re-append = %d, want 5", got)
	}
	if got, _ := s2.Term(5); got != 4 {
		t.Errorf("Term(5) = %d, want 4", got)
	}
	if got := s2.TruncatedBytes(); got != 0 {
		t.Errorf("TruncatedBytes = %d, want 0: the tail was already repaired", got)
	}
}

func TestTruncatedLengthPrefixDropsRecord(t *testing.T) {
	dir := t.TempDir()
	sizes := seedLog(t, dir, 3)

	// Only two bytes of the third record's uint32 length made it to disk.
	if err := os.Truncate(walFile(dir), sizes[1]+2); err != nil {
		t.Fatal(err)
	}
	s := mustOpen(t, dir)
	defer s.Close()
	if got := s.LastIndex(); got != 2 {
		t.Fatalf("LastIndex = %d, want 2", got)
	}
	if got := s.TruncatedBytes(); got != 2 {
		t.Errorf("TruncatedBytes = %d, want 2", got)
	}
}

func TestCorruptCRCStopsReplayThere(t *testing.T) {
	dir := t.TempDir()
	sizes := seedLog(t, dir, 6)

	// Flip a payload byte inside the fourth record. Entries 1..3 precede it and
	// must survive; 4..6 are unreachable because replay cannot step over a
	// record whose length it no longer trusts.
	f, err := os.OpenFile(walFile(dir), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	target := sizes[2] + int64(lenFieldSize+recFixed) + 1
	if _, err := f.ReadAt(b[:], target); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], target); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	s := mustOpen(t, dir)
	defer s.Close()
	if got := s.LastIndex(); got != 3 {
		t.Fatalf("LastIndex = %d, want 3: replay must stop at the bad CRC", got)
	}
	if want := sizes[5] - sizes[2]; s.TruncatedBytes() != want {
		t.Errorf("TruncatedBytes = %d, want %d", s.TruncatedBytes(), want)
	}
	if got := fileSize(t, walFile(dir)); got != sizes[2] {
		t.Errorf("log size = %d, want %d", got, sizes[2])
	}
	wantEntries(t, s, 1, 4, []raft.LogEntry{entry(1, 3), entry(2, 3), entry(3, 3)})
}

func TestCorruptLengthFieldStopsReplay(t *testing.T) {
	dir := t.TempDir()
	sizes := seedLog(t, dir, 4)

	// A length field that claims an absurd size must be rejected rather than
	// used to allocate or to skip ahead into the middle of a later record.
	f, err := os.OpenFile(walFile(dir), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], 1<<30)
	if _, err := f.WriteAt(buf[:], sizes[1]); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s := mustOpen(t, dir)
	defer s.Close()
	if got := s.LastIndex(); got != 2 {
		t.Fatalf("LastIndex = %d, want 2", got)
	}
	if got := fileSize(t, walFile(dir)); got != sizes[1] {
		t.Errorf("log size = %d, want %d", got, sizes[1])
	}
}

func TestNonContiguousIndexStopsReplay(t *testing.T) {
	dir := t.TempDir()
	sizes := seedLog(t, dir, 4)

	// Rewrite the third record's index as 99 and fix up its CRC, so the record
	// itself is intact but does not follow its predecessor. A stale block from a
	// reused sector looks like this, and it must not be accepted.
	f, err := os.OpenFile(walFile(dir), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	start := sizes[1]
	recLen := sizes[2] - sizes[1] - crcSize
	rec := make([]byte, recLen)
	if _, err := f.ReadAt(rec, start); err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint64(rec[lenFieldSize:], 99)
	if _, err := f.WriteAt(rec, start); err != nil {
		t.Fatal(err)
	}
	var sum [4]byte
	binary.LittleEndian.PutUint32(sum[:], crc32.Checksum(rec, crcTable))
	if _, err := f.WriteAt(sum[:], start+recLen); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s := mustOpen(t, dir)
	defer s.Close()
	if got := s.LastIndex(); got != 2 {
		t.Fatalf("LastIndex = %d, want 2: a non-contiguous index must stop replay", got)
	}
	if got := fileSize(t, walFile(dir)); got != sizes[1] {
		t.Errorf("log size = %d, want %d", got, sizes[1])
	}
}

func TestRecoveryAfterSnapshotWithStaleLog(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	for i := raft.Index(1); i <= 6; i++ {
		mustAppend(t, s, entry(i, 2))
	}
	// Simulate a crash between SaveSnapshot's two durable steps: the snapshot
	// file exists but the log was never compacted.
	snapBytes := snapshotBytes(raft.Snapshot{Meta: raft.SnapshotMeta{Index: 4, Term: 2}, Data: []byte("img")})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, snapName(raft.SnapshotMeta{Index: 4, Term: 2})), snapBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := mustOpen(t, dir)
	defer s2.Close()
	if got := s2.FirstIndex(); got != 5 {
		t.Errorf("FirstIndex = %d, want 5", got)
	}
	if got := s2.LastIndex(); got != 6 {
		t.Errorf("LastIndex = %d, want 6", got)
	}
	if got, err := s2.Term(4); err != nil || got != 2 {
		t.Errorf("Term(4) = (%d, %v), want (2, nil)", got, err)
	}
	if _, err := s2.Entries(4, 6); !errors.Is(err, raft.ErrCompacted) {
		t.Errorf("Entries(4, 6) err = %v, want ErrCompacted", err)
	}
	wantEntries(t, s2, 5, 7, []raft.LogEntry{entry(5, 2), entry(6, 2)})

	// The compaction the crash interrupted must have happened during recovery.
	s2.Close()
	s3 := mustOpen(t, dir)
	defer s3.Close()
	if got := s3.FirstIndex(); got != 5 {
		t.Errorf("FirstIndex after second reopen = %d, want 5", got)
	}
	wantEntries(t, s3, 5, 7, []raft.LogEntry{entry(5, 2), entry(6, 2)})
}

func TestCorruptSnapshotFallsBackToOlder(t *testing.T) {
	dir := t.TempDir()
	older := raft.SnapshotMeta{Index: 3, Term: 1}
	newer := raft.SnapshotMeta{Index: 7, Term: 2}
	if err := os.WriteFile(filepath.Join(dir, snapName(older)), snapshotBytes(raft.Snapshot{Meta: older, Data: []byte("old")}), 0o644); err != nil {
		t.Fatal(err)
	}
	bad := snapshotBytes(raft.Snapshot{Meta: newer, Data: []byte("new")})
	bad[len(bad)-6] ^= 0xff
	if err := os.WriteFile(filepath.Join(dir, snapName(newer)), bad, 0o644); err != nil {
		t.Fatal(err)
	}

	s := mustOpen(t, dir)
	defer s.Close()
	snap, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap.Meta != older {
		t.Fatalf("LoadSnapshot = %+v, want the older intact snapshot %+v", snap.Meta, older)
	}
	if got := s.FirstIndex(); got != 4 {
		t.Errorf("FirstIndex = %d, want 4", got)
	}
}

func TestCorruptStateFileIsReported(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	if err := s.SaveState(12, "node-c"); err != nil {
		t.Fatal(err)
	}
	s.Close()

	raw, err := os.ReadFile(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	raw[5] ^= 0xff
	if err := os.WriteFile(filepath.Join(dir, "state"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Error("Open with a corrupt vote record succeeded; a silently lost vote breaks election safety")
	}
}
