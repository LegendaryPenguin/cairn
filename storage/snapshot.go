package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/LegendaryPenguin/cairn/raft"
)

const (
	snapMagic  uint32 = 0x43524e50 // "CRNP"
	snapPrefix        = "snap-"
	snapSuffix        = ".snap"
	snapHeader        = 4 + 8 + 8 + 4 // magic, index, term, dataLen
)

// snapName encodes (index, term) in the filename, zero padded so lexical order
// matches numeric order. The name carries the metadata so LoadSnapshot can pick
// the newest without opening every candidate.
func snapName(m raft.SnapshotMeta) string {
	return fmt.Sprintf("%s%020d-%020d%s", snapPrefix, uint64(m.Index), uint64(m.Term), snapSuffix)
}

func parseSnapName(name string) (raft.SnapshotMeta, bool) {
	if !strings.HasPrefix(name, snapPrefix) || !strings.HasSuffix(name, snapSuffix) {
		return raft.SnapshotMeta{}, false
	}
	var idx, term uint64
	if _, err := fmt.Sscanf(name, snapPrefix+"%020d-%020d"+snapSuffix, &idx, &term); err != nil {
		return raft.SnapshotMeta{}, false
	}
	return raft.SnapshotMeta{Index: raft.Index(idx), Term: raft.Term(term)}, true
}

func snapshotBytes(snap raft.Snapshot) []byte {
	buf := make([]byte, 0, snapHeader+len(snap.Data)+crcSize)
	buf = binary.LittleEndian.AppendUint32(buf, snapMagic)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(snap.Meta.Index))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(snap.Meta.Term))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(snap.Data)))
	buf = append(buf, snap.Data...)
	return binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf, crcTable))
}

// SaveSnapshot persists snap and then compacts the log.
//
// The order matters and cannot be reversed: if the log were compacted first, a
// crash before the snapshot landed would leave a node with neither the entries
// nor the state they produced, which is unrecoverable. Publishing the snapshot
// first means the worst case is a durable snapshot plus a log that still holds
// entries it covers — redundant, and trimmed on the next Open.
func (s *Store) SaveSnapshot(snap raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return raft.ErrShutdown
	}
	if snap.Meta.Index == 0 {
		return fmt.Errorf("cairn/storage: snapshot index must be nonzero")
	}
	if snap.Meta.Index < s.snap.Index {
		return fmt.Errorf("cairn/storage: snapshot at %d is older than existing snapshot at %d", snap.Meta.Index, s.snap.Index)
	}

	name := filepath.Join(s.dir, snapName(snap.Meta))
	if err := writeFileAtomic(s.dir, name, snapshotBytes(snap)); err != nil {
		return err
	}

	prev := s.snap
	s.snap = snap.Meta
	if err := s.compactLocked(); err != nil {
		s.snap = prev
		return err
	}

	// Only now are the old snapshots unreachable. They are deleted rather than
	// kept as a fallback because the compacted log can no longer replay forward
	// from an older snapshot, so an older image has no use.
	s.removeSnapshotsBelowLocked(snap.Meta.Index)
	return nil
}

// compactLocked rewrites the log without the entries the snapshot covers.
//
// The rewrite goes to a temp file that is fsynced and then renamed over the log.
// An in-place rewrite would be the unsafe variant: a crash partway through
// leaves records from two different generations interleaved, and since every
// record checksums correctly there is nothing for recovery to detect. With
// rename, the log file is either entirely the old generation or entirely the
// new one.
func (s *Store) compactLocked() error {
	keep := 0
	for keep < len(s.ents) && s.ents[keep].Index <= s.snap.Index {
		keep++
	}
	if keep == 0 {
		// Nothing to discard; rewriting would only cost an fsync.
		return nil
	}
	remaining := s.ents[keep:]

	buf := make([]byte, 0, 4096)
	offs := make([]int64, len(remaining))
	var off int64
	for i, e := range remaining {
		offs[i] = off
		buf = appendRecord(buf, e)
		off += recordSize(e)
	}

	tmpPath := s.walPath() + ".compact"
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := tmp.Write(buf); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := s.wal.close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, s.walPath()); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}

	w, err := openWAL(s.walPath())
	if err != nil {
		return err
	}
	s.wal = w

	// The entries are copied out of the old backing array so the discarded
	// prefix can be collected.
	s.ents = append(make([]raft.LogEntry, 0, len(remaining)), remaining...)
	s.offsets = offs
	return nil
}

func (s *Store) removeSnapshotsBelowLocked(keep raft.Index) {
	names, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	removed := false
	for _, de := range names {
		m, ok := parseSnapName(de.Name())
		if !ok || m.Index >= keep {
			continue
		}
		if os.Remove(filepath.Join(s.dir, de.Name())) == nil {
			removed = true
		}
	}
	if removed {
		// Best effort: a surviving stale snapshot is harmless, it is just never
		// chosen by LoadSnapshot.
		_ = syncDir(s.dir)
	}
}

// LoadSnapshot returns the newest snapshot on disk, falling back to older ones
// if the newest fails validation. Meta.Index is zero when there is none.
func (s *Store) LoadSnapshot() (raft.Snapshot, error) {
	s.mu.RLock()
	dir := s.dir
	s.mu.RUnlock()
	return loadNewestSnapshot(dir)
}

func loadNewestSnapshot(dir string) (raft.Snapshot, error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return raft.Snapshot{}, err
	}
	var metas []raft.SnapshotMeta
	for _, de := range des {
		if m, ok := parseSnapName(de.Name()); ok {
			metas = append(metas, m)
		}
	}
	if len(metas) == 0 {
		return raft.Snapshot{}, nil
	}
	sort.Slice(metas, func(i, j int) bool {
		if metas[i].Index != metas[j].Index {
			return metas[i].Index > metas[j].Index
		}
		return metas[i].Term > metas[j].Term
	})

	var firstErr error
	for _, m := range metas {
		snap, err := readSnapshot(filepath.Join(dir, snapName(m)))
		if err == nil {
			return snap, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return raft.Snapshot{}, firstErr
}

func readSnapshot(path string) (raft.Snapshot, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return raft.Snapshot{}, err
	}
	if len(buf) < snapHeader+crcSize {
		return raft.Snapshot{}, fmt.Errorf("cairn/storage: snapshot %s is %d bytes, too short", path, len(buf))
	}
	body := buf[:len(buf)-crcSize]
	if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(buf[len(buf)-crcSize:]) {
		return raft.Snapshot{}, fmt.Errorf("cairn/storage: snapshot %s failed its checksum", path)
	}
	if binary.LittleEndian.Uint32(body) != snapMagic {
		return raft.Snapshot{}, fmt.Errorf("cairn/storage: snapshot %s has wrong magic", path)
	}
	var snap raft.Snapshot
	snap.Meta.Index = raft.Index(binary.LittleEndian.Uint64(body[4:]))
	snap.Meta.Term = raft.Term(binary.LittleEndian.Uint64(body[12:]))
	n := binary.LittleEndian.Uint32(body[20:])
	if uint64(n) != uint64(len(body)-snapHeader) {
		return raft.Snapshot{}, fmt.Errorf("cairn/storage: snapshot %s data length %d disagrees with record size", path, n)
	}
	if n > 0 {
		snap.Data = body[snapHeader:]
	}
	return snap, nil
}
