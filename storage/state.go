package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/LegendaryPenguin/cairn/raft"
)

const stateMagic uint32 = 0x43524e53 // "CRNS"

// The vote record is written to a temp file, fsynced, and renamed over the real
// one, rather than overwritten in place.
//
// In-place looks tempting because the record is small, and a single-sector
// overwrite is widely *assumed* atomic. That assumption is not portable: the
// record is variable length because NodeID is a string, so it can straddle a
// sector boundary, and on a crash a straddling overwrite can leave the first
// half new and the second half old. A torn vote record is worse than a lost one
// — it can decode as a vote for a node in a term where none was granted, which
// breaks the at-most-one-vote-per-term invariant that election safety rests on.
//
// rename(2) within a directory is atomic by POSIX: a concurrent or post-crash
// reader sees either the old inode or the new one. Durability needs three
// syncs: the temp file's contents, then the directory entry (otherwise the
// rename can be lost even though the data survived).
func (s *Store) SaveState(term raft.Term, votedFor raft.NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return raft.ErrShutdown
	}

	buf := make([]byte, 0, 4+8+4+len(votedFor)+4)
	buf = binary.LittleEndian.AppendUint32(buf, stateMagic)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(term))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(votedFor)))
	buf = append(buf, votedFor...)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf, crcTable))

	if err := writeFileAtomic(s.dir, s.statePath(), buf); err != nil {
		return err
	}
	s.term, s.votedFor = term, votedFor
	return nil
}

func (s *Store) LoadState() (raft.Term, raft.NodeID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.term, s.votedFor, nil
}

func (s *Store) statePath() string { return filepath.Join(s.dir, "state") }

// loadStateFile reads the vote record from disk. A missing file means a fresh
// node. A corrupt file is reported: unlike a log tail, there is no earlier
// version to fall back on, and silently returning term 0 would let the node
// vote twice in the same term.
func loadStateFile(path string) (raft.Term, raft.NodeID, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, "", nil
		}
		return 0, "", err
	}
	if len(buf) < 4+8+4+4 {
		return 0, "", fmt.Errorf("cairn/storage: state file %s is %d bytes, too short", path, len(buf))
	}
	body := buf[:len(buf)-4]
	if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(buf[len(buf)-4:]) {
		return 0, "", fmt.Errorf("cairn/storage: state file %s failed its checksum", path)
	}
	if binary.LittleEndian.Uint32(body) != stateMagic {
		return 0, "", fmt.Errorf("cairn/storage: state file %s has wrong magic", path)
	}
	term := raft.Term(binary.LittleEndian.Uint64(body[4:]))
	n := binary.LittleEndian.Uint32(body[12:])
	if uint64(n) != uint64(len(body)-16) {
		return 0, "", fmt.Errorf("cairn/storage: state file %s vote length %d disagrees with record size", path, n)
	}
	return term, raft.NodeID(body[16:]), nil
}

// writeFileAtomic replaces path with data such that a crash leaves either the
// previous contents or the new ones.
func writeFileAtomic(dir, path string, data []byte) error {
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once the rename succeeds

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	// Sync before rename: rename only publishes the name, so if the blocks are
	// still in page cache the published name can point at zeros after a crash.
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return syncDir(dir)
}

// syncDir makes a directory entry change (create, rename, unlink) durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
