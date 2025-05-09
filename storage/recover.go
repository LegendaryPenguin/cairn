package storage

import (
	"errors"
	"io"
	"os"

	"github.com/LegendaryPenguin/cairn/raft"
)

// replayResult is what a recovery pass over the log could salvage.
type replayResult struct {
	entries []raft.LogEntry

	// offsets[i] is the file offset of entries[i]'s record. TruncateSuffix needs
	// it to cut the file at a record boundary.
	offsets []int64

	// end is the offset just past the last intact record: the point the file is
	// truncated to when a bad tail is found, and the append point afterwards.
	end int64

	// truncated counts bytes discarded from the tail. Nonzero means the process
	// died mid-write or the tail was damaged.
	truncated int64
}

// replayWAL reads path from the beginning and returns every entry up to the
// first problem.
//
// The torn-tail rule: a crash can interrupt a write at any byte, so the final
// record may be short, may carry a half-written payload that fails its CRC, or
// (if a stale block resurfaced) may carry an index that does not follow its
// predecessor. None of those make the *earlier* records suspect, because the log
// is only ever appended to and each record is self-checking. So recovery stops
// at the first bad record, drops everything from there on, and reports success.
// Raft can lose unacknowledged tail entries safely — it cannot lose
// acknowledged ones, and those were fsynced before the acknowledgement.
//
// Damage in the middle of the log is indistinguishable from damage at the tail
// from the reader's point of view, and is handled the same way. That loses
// entries that were once durable, which is a real (unavoidable without
// replication of the log itself) exposure; it is still strictly better than
// serving an entry whose contents silently changed.
func replayWAL(path string) (replayResult, error) {
	var res replayResult

	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return res, nil
		}
		return res, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return res, err
	}
	size := st.Size()

	r := newWALReader(f)
	var prev raft.Index
	var haveIndex bool
	// end tracks the offset past the last *accepted* record, not the last record
	// read: a record can decode cleanly and still be rejected for its index, and
	// in that case it must not be counted as part of the log.
	var end int64
	for {
		e, err := r.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// errShortRecord and CRC/length failures are all "stop here".
			break
		}
		if haveIndex && e.Index != prev+1 {
			break
		}
		res.entries = append(res.entries, e)
		res.offsets = append(res.offsets, r.off-recordSize(e))
		end = r.off
		prev = e.Index
		haveIndex = true
	}
	res.end = end
	res.truncated = size - end
	return res, nil
}

// repairTail physically removes the bytes replay rejected. It runs before any
// new append so the next record lands at a clean boundary; without it a reader
// would see the new record as a continuation of the garbage.
func (s *Store) repairTail(res replayResult) error {
	if res.truncated <= 0 {
		return nil
	}
	if err := s.wal.truncate(res.end); err != nil {
		return err
	}
	s.truncatedBytes = res.truncated
	return nil
}
