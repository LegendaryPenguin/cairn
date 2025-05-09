// Package storage implements raft.Storage on top of a crash-safe
// append-only log, a durable vote record, and atomically replaced snapshots.
package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/LegendaryPenguin/cairn/raft"
)

// Record layout, little-endian throughout:
//
//	uint32 recLen   bytes following this field up to but excluding the CRC
//	uint64 index
//	uint64 term
//	uint8  kind
//	uint32 dataLen
//	[]byte data
//	uint32 crc      Castagnoli over every preceding byte of this record
//
// recLen is covered by the CRC so a corrupted length cannot steer the reader
// into a plausible-looking but wrong record boundary without being detected.
const (
	// recFixed is index+term+kind+dataLen: the part of recLen that is constant.
	recFixed = 8 + 8 + 1 + 4

	lenFieldSize = 4
	crcSize      = 4

	// maxRecordLen bounds how much a single corrupted length field can make the
	// reader allocate before the CRC check has a chance to reject it.
	maxRecordLen = 64 << 20
)

// Castagnoli rather than the IEEE polynomial because amd64 and arm64 implement
// it in hardware, so checksumming is not the cost that makes fsync look cheap.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

var errShortRecord = errors.New("cairn/storage: truncated record")

// recordSize is the on-disk footprint of e, used to advance offsets without
// re-encoding.
func recordSize(e raft.LogEntry) int64 {
	return int64(lenFieldSize + recFixed + len(e.Data) + crcSize)
}

// appendRecord encodes e onto dst and returns the grown slice.
func appendRecord(dst []byte, e raft.LogEntry) []byte {
	start := len(dst)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(recFixed+len(e.Data)))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(e.Index))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(e.Term))
	dst = append(dst, byte(e.Kind))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(e.Data)))
	dst = append(dst, e.Data...)
	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst[start:], crcTable))
}

// wal is an append-only entry log. It tracks its own logical size instead of
// trusting the file offset, so a failed partial write cannot leave the next
// append writing into the middle of a record.
type wal struct {
	f    *os.File
	path string
	size int64

	// buf is reused across Append calls to encode a whole batch into one write.
	buf []byte
}

func openWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &wal{f: f, path: path, size: st.Size()}, nil
}

// append writes every entry in one syscall and fsyncs once for the whole batch.
// Raft only needs the batch to be durable before it is acknowledged, not each
// entry individually, and the fsync dominates the cost of an append.
//
// It returns the file offset each entry landed at, in order.
func (w *wal) append(entries []raft.LogEntry) ([]int64, error) {
	w.buf = w.buf[:0]
	offs := make([]int64, len(entries))
	off := w.size
	for i, e := range entries {
		offs[i] = off
		w.buf = appendRecord(w.buf, e)
		off += recordSize(e)
	}
	n, err := w.f.WriteAt(w.buf, w.size)
	if err != nil {
		// A short or failed write leaves a torn tail on disk. Leaving size
		// unchanged is safe: recovery drops the tail, and the next append
		// overwrites it. Advancing size by n would instead cement the garbage.
		return nil, err
	}
	if n != len(w.buf) {
		return nil, io.ErrShortWrite
	}
	if err := w.f.Sync(); err != nil {
		return nil, err
	}
	w.size = off
	return offs, nil
}

// truncate drops everything at or after off and makes the shorter length
// durable. Only whole-record offsets may be passed.
func (w *wal) truncate(off int64) error {
	if off > w.size {
		return fmt.Errorf("cairn/storage: truncate offset %d beyond log size %d", off, w.size)
	}
	if err := w.f.Truncate(off); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.size = off
	return nil
}

func (w *wal) close() error {
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// walReader walks records in write order. It reports the offset just past the
// last record it returned so a caller recovering a torn tail knows where the
// last intact record ended.
type walReader struct {
	br  *bufio.Reader
	off int64
	hdr [lenFieldSize + recFixed]byte
}

func newWALReader(r io.Reader) *walReader {
	return &walReader{br: bufio.NewReaderSize(r, 1<<16)}
}

// next returns the next entry. It reports errShortRecord for a tail that ends
// mid-record and raft-independent corruption errors for a failed CRC; both are
// "stop here" signals to the recovery pass rather than fatal errors.
func (r *walReader) next() (raft.LogEntry, error) {
	var e raft.LogEntry
	if _, err := io.ReadFull(r.br, r.hdr[:lenFieldSize]); err != nil {
		if errors.Is(err, io.EOF) {
			return e, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return e, errShortRecord
		}
		return e, err
	}
	recLen := binary.LittleEndian.Uint32(r.hdr[:lenFieldSize])
	if recLen < recFixed || recLen > maxRecordLen {
		return e, fmt.Errorf("cairn/storage: implausible record length %d at offset %d", recLen, r.off)
	}

	if _, err := io.ReadFull(r.br, r.hdr[lenFieldSize:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return e, errShortRecord
		}
		return e, err
	}
	dataLen := binary.LittleEndian.Uint32(r.hdr[lenFieldSize+17:])
	if uint64(dataLen)+recFixed != uint64(recLen) {
		return e, fmt.Errorf("cairn/storage: record length %d disagrees with data length %d at offset %d", recLen, dataLen, r.off)
	}

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(r.br, data); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return e, errShortRecord
		}
		return e, err
	}
	var crcBuf [crcSize]byte
	if _, err := io.ReadFull(r.br, crcBuf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return e, errShortRecord
		}
		return e, err
	}

	sum := crc32.Checksum(r.hdr[:], crcTable)
	sum = crc32.Update(sum, crcTable, data)
	if sum != binary.LittleEndian.Uint32(crcBuf[:]) {
		return e, fmt.Errorf("cairn/storage: crc mismatch at offset %d", r.off)
	}

	e.Index = raft.Index(binary.LittleEndian.Uint64(r.hdr[lenFieldSize:]))
	e.Term = raft.Term(binary.LittleEndian.Uint64(r.hdr[lenFieldSize+8:]))
	e.Kind = raft.EntryKind(r.hdr[lenFieldSize+16])
	if dataLen > 0 {
		e.Data = data
	}
	r.off += int64(lenFieldSize) + int64(recLen) + crcSize
	return e, nil
}
