// Package transport carries Raft RPCs between cluster members. It provides two
// implementations of raft.Transport: an in-memory Switchboard that routes calls
// directly into a peer's raft.RPCHandler (with fault injection, for tests and
// benchmarks), and a TCP transport over a hand-rolled binary framing.
package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/LegendaryPenguin/cairn/raft"
)

// msgType is the explicit discriminator byte that precedes every message body.
//
// The wire format is hand-rolled rather than delegated to encoding/gob (or
// net/rpc, which is gob underneath) for three reasons. First, gob's format is
// derived from Go struct shapes by reflection, so adding or renaming a field in
// package raft silently changes what goes on the wire and can break a mixed
// version cluster during a rolling upgrade; here the layout is written down and
// changing it requires editing this file. Second, gob transmits type
// definitions on first use per connection, which interacts badly with the
// connection pooling and reconnects below: a pooled connection carries hidden
// per-stream state. Third, a Raft heartbeat is a handful of integers, and
// reflection-driven encoding costs far more than the bytes it moves.
type msgType uint8

const (
	msgRequestVoteArgs      msgType = 1
	msgRequestVoteReply     msgType = 2
	msgAppendEntriesArgs    msgType = 3
	msgAppendEntriesReply   msgType = 4
	msgInstallSnapshotArgs  msgType = 5
	msgInstallSnapshotReply msgType = 6
)

func (m msgType) String() string {
	switch m {
	case msgRequestVoteArgs:
		return "RequestVoteArgs"
	case msgRequestVoteReply:
		return "RequestVoteReply"
	case msgAppendEntriesArgs:
		return "AppendEntriesArgs"
	case msgAppendEntriesReply:
		return "AppendEntriesReply"
	case msgInstallSnapshotArgs:
		return "InstallSnapshotArgs"
	case msgInstallSnapshotReply:
		return "InstallSnapshotReply"
	}
	return fmt.Sprintf("msgType(%d)", uint8(m))
}

// maxFrameSize bounds how large a frame a reader will allocate for. Without it
// a corrupt or hostile four-byte length prefix turns into an out-of-memory
// crash on a node that has not even authenticated the sender.
const maxFrameSize = 256 << 20 // 256 MiB

var (
	// ErrMalformed means the bytes on the wire do not decode as the message
	// they claim to be. It is not retryable: the same bytes will fail again.
	ErrMalformed = errors.New("cairn/transport: malformed message")

	// ErrUnknownMessage means the type byte is not one this build understands,
	// which is what a peer running a newer protocol looks like.
	ErrUnknownMessage = errors.New("cairn/transport: unknown message type")

	// ErrFrameTooLarge means a length prefix exceeded maxFrameSize.
	ErrFrameTooLarge = errors.New("cairn/transport: frame exceeds maximum size")
)

// --- encoding primitives ---

type encoder struct {
	buf []byte
}

func (e *encoder) uint8(v uint8) {
	e.buf = append(e.buf, v)
}

func (e *encoder) bool(v bool) {
	if v {
		e.buf = append(e.buf, 1)
	} else {
		e.buf = append(e.buf, 0)
	}
}

func (e *encoder) uint32(v uint32) {
	e.buf = binary.BigEndian.AppendUint32(e.buf, v)
}

func (e *encoder) uint64(v uint64) {
	e.buf = binary.BigEndian.AppendUint64(e.buf, v)
}

// bytes writes a 4-byte length followed by the payload. A nil slice and an
// empty slice both encode as length zero; the Raft layer treats them
// identically (an empty Entries slice is a heartbeat, nil Data is a no-op
// entry's payload), so collapsing them costs nothing and keeps the format from
// needing a nil flag on every field.
func (e *encoder) bytes(b []byte) {
	e.uint32(uint32(len(b)))
	e.buf = append(e.buf, b...)
}

func (e *encoder) string(s string) {
	e.uint32(uint32(len(s)))
	e.buf = append(e.buf, s...)
}

type decoder struct {
	buf []byte
	err error
}

func (d *decoder) fail() {
	if d.err == nil {
		d.err = ErrMalformed
	}
}

func (d *decoder) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 || len(d.buf) < n {
		d.fail()
		return nil
	}
	b := d.buf[:n]
	d.buf = d.buf[n:]
	return b
}

func (d *decoder) uint8() uint8 {
	b := d.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}

func (d *decoder) bool() bool {
	b := d.take(1)
	if b == nil {
		return false
	}
	switch b[0] {
	case 0:
		return false
	case 1:
		return true
	}
	d.fail()
	return false
}

func (d *decoder) uint32() uint32 {
	b := d.take(4)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

func (d *decoder) uint64() uint64 {
	b := d.take(8)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// bytes returns a copy, not a sub-slice of the frame, so the caller may retain
// an entry's Data after the read buffer is reused for the next frame.
func (d *decoder) bytes() []byte {
	n := d.uint32()
	if d.err != nil {
		return nil
	}
	if n > maxFrameSize {
		d.err = ErrFrameTooLarge
		return nil
	}
	if n == 0 {
		return nil
	}
	b := d.take(int(n))
	if b == nil {
		return nil
	}
	out := make([]byte, n)
	copy(out, b)
	return out
}

func (d *decoder) string() string {
	n := d.uint32()
	if d.err != nil {
		return ""
	}
	if n > maxFrameSize {
		d.err = ErrFrameTooLarge
		return ""
	}
	b := d.take(int(n))
	if b == nil {
		return ""
	}
	return string(b)
}

// done reports a decode error, including trailing bytes the message did not
// account for. Trailing garbage means sender and receiver disagree about the
// layout, which is worth failing loudly rather than ignoring.
func (d *decoder) done() error {
	if d.err != nil {
		return d.err
	}
	if len(d.buf) != 0 {
		return fmt.Errorf("%w: %d trailing bytes", ErrMalformed, len(d.buf))
	}
	return nil
}

// --- per-message codecs ---

func encodeLogEntry(e *encoder, ent raft.LogEntry) {
	e.uint64(uint64(ent.Index))
	e.uint64(uint64(ent.Term))
	e.uint8(uint8(ent.Kind))
	e.bytes(ent.Data)
}

func decodeLogEntry(d *decoder) raft.LogEntry {
	var ent raft.LogEntry
	ent.Index = raft.Index(d.uint64())
	ent.Term = raft.Term(d.uint64())
	ent.Kind = raft.EntryKind(d.uint8())
	ent.Data = d.bytes()
	return ent
}

// minEntryBytes is the smallest an encoded entry can be (index, term, kind,
// empty data length). It bounds the entry count against the remaining frame so
// a bogus count cannot make the decoder preallocate a huge slice.
const minEntryBytes = 8 + 8 + 1 + 4

func encodeRequestVoteArgs(a *raft.RequestVoteArgs) []byte {
	e := &encoder{buf: make([]byte, 0, 64)}
	e.uint64(uint64(a.Term))
	e.string(string(a.CandidateID))
	e.uint64(uint64(a.LastLogIndex))
	e.uint64(uint64(a.LastLogTerm))
	e.bool(a.PreVote)
	return e.buf
}

func decodeRequestVoteArgs(b []byte) (*raft.RequestVoteArgs, error) {
	d := &decoder{buf: b}
	a := &raft.RequestVoteArgs{}
	a.Term = raft.Term(d.uint64())
	a.CandidateID = raft.NodeID(d.string())
	a.LastLogIndex = raft.Index(d.uint64())
	a.LastLogTerm = raft.Term(d.uint64())
	a.PreVote = d.bool()
	if err := d.done(); err != nil {
		return nil, err
	}
	return a, nil
}

func encodeRequestVoteReply(r *raft.RequestVoteReply) []byte {
	e := &encoder{buf: make([]byte, 0, 16)}
	e.uint64(uint64(r.Term))
	e.bool(r.VoteGranted)
	return e.buf
}

func decodeRequestVoteReply(b []byte) (*raft.RequestVoteReply, error) {
	d := &decoder{buf: b}
	r := &raft.RequestVoteReply{}
	r.Term = raft.Term(d.uint64())
	r.VoteGranted = d.bool()
	if err := d.done(); err != nil {
		return nil, err
	}
	return r, nil
}

func encodeAppendEntriesArgs(a *raft.AppendEntriesArgs) []byte {
	e := &encoder{buf: make([]byte, 0, 128)}
	e.uint64(uint64(a.Term))
	e.string(string(a.LeaderID))
	e.uint64(uint64(a.PrevLogIndex))
	e.uint64(uint64(a.PrevLogTerm))
	e.uint64(uint64(a.LeaderCommit))
	e.uint32(uint32(len(a.Entries)))
	for i := range a.Entries {
		encodeLogEntry(e, a.Entries[i])
	}
	return e.buf
}

func decodeAppendEntriesArgs(b []byte) (*raft.AppendEntriesArgs, error) {
	d := &decoder{buf: b}
	a := &raft.AppendEntriesArgs{}
	a.Term = raft.Term(d.uint64())
	a.LeaderID = raft.NodeID(d.string())
	a.PrevLogIndex = raft.Index(d.uint64())
	a.PrevLogTerm = raft.Term(d.uint64())
	a.LeaderCommit = raft.Index(d.uint64())
	n := d.uint32()
	if d.err != nil {
		return nil, d.err
	}
	if uint64(n)*minEntryBytes > uint64(len(d.buf)) {
		return nil, fmt.Errorf("%w: entry count %d exceeds frame", ErrMalformed, n)
	}
	if n > 0 {
		a.Entries = make([]raft.LogEntry, n)
		for i := range a.Entries {
			a.Entries[i] = decodeLogEntry(d)
		}
	}
	if err := d.done(); err != nil {
		return nil, err
	}
	return a, nil
}

func encodeAppendEntriesReply(r *raft.AppendEntriesReply) []byte {
	e := &encoder{buf: make([]byte, 0, 32)}
	e.uint64(uint64(r.Term))
	e.bool(r.Success)
	e.uint64(uint64(r.ConflictTerm))
	e.uint64(uint64(r.ConflictIndex))
	return e.buf
}

func decodeAppendEntriesReply(b []byte) (*raft.AppendEntriesReply, error) {
	d := &decoder{buf: b}
	r := &raft.AppendEntriesReply{}
	r.Term = raft.Term(d.uint64())
	r.Success = d.bool()
	r.ConflictTerm = raft.Term(d.uint64())
	r.ConflictIndex = raft.Index(d.uint64())
	if err := d.done(); err != nil {
		return nil, err
	}
	return r, nil
}

func encodeInstallSnapshotArgs(a *raft.InstallSnapshotArgs) []byte {
	e := &encoder{buf: make([]byte, 0, 64+len(a.Data))}
	e.uint64(uint64(a.Term))
	e.string(string(a.LeaderID))
	e.uint64(uint64(a.LastIncludedIndex))
	e.uint64(uint64(a.LastIncludedTerm))
	e.uint64(a.Offset)
	e.bool(a.Done)
	e.bytes(a.Data)
	return e.buf
}

func decodeInstallSnapshotArgs(b []byte) (*raft.InstallSnapshotArgs, error) {
	d := &decoder{buf: b}
	a := &raft.InstallSnapshotArgs{}
	a.Term = raft.Term(d.uint64())
	a.LeaderID = raft.NodeID(d.string())
	a.LastIncludedIndex = raft.Index(d.uint64())
	a.LastIncludedTerm = raft.Term(d.uint64())
	a.Offset = d.uint64()
	a.Done = d.bool()
	a.Data = d.bytes()
	if err := d.done(); err != nil {
		return nil, err
	}
	return a, nil
}

func encodeInstallSnapshotReply(r *raft.InstallSnapshotReply) []byte {
	e := &encoder{buf: make([]byte, 0, 8)}
	e.uint64(uint64(r.Term))
	return e.buf
}

func decodeInstallSnapshotReply(b []byte) (*raft.InstallSnapshotReply, error) {
	d := &decoder{buf: b}
	r := &raft.InstallSnapshotReply{}
	r.Term = raft.Term(d.uint64())
	if err := d.done(); err != nil {
		return nil, err
	}
	return r, nil
}

// frame assembles a complete wire frame: a 4-byte big-endian length covering
// the type byte and body, then the type byte, then the body.
func frame(t msgType, body []byte) ([]byte, error) {
	n := len(body) + 1
	if n > maxFrameSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, n)
	}
	out := make([]byte, 0, 4+n)
	out = binary.BigEndian.AppendUint32(out, uint32(n))
	out = append(out, byte(t))
	return append(out, body...), nil
}

func init() {
	// The length prefix is a uint32, so maxFrameSize must fit in one.
	if maxFrameSize > math.MaxUint32 {
		panic("cairn/transport: maxFrameSize does not fit a uint32 length prefix")
	}
}
