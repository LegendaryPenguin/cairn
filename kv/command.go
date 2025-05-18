// Package kv is the replicated key-value state machine Cairn's Raft log drives.
//
// Everything in this package is written against one constraint: the bytes a
// client submits are replayed on every node, possibly years later by a node
// that was rebuilt from a snapshot and a log tail. So the wire format is
// explicit and hand-rolled rather than reflective. encoding/gob and
// encoding/json are both unsuitable here — gob's output depends on the Go type
// graph and emits type descriptors that shift when a struct field is added or
// reordered, and JSON's map and float rendering has changed across Go releases.
// Either would let two nodes running different binaries disagree about what a
// committed entry means, which Raft cannot detect and cannot repair.
package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Op is the command opcode. Values are explicit and permanent: a committed log
// entry encodes the numeric opcode, so renumbering these would reinterpret
// history. Never reuse a retired value.
type Op uint8

const (
	OpPut Op = 1
	OpDel Op = 2
	OpCAS Op = 3
)

func (o Op) String() string {
	switch o {
	case OpPut:
		return "put"
	case OpDel:
		return "delete"
	case OpCAS:
		return "cas"
	}
	return fmt.Sprintf("op(%d)", uint8(o))
}

// commandVersion prefixes every encoded command so a future format change can
// be recognized rather than silently misparsed.
const commandVersion byte = 1

var (
	// ErrMalformed means a payload could not be parsed. On the apply path this
	// is not recoverable by retrying: the entry is already committed, so every
	// node will fail on it identically, which at least keeps them consistent.
	ErrMalformed = errors.New("cairn/kv: malformed payload")

	// ErrUnknownOp means the opcode is not one this binary understands, which
	// in practice means an older node is replaying a newer cluster's log.
	ErrUnknownOp = errors.New("cairn/kv: unknown opcode")
)

// Command is a single state-machine mutation.
//
// ClientID and SeqNo are carried on every command, not just on the ones that
// look dangerous. Raft gives at-least-once delivery from the client's point of
// view: a client whose request times out between commit and reply has no way to
// learn the outcome, so it retries, and the retry can commit a second time.
// Without the pair there is no way to tell a retry from a genuine second write.
type Command struct {
	Op Op

	// ClientID identifies the submitting session. Empty disables dedup, which
	// is appropriate only for commands no client is waiting on a reply for.
	ClientID string

	// SeqNo is monotonically increasing per ClientID.
	SeqNo uint64

	Key string

	// Value is the new value for OpPut and the post-swap value for OpCAS.
	Value []byte

	// Expected is the value OpCAS requires the key to currently hold. It is
	// only meaningful when ExpectExists is true.
	Expected []byte

	// ExpectExists distinguishes "swap only if the key currently holds
	// Expected" from "swap only if the key is absent". A nil Expected cannot
	// express the second case on its own, because nil and absent are different
	// states and a stored value is allowed to be empty.
	ExpectExists bool
}

// Encode serializes c. The layout is fixed-width-length-prefixed throughout:
// version, opcode, then clientID, seqNo, key, and the op-specific tail. Lengths
// are big-endian uint32 so the encoding does not depend on host byte order.
func (c Command) Encode() ([]byte, error) {
	switch c.Op {
	case OpPut, OpDel, OpCAS:
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownOp, uint8(c.Op))
	}

	buf := make([]byte, 0, 32+len(c.ClientID)+len(c.Key)+len(c.Value)+len(c.Expected))
	buf = append(buf, commandVersion, byte(c.Op))
	buf = appendString(buf, c.ClientID)
	buf = binary.BigEndian.AppendUint64(buf, c.SeqNo)
	buf = appendString(buf, c.Key)

	switch c.Op {
	case OpPut:
		buf = appendOptBytes(buf, c.Value)
	case OpDel:
		// No payload beyond the key.
	case OpCAS:
		buf = appendBool(buf, c.ExpectExists)
		buf = appendOptBytes(buf, c.Expected)
		buf = appendOptBytes(buf, c.Value)
	}
	return buf, nil
}

// Decode parses a command produced by Encode. It rejects trailing bytes: a
// payload with unexplained tail data means the writer and reader disagree about
// the format, and guessing is worse than failing.
func Decode(b []byte) (Command, error) {
	var c Command
	d := &decoder{b: b}

	if v := d.byteAt(); d.err != nil || v != commandVersion {
		if d.err != nil {
			return c, d.err
		}
		return c, fmt.Errorf("%w: command version %d", ErrMalformed, v)
	}
	c.Op = Op(d.byteAt())
	c.ClientID = d.str()
	c.SeqNo = d.u64()
	c.Key = d.str()

	switch c.Op {
	case OpPut:
		c.Value = d.optBytes()
	case OpDel:
	case OpCAS:
		c.ExpectExists = d.boolean()
		c.Expected = d.optBytes()
		c.Value = d.optBytes()
	default:
		if d.err == nil {
			d.err = fmt.Errorf("%w: %d", ErrUnknownOp, uint8(c.Op))
		}
	}

	if d.err != nil {
		return Command{}, d.err
	}
	if d.i != len(d.b) {
		return Command{}, fmt.Errorf("%w: %d trailing bytes", ErrMalformed, len(d.b)-d.i)
	}
	return c, nil
}

// Result is what Apply hands back for a command. It is also what the dedup
// table memoizes, so a retry of an already-applied command can be answered with
// the original outcome instead of a freshly recomputed — and therefore wrong —
// one.
type Result struct {
	Op Op

	// Swapped reports whether an OpCAS matched its precondition and wrote.
	Swapped bool

	// Existed reports whether the key was present before the command ran.
	Existed bool

	// Prior is the value the key held before the command ran, nil when absent.
	// For a failed CAS this is the value that did not match, which lets a
	// client re-plan without a second round trip.
	Prior []byte
}

func appendString(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

// appendOptBytes keeps nil distinguishable from a zero-length slice. The store
// treats both as "present with no bytes", but the command round trip must be
// exact or a re-encoded command would hash differently than the one committed.
func appendOptBytes(b []byte, v []byte) []byte {
	if v == nil {
		return append(b, 0)
	}
	b = append(b, 1)
	b = binary.BigEndian.AppendUint32(b, uint32(len(v)))
	return append(b, v...)
}

func appendBool(b []byte, v bool) []byte {
	if v {
		return append(b, 1)
	}
	return append(b, 0)
}

// decoder accumulates the first error and then reports zero values, so the
// parse functions above can read fields in sequence without a check per field.
type decoder struct {
	b   []byte
	i   int
	err error
}

func (d *decoder) fail(format string, args ...any) {
	if d.err == nil {
		d.err = fmt.Errorf("%w: "+format, append([]any{ErrMalformed}, args...)...)
	}
}

func (d *decoder) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 || len(d.b)-d.i < n {
		d.fail("want %d bytes at offset %d, have %d", n, d.i, len(d.b)-d.i)
		return nil
	}
	out := d.b[d.i : d.i+n]
	d.i += n
	return out
}

func (d *decoder) byteAt() byte {
	p := d.take(1)
	if p == nil {
		return 0
	}
	return p[0]
}

func (d *decoder) u32() int {
	p := d.take(4)
	if p == nil {
		return 0
	}
	return int(binary.BigEndian.Uint32(p))
}

func (d *decoder) u64() uint64 {
	p := d.take(8)
	if p == nil {
		return 0
	}
	return binary.BigEndian.Uint64(p)
}

func (d *decoder) str() string {
	n := d.u32()
	p := d.take(n)
	if p == nil {
		return ""
	}
	return string(p)
}

func (d *decoder) optBytes() []byte {
	switch d.byteAt() {
	case 0:
		return nil
	case 1:
		n := d.u32()
		p := d.take(n)
		if p == nil {
			return nil
		}
		// Copy: the caller outlives the backing array, which for an applied
		// entry is owned by the log reader and may be reused.
		out := make([]byte, n)
		copy(out, p)
		return out
	default:
		d.fail("bad presence byte at offset %d", d.i-1)
		return nil
	}
}

func (d *decoder) boolean() bool {
	switch d.byteAt() {
	case 0:
		return false
	case 1:
		return true
	default:
		d.fail("bad boolean at offset %d", d.i-1)
		return false
	}
}
