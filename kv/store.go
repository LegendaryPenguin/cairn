package kv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/LegendaryPenguin/cairn/raft"
)

// snapshotVersion prefixes snapshot payloads, which outlive command payloads:
// a snapshot written by one binary is restored by whatever binary is running
// when the node next starts.
const snapshotVersion byte = 1

// session is the dedup record for one client: the highest SeqNo applied and the
// Result that application produced.
type session struct {
	seqNo  uint64
	result *Result
}

// Store is the replicated map. It implements raft.StateMachine.
//
// Apply, Snapshot, and Restore are all called from Raft's single apply
// goroutine and so need no mutual exclusion against each other. The RWMutex
// exists for the read path: the server layer serves Get concurrently with
// applies, from a different goroutine.
type Store struct {
	mu sync.RWMutex

	data map[string][]byte

	// sessions is the per-client last-applied table. It lives inside the state
	// machine, and inside the snapshot, for the same reason data does: it is
	// derived from the log and must survive compaction. A node restored from a
	// snapshot that omitted it would have forgotten which commands it had
	// already acknowledged and would re-apply every retry that arrives in the
	// log tail, turning a duplicate CAS into a second real swap.
	sessions map[string]session
}

func New() *Store {
	return &Store{
		data:     make(map[string][]byte),
		sessions: make(map[string]session),
	}
}

var _ raft.StateMachine = (*Store)(nil)

// Apply executes one committed entry.
//
// Non-command entries (the leader's no-op, configuration changes) are consensus
// bookkeeping the application has no opinion about, so they are accepted and
// ignored rather than treated as corrupt payloads.
func (s *Store) Apply(e raft.LogEntry) (any, error) {
	if e.Kind != raft.EntryCommand {
		return nil, nil
	}

	cmd, err := Decode(e.Data)
	if err != nil {
		return nil, fmt.Errorf("kv: apply index %d: %w", e.Index, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if cmd.ClientID != "" {
		if prev, ok := s.sessions[cmd.ClientID]; ok && cmd.SeqNo <= prev.seqNo {
			// At or below the high-water mark means this command has already
			// been applied; replaying it would double-apply. Only the newest
			// result is retained, so a SeqNo strictly below the mark gets that
			// newest result — acceptable because a client that has moved on to a
			// higher SeqNo is no longer waiting on the older reply.
			return copyResult(prev.result), nil
		}
	}

	res := s.applyLocked(cmd)

	if cmd.ClientID != "" {
		s.sessions[cmd.ClientID] = session{seqNo: cmd.SeqNo, result: res}
	}
	return copyResult(res), nil
}

func (s *Store) applyLocked(cmd Command) *Result {
	prior, existed := s.data[cmd.Key]
	res := &Result{Op: cmd.Op, Existed: existed, Prior: cloneBytes(prior)}

	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = cloneBytes(cmd.Value)

	case OpDel:
		delete(s.data, cmd.Key)

	case OpCAS:
		// A CAS against a missing key succeeds only when the caller said it
		// expected the key to be missing. Comparing a nil Expected against an
		// absent key would otherwise make "create if absent" and "swap a nil
		// value" indistinguishable.
		var match bool
		if existed == cmd.ExpectExists {
			match = !existed || bytes.Equal(prior, cmd.Expected)
		}
		if match {
			s.data[cmd.Key] = cloneBytes(cmd.Value)
			res.Swapped = true
		}
	}
	return res
}

// Get reads the applied state under a read lock.
//
// Reads deliberately do not go through the log: a Put-heavy workload should not
// pay a round of consensus to answer a lookup. The cost is that this method
// alone guarantees nothing about linearizability — it reports whatever this
// node has applied, and a stale leader that has not yet noticed its deposition
// will happily answer from state the real leader has already moved past.
// Establishing that a read is safe to serve is the server layer's job: it must
// confirm leadership at the read's start via ReadIndex (a heartbeat round
// confirming a quorum still follows it) and wait for lastApplied to reach that
// index before calling here. The state machine has no view of terms or commit
// indexes and cannot make that determination itself.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return cloneBytes(v), ok
}

// Len is the number of live keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Snapshot serializes the map and the dedup table.
//
// Keys and client IDs are sorted before writing. This is not cosmetic. Go
// randomizes map iteration order on purpose, so an unsorted encoding would
// produce a different byte string on every call for the same logical state.
// Raft compares and ships snapshots as opaque bytes, and two nodes that reached
// identical state by replaying identical logs must produce identical snapshot
// bytes — otherwise a snapshot's identity depends on which node happened to take
// it, snapshot-based divergence checks become meaningless, and any future
// content-addressed transfer or chunk-level deduplication of snapshot data
// breaks. Sorting makes the encoding a function of the state alone.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	buf := make([]byte, 0, 16+len(s.data)*32)
	buf = append(buf, snapshotVersion)

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	buf = binary.BigEndian.AppendUint32(buf, uint32(len(keys)))
	for _, k := range keys {
		buf = appendString(buf, k)
		buf = appendOptBytes(buf, s.data[k])
	}

	ids := make([]string, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	buf = binary.BigEndian.AppendUint32(buf, uint32(len(ids)))
	for _, id := range ids {
		sess := s.sessions[id]
		buf = appendString(buf, id)
		buf = binary.BigEndian.AppendUint64(buf, sess.seqNo)
		buf = appendResult(buf, sess.result)
	}
	return buf, nil
}

// Restore replaces all state. It builds into fresh maps and only swaps them in
// once the whole payload has parsed, so a truncated snapshot cannot leave the
// store holding half of one state and half of another.
func (s *Store) Restore(data []byte) error {
	d := &decoder{b: data}

	if v := d.byteAt(); d.err != nil || v != snapshotVersion {
		if d.err != nil {
			return d.err
		}
		return fmt.Errorf("%w: snapshot version %d", ErrMalformed, v)
	}

	nKeys := d.u32()
	if d.err != nil {
		return d.err
	}
	newData := make(map[string][]byte, nKeys)
	for i := 0; i < nKeys; i++ {
		k := d.str()
		v := d.optBytes()
		if d.err != nil {
			return d.err
		}
		newData[k] = v
	}

	nSess := d.u32()
	if d.err != nil {
		return d.err
	}
	newSessions := make(map[string]session, nSess)
	for i := 0; i < nSess; i++ {
		id := d.str()
		seq := d.u64()
		res := readResult(d)
		if d.err != nil {
			return d.err
		}
		newSessions[id] = session{seqNo: seq, result: res}
	}

	if d.i != len(d.b) {
		return fmt.Errorf("%w: %d trailing snapshot bytes", ErrMalformed, len(d.b)-d.i)
	}

	s.mu.Lock()
	s.data = newData
	s.sessions = newSessions
	s.mu.Unlock()
	return nil
}

func appendResult(b []byte, r *Result) []byte {
	if r == nil {
		return append(b, 0)
	}
	b = append(b, 1, byte(r.Op))
	b = appendBool(b, r.Swapped)
	b = appendBool(b, r.Existed)
	return appendOptBytes(b, r.Prior)
}

func readResult(d *decoder) *Result {
	switch d.byteAt() {
	case 0:
		return nil
	case 1:
		r := &Result{}
		r.Op = Op(d.byteAt())
		r.Swapped = d.boolean()
		r.Existed = d.boolean()
		r.Prior = d.optBytes()
		if d.err != nil {
			return nil
		}
		return r
	default:
		d.fail("bad result presence byte at offset %d", d.i-1)
		return nil
	}
}

// cloneBytes defends the map against aliasing: a stored value must not share an
// array with a decoded command or with whatever the caller does next to a
// returned slice.
func cloneBytes(b []byte) []byte {
	return bytes.Clone(b)
}

func copyResult(r *Result) *Result {
	if r == nil {
		return nil
	}
	c := *r
	c.Prior = cloneBytes(r.Prior)
	return &c
}
