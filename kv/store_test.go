package kv

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/LegendaryPenguin/cairn/raft"
)

// apply wraps a command in a log entry the way the consensus layer would, so the
// tests exercise the real Apply path including encode/decode.
func apply(t *testing.T, s *Store, idx raft.Index, cmd Command) *Result {
	t.Helper()
	data, err := cmd.Encode()
	if err != nil {
		t.Fatalf("encode %v: %v", cmd, err)
	}
	out, err := s.Apply(raft.LogEntry{Index: idx, Term: 1, Kind: raft.EntryCommand, Data: data})
	if err != nil {
		t.Fatalf("apply %v: %v", cmd, err)
	}
	res, ok := out.(*Result)
	if !ok {
		t.Fatalf("apply returned %T, want *Result", out)
	}
	return res
}

func wantGet(t *testing.T, s *Store, key, want string) {
	t.Helper()
	got, ok := s.Get(key)
	if !ok {
		t.Fatalf("Get(%q): missing, want %q", key, want)
	}
	if string(got) != want {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}

func wantMissing(t *testing.T, s *Store, key string) {
	t.Helper()
	if got, ok := s.Get(key); ok {
		t.Fatalf("Get(%q) = %q, want missing", key, got)
	}
}

func TestCommandRoundTrip(t *testing.T) {
	cases := []Command{
		{Op: OpPut, ClientID: "c1", SeqNo: 1, Key: "a", Value: []byte("v")},
		{Op: OpPut, ClientID: "", SeqNo: 0, Key: "", Value: nil},
		{Op: OpPut, ClientID: "c1", SeqNo: 9, Key: "k", Value: []byte{}},
		{Op: OpPut, ClientID: "c2", SeqNo: 1 << 40, Key: "bin", Value: []byte{0, 1, 0xff, 0x7f}},
		{Op: OpDel, ClientID: "c3", SeqNo: 7, Key: "gone"},
		{Op: OpCAS, ClientID: "c4", SeqNo: 2, Key: "x", Expected: []byte("old"), Value: []byte("new"), ExpectExists: true},
		{Op: OpCAS, ClientID: "c4", SeqNo: 3, Key: "x", Expected: nil, Value: []byte("created"), ExpectExists: false},
		{Op: OpCAS, ClientID: "c5", SeqNo: 4, Key: "utf8-ключ", Expected: []byte{}, Value: nil, ExpectExists: true},
	}

	for _, want := range cases {
		enc, err := want.Encode()
		if err != nil {
			t.Fatalf("Encode(%+v): %v", want, err)
		}
		got, err := Decode(enc)
		if err != nil {
			t.Fatalf("Decode(%+v): %v", want, err)
		}
		if !commandsEqual(got, want) {
			t.Errorf("round trip changed command:\n got %+v\nwant %+v", got, want)
		}

		// Encoding must be a pure function of the command.
		again, err := got.Encode()
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if !bytes.Equal(enc, again) {
			t.Errorf("re-encode differs for %+v", want)
		}
	}
}

func commandsEqual(a, b Command) bool {
	return a.Op == b.Op &&
		a.ClientID == b.ClientID &&
		a.SeqNo == b.SeqNo &&
		a.Key == b.Key &&
		bytes.Equal(a.Value, b.Value) && (a.Value == nil) == (b.Value == nil) &&
		bytes.Equal(a.Expected, b.Expected) && (a.Expected == nil) == (b.Expected == nil) &&
		a.ExpectExists == b.ExpectExists
}

func TestDecodeRejectsBadPayloads(t *testing.T) {
	good, err := Command{Op: OpPut, ClientID: "c", SeqNo: 1, Key: "k", Value: []byte("v")}.Encode()
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string][]byte{
		"empty":          {},
		"bad version":    {99, byte(OpPut)},
		"truncated":      good[:len(good)-2],
		"trailing bytes": append(bytes.Clone(good), 0),
	}
	for name, payload := range cases {
		if _, err := Decode(payload); err == nil {
			t.Errorf("Decode(%s) succeeded, want error", name)
		}
	}

	unknown := bytes.Clone(good)
	unknown[1] = 200
	if _, err := Decode(unknown); err == nil {
		t.Error("Decode(unknown opcode) succeeded, want error")
	}
}

func TestPutGetDelete(t *testing.T) {
	s := New()

	res := apply(t, s, 1, Command{Op: OpPut, ClientID: "c", SeqNo: 1, Key: "alpha", Value: []byte("one")})
	if res.Existed {
		t.Error("first Put reported a prior value")
	}
	wantGet(t, s, "alpha", "one")

	res = apply(t, s, 2, Command{Op: OpPut, ClientID: "c", SeqNo: 2, Key: "alpha", Value: []byte("two")})
	if !res.Existed || string(res.Prior) != "one" {
		t.Errorf("overwrite result = %+v, want existed with prior %q", res, "one")
	}
	wantGet(t, s, "alpha", "two")

	res = apply(t, s, 3, Command{Op: OpDel, ClientID: "c", SeqNo: 3, Key: "alpha"})
	if !res.Existed || string(res.Prior) != "two" {
		t.Errorf("delete result = %+v, want existed with prior %q", res, "two")
	}
	wantMissing(t, s, "alpha")

	res = apply(t, s, 4, Command{Op: OpDel, ClientID: "c", SeqNo: 4, Key: "alpha"})
	if res.Existed {
		t.Error("delete of a missing key reported it existed")
	}

	if s.Len() != 0 {
		t.Errorf("Len = %d, want 0", s.Len())
	}
}

func TestGetReturnsACopy(t *testing.T) {
	s := New()
	apply(t, s, 1, Command{Op: OpPut, ClientID: "c", SeqNo: 1, Key: "k", Value: []byte("orig")})

	got, _ := s.Get("k")
	got[0] = 'X'
	wantGet(t, s, "k", "orig")
}

func TestNonCommandEntriesIgnored(t *testing.T) {
	s := New()
	for _, kind := range []raft.EntryKind{raft.EntryNoop, raft.EntryConfig} {
		out, err := s.Apply(raft.LogEntry{Index: 1, Term: 1, Kind: kind, Data: []byte("not a command")})
		if err != nil {
			t.Errorf("Apply(kind=%d): %v", kind, err)
		}
		if out != nil {
			t.Errorf("Apply(kind=%d) = %v, want nil", kind, out)
		}
	}
	if s.Len() != 0 {
		t.Errorf("Len = %d, want 0", s.Len())
	}
}

func TestApplyRejectsMalformedCommand(t *testing.T) {
	s := New()
	_, err := s.Apply(raft.LogEntry{Index: 5, Term: 1, Kind: raft.EntryCommand, Data: []byte{0xff}})
	if err == nil {
		t.Fatal("Apply(garbage) succeeded, want error")
	}
}

func TestCASSuccessAndFailure(t *testing.T) {
	s := New()
	apply(t, s, 1, Command{Op: OpPut, ClientID: "w", SeqNo: 1, Key: "k", Value: []byte("v1")})

	res := apply(t, s, 2, Command{
		Op: OpCAS, ClientID: "w", SeqNo: 2, Key: "k",
		Expected: []byte("v1"), Value: []byte("v2"), ExpectExists: true,
	})
	if !res.Swapped {
		t.Fatal("CAS with matching expected did not swap")
	}
	if string(res.Prior) != "v1" {
		t.Errorf("Prior = %q, want %q", res.Prior, "v1")
	}
	wantGet(t, s, "k", "v2")

	res = apply(t, s, 3, Command{
		Op: OpCAS, ClientID: "w", SeqNo: 3, Key: "k",
		Expected: []byte("v1"), Value: []byte("v3"), ExpectExists: true,
	})
	if res.Swapped {
		t.Fatal("CAS with stale expected swapped")
	}
	if string(res.Prior) != "v2" {
		t.Errorf("failed CAS Prior = %q, want the current value %q", res.Prior, "v2")
	}
	wantGet(t, s, "k", "v2")
}

func TestCASOnMissingKey(t *testing.T) {
	s := New()

	// Expecting a value on an absent key must fail, even when the expected
	// value is empty, because absent is not the same state as empty.
	for _, expected := range [][]byte{nil, {}, []byte("something")} {
		res := apply(t, s, 1, Command{
			Op: OpCAS, ClientID: "a", SeqNo: 1, Key: "absent",
			Expected: expected, Value: []byte("x"), ExpectExists: true,
		})
		if res.Swapped {
			t.Fatalf("CAS(expect exists, expected=%q) swapped on a missing key", expected)
		}
		if res.Existed || res.Prior != nil {
			t.Errorf("result = %+v, want absent prior", res)
		}
		wantMissing(t, s, "absent")
		// Fresh store so each iteration starts from the same state.
		s = New()
	}

	// Expecting absence is the create-if-not-exists case and must succeed.
	res := apply(t, s, 1, Command{
		Op: OpCAS, ClientID: "a", SeqNo: 2, Key: "absent",
		Value: []byte("created"), ExpectExists: false,
	})
	if !res.Swapped {
		t.Fatal("CAS(expect absent) failed on a missing key")
	}
	wantGet(t, s, "absent", "created")

	// The same command against the now-present key must fail.
	res = apply(t, s, 2, Command{
		Op: OpCAS, ClientID: "a", SeqNo: 3, Key: "absent",
		Value: []byte("again"), ExpectExists: false,
	})
	if res.Swapped {
		t.Fatal("CAS(expect absent) swapped over an existing key")
	}
	wantGet(t, s, "absent", "created")
}

func TestDuplicateDeliveryIsIdempotent(t *testing.T) {
	s := New()
	apply(t, s, 1, Command{Op: OpPut, ClientID: "c", SeqNo: 1, Key: "n", Value: []byte("100")})

	cas := Command{
		Op: OpCAS, ClientID: "c", SeqNo: 2, Key: "n",
		Expected: []byte("100"), Value: []byte("200"), ExpectExists: true,
	}

	first := apply(t, s, 2, cas)
	if !first.Swapped {
		t.Fatal("first CAS did not swap")
	}

	// The client timed out and retried; the retry commits at a second index.
	// Re-running the CAS would now fail (the value moved on) and, worse, a
	// retried Put would clobber a newer writer. Dedup must suppress it.
	second := apply(t, s, 3, cas)
	if !second.Swapped {
		t.Error("duplicate CAS returned a different outcome than the original")
	}
	if !bytes.Equal(first.Prior, second.Prior) || first.Existed != second.Existed || first.Op != second.Op {
		t.Errorf("duplicate result = %+v, want %+v", second, first)
	}
	wantGet(t, s, "n", "200")

	// A duplicated stale Put must not resurrect an old value either.
	stalePut := Command{Op: OpPut, ClientID: "c", SeqNo: 2, Key: "n", Value: []byte("stale")}
	apply(t, s, 4, stalePut)
	wantGet(t, s, "n", "200")

	// A genuinely new SeqNo still applies.
	apply(t, s, 5, Command{Op: OpPut, ClientID: "c", SeqNo: 3, Key: "n", Value: []byte("300")})
	wantGet(t, s, "n", "300")
}

func TestDedupIsPerClient(t *testing.T) {
	s := New()
	apply(t, s, 1, Command{Op: OpPut, ClientID: "a", SeqNo: 1, Key: "k", Value: []byte("from-a")})
	// Same SeqNo, different client: unrelated command, must apply.
	apply(t, s, 2, Command{Op: OpPut, ClientID: "b", SeqNo: 1, Key: "k", Value: []byte("from-b")})
	wantGet(t, s, "k", "from-b")
}

func TestEmptyClientIDSkipsDedup(t *testing.T) {
	s := New()
	cmd := Command{Op: OpPut, Key: "k", Value: []byte("v")}
	apply(t, s, 1, cmd)
	apply(t, s, 2, Command{Op: OpPut, Key: "k", Value: []byte("w")})
	// Without a ClientID there is no session to dedup against, so the second
	// Put must land rather than being mistaken for a retry of the first.
	wantGet(t, s, "k", "w")
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	s := New()
	apply(t, s, 1, Command{Op: OpPut, ClientID: "c1", SeqNo: 1, Key: "a", Value: []byte("1")})
	apply(t, s, 2, Command{Op: OpPut, ClientID: "c1", SeqNo: 2, Key: "b", Value: []byte{}})
	apply(t, s, 3, Command{Op: OpPut, ClientID: "c2", SeqNo: 1, Key: "c", Value: []byte{0, 0xff}})
	apply(t, s, 4, Command{Op: OpDel, ClientID: "c2", SeqNo: 2, Key: "a"})
	casRes := apply(t, s, 5, Command{
		Op: OpCAS, ClientID: "c3", SeqNo: 10, Key: "b",
		Expected: []byte{}, Value: []byte("swapped"), ExpectExists: true,
	})
	if !casRes.Swapped {
		t.Fatal("setup CAS did not swap")
	}

	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored := New()
	// Pre-dirty the target to prove Restore replaces rather than merges.
	apply(t, restored, 1, Command{Op: OpPut, ClientID: "zz", SeqNo: 99, Key: "junk", Value: []byte("junk")})
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	wantMissing(t, restored, "junk")
	wantMissing(t, restored, "a")
	wantGet(t, restored, "b", "swapped")
	if v, ok := restored.Get("c"); !ok || !bytes.Equal(v, []byte{0, 0xff}) {
		t.Errorf("Get(c) = %v, %v; want [0 255], true", v, ok)
	}

	// The dedup table must have survived: replaying an already-applied command
	// against the restored node has to be a no-op. This is the case that breaks
	// if the sessions table is left out of the snapshot.
	dupCAS := Command{
		Op: OpCAS, ClientID: "c3", SeqNo: 10, Key: "b",
		Expected: []byte{}, Value: []byte("replayed"), ExpectExists: true,
	}
	res := apply(t, restored, 2, dupCAS)
	wantGet(t, restored, "b", "swapped")
	if !res.Swapped {
		t.Error("memoized result lost across restore")
	}

	dupPut := Command{Op: OpPut, ClientID: "c1", SeqNo: 2, Key: "b", Value: []byte("stale")}
	apply(t, restored, 3, dupPut)
	wantGet(t, restored, "b", "swapped")

	// Restoring must also reproduce the exact snapshot bytes, or a node that
	// restores and immediately re-snapshots would diverge from its source.
	again, err := restored.Snapshot()
	if err != nil {
		t.Fatalf("re-Snapshot: %v", err)
	}
	if !bytes.Equal(snap, again) {
		t.Error("snapshot of a restored store differs from the original")
	}
}

func TestRestoreRejectsCorruptSnapshot(t *testing.T) {
	s := New()
	apply(t, s, 1, Command{Op: OpPut, ClientID: "c", SeqNo: 1, Key: "keep", Value: []byte("v")})
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string][]byte{
		"empty":       {},
		"bad version": {42},
		"truncated":   snap[:len(snap)-3],
		"trailing":    append(bytes.Clone(snap), 7),
	}
	for name, payload := range cases {
		target := New()
		apply(t, target, 1, Command{Op: OpPut, ClientID: "x", SeqNo: 1, Key: "pre", Value: []byte("v")})
		if err := target.Restore(payload); err == nil {
			t.Errorf("Restore(%s) succeeded, want error", name)
			continue
		}
		// A rejected snapshot must leave prior state intact rather than a
		// half-applied mixture.
		wantGet(t, target, "pre", "v")
	}
}

// TestSnapshotDeterminismAcrossInsertionOrders is the guard on the sorted-key
// requirement. Both stores reach the same logical state; the snapshots must be
// byte-identical regardless of the order the keys went in, and regardless of Go's
// randomized map iteration.
func TestSnapshotDeterminismAcrossInsertionOrders(t *testing.T) {
	keys := make([]string, 64)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%03d", i)
	}

	build := func(order []int) []byte {
		s := New()
		idx := raft.Index(1)
		for _, i := range order {
			apply(t, s, idx, Command{
				Op:       OpPut,
				ClientID: fmt.Sprintf("client-%d", i%7),
				SeqNo:    uint64(idx),
				Key:      keys[i],
				Value:    []byte(fmt.Sprintf("value-%d", i)),
			})
			idx++
		}
		snap, err := s.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		return snap
	}

	forward := make([]int, len(keys))
	reverse := make([]int, len(keys))
	for i := range keys {
		forward[i] = i
		reverse[i] = len(keys) - 1 - i
	}

	// The dedup table is keyed by client and holds the last SeqNo, so the two
	// orders must agree on the final per-client SeqNo for the comparison to be
	// about ordering rather than about different logical state. Use a single
	// client so both runs end at the same high-water mark.
	buildSingleClient := func(order []int) []byte {
		s := New()
		idx := raft.Index(1)
		for _, i := range order {
			apply(t, s, idx, Command{
				Op:       OpPut,
				ClientID: "solo",
				SeqNo:    uint64(idx),
				Key:      keys[i],
				Value:    []byte(fmt.Sprintf("value-%d", i)),
			})
			idx++
		}
		snap, err := s.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		return snap
	}

	a := buildSingleClient(forward)
	b := buildSingleClient(reverse)
	if !bytes.Equal(a, b) {
		t.Errorf("snapshots differ across insertion orders (%d vs %d bytes)", len(a), len(b))
	}

	// Repeated snapshots of one store must also be stable, which is the part
	// map iteration order would break on its own.
	c := build(forward)
	for i := 0; i < 10; i++ {
		if got := build(forward); !bytes.Equal(c, got) {
			t.Fatalf("snapshot not stable across runs (attempt %d)", i)
		}
	}
}

// model is the reference implementation the randomized test checks against:
// plain maps, no locking, no encoding, no dedup subtleties beyond the rule
// itself.
type model struct {
	data     map[string]string
	present  map[string]bool
	lastSeq  map[string]uint64
	lastSwap map[string]bool
}

func newModel() *model {
	return &model{
		data:     map[string]string{},
		present:  map[string]bool{},
		lastSeq:  map[string]uint64{},
		lastSwap: map[string]bool{},
	}
}

func (m *model) apply(c Command) (swapped bool, deduped bool) {
	if c.ClientID != "" {
		if seq, ok := m.lastSeq[c.ClientID]; ok && c.SeqNo <= seq {
			return m.lastSwap[c.ClientID], true
		}
	}
	switch c.Op {
	case OpPut:
		m.data[c.Key] = string(c.Value)
		m.present[c.Key] = true
	case OpDel:
		delete(m.data, c.Key)
		delete(m.present, c.Key)
	case OpCAS:
		exists := m.present[c.Key]
		if exists == c.ExpectExists && (!exists || m.data[c.Key] == string(c.Expected)) {
			m.data[c.Key] = string(c.Value)
			m.present[c.Key] = true
			swapped = true
		}
	}
	if c.ClientID != "" {
		m.lastSeq[c.ClientID] = c.SeqNo
		m.lastSwap[c.ClientID] = swapped
	}
	return swapped, false
}

func TestRandomizedAgainstModel(t *testing.T) {
	const (
		clients = 4
		keys    = 12
		steps   = 4000
	)

	rng := rand.New(rand.NewSource(20260104))
	s := New()
	m := newModel()
	seqs := make([]uint64, clients)

	for step := 1; step <= steps; step++ {
		cid := rng.Intn(clients)
		key := fmt.Sprintf("k%02d", rng.Intn(keys))

		var cmd Command
		cmd.ClientID = fmt.Sprintf("c%d", cid)
		cmd.Key = key

		// Mostly fresh sequence numbers, with a deliberate minority of replays
		// at or below the high-water mark to exercise dedup.
		switch {
		case rng.Intn(5) == 0 && seqs[cid] > 0:
			cmd.SeqNo = 1 + uint64(rng.Intn(int(seqs[cid])))
		default:
			seqs[cid]++
			cmd.SeqNo = seqs[cid]
		}

		switch rng.Intn(6) {
		case 0, 1, 2:
			cmd.Op = OpPut
			cmd.Value = []byte(fmt.Sprintf("v%d", rng.Intn(4)))
		case 3:
			cmd.Op = OpDel
		default:
			cmd.Op = OpCAS
			cmd.Value = []byte(fmt.Sprintf("v%d", rng.Intn(4)))
			if rng.Intn(4) == 0 {
				cmd.ExpectExists = false
			} else {
				cmd.ExpectExists = true
				cmd.Expected = []byte(fmt.Sprintf("v%d", rng.Intn(4)))
			}
		}

		res := apply(t, s, raft.Index(step), cmd)
		wantSwapped, _ := m.apply(cmd)
		if cmd.Op == OpCAS && res.Swapped != wantSwapped {
			t.Fatalf("step %d: %+v: Swapped = %v, want %v", step, cmd, res.Swapped, wantSwapped)
		}

		// Snapshot/restore mid-run: a node that compacts and reloads must be
		// indistinguishable from one that never did.
		if step%500 == 0 {
			snap, err := s.Snapshot()
			if err != nil {
				t.Fatalf("step %d: Snapshot: %v", step, err)
			}
			next := New()
			if err := next.Restore(snap); err != nil {
				t.Fatalf("step %d: Restore: %v", step, err)
			}
			s = next
		}
	}

	if s.Len() != len(m.data) {
		t.Fatalf("Len = %d, want %d", s.Len(), len(m.data))
	}
	for k, want := range m.data {
		got, ok := s.Get(k)
		if !ok {
			t.Fatalf("Get(%q) missing, want %q", k, want)
		}
		if string(got) != want {
			t.Fatalf("Get(%q) = %q, want %q", k, got, want)
		}
	}
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("k%02d", i)
		if _, inModel := m.data[k]; !inModel {
			wantMissing(t, s, k)
		}
	}
}
