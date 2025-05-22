package transport

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/LegendaryPenguin/cairn/raft"
)

var (
	// ErrUnknownPeer means the target NodeID was never registered.
	ErrUnknownPeer = errors.New("cairn/transport: unknown peer")

	// ErrUnreachable means a partition or isolation rule blocked the RPC. The
	// Raft layer must treat this like any lost message and retry on its next
	// tick, which is exactly what a real network partition looks like to it.
	ErrUnreachable = errors.New("cairn/transport: peer unreachable")

	// ErrDropped means the fault injector discarded the RPC before delivery.
	ErrDropped = errors.New("cairn/transport: rpc dropped")

	// ErrPeerStopped means the sender or the target is paused, standing in for a
	// process that is not scheduled (SIGSTOP, a long GC pause, a hung host).
	ErrPeerStopped = errors.New("cairn/transport: peer stopped")
)

// Pair identifies a directed sender/receiver combination for message counting.
type Pair struct {
	From raft.NodeID
	To   raft.NodeID
}

// Stats is a snapshot of the message counters. Attempts count every RPC the
// consensus layer handed to the transport, including ones that were dropped or
// blocked, because that is the message cost a benchmark cares about: the work a
// node chose to do, not the work that happened to land.
type Stats struct {
	Attempts   uint64
	ByType     map[string]uint64
	ByPair     map[Pair]uint64
	Delivered  uint64
	Dropped    uint64
	Blocked    uint64
	Duplicated uint64
}

// Switchboard is an in-memory network. Registered nodes are reachable by
// NodeID, and a call is a direct invocation of the target's raft.RPCHandler on
// the caller's goroutine, so a cluster test runs with no sockets and no ports.
//
// Every fault knob is settable at any time from any goroutine while RPCs are in
// flight, because that is how the interesting Raft bugs surface: a partition
// that opens in the middle of an election, a node that pauses between appending
// an entry and acknowledging it.
type Switchboard struct {
	mu       sync.Mutex
	rng      *rand.Rand
	handlers map[raft.NodeID]raft.RPCHandler

	// group maps a node to its partition group. Nil means no partition is in
	// effect; a node missing from a non-nil map can reach nobody, since
	// Partition's caller is stating the complete topology.
	group map[raft.NodeID]int

	isolated map[raft.NodeID]bool
	paused   map[raft.NodeID]bool

	latMin, latMax time.Duration
	dropRate       float64
	dupRate        float64

	stats Stats
}

// NewSwitchboard returns an empty switchboard whose fault injection is driven
// by the given seed. A test that fails prints its seed and rerunning with that
// seed replays the same drop, duplicate and latency decisions, provided the
// RPCs arrive in the same order; the draws come from one mutex-protected
// generator in call order, so a single-goroutine driver reproduces exactly.
func NewSwitchboard(seed int64) *Switchboard {
	u := uint64(seed)
	return &Switchboard{
		// Two PCG streams are derived from the one seed so callers only have to
		// record a single number to reproduce a failure.
		rng:      rand.New(rand.NewPCG(u, u^0x9e3779b97f4a7c15)),
		handlers: make(map[raft.NodeID]raft.RPCHandler),
		isolated: make(map[raft.NodeID]bool),
		paused:   make(map[raft.NodeID]bool),
		stats: Stats{
			ByType: make(map[string]uint64),
			ByPair: make(map[Pair]uint64),
		},
	}
}

// Register makes a node reachable. Registering the same ID again replaces the
// handler, which is how a test restarts a node with fresh state.
func (s *Switchboard) Register(id raft.NodeID, h raft.RPCHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[id] = h
}

// Deregister removes a node entirely; calls to it then fail with ErrUnknownPeer.
func (s *Switchboard) Deregister(id raft.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.handlers, id)
}

// Transport returns the raft.Transport a node uses to reach its peers. The
// sender identity is bound here rather than passed per call because
// raft.Transport has no "from" parameter, and the switchboard needs the sender
// to apply pause and partition rules and to attribute message counts.
func (s *Switchboard) Transport(from raft.NodeID) raft.Transport {
	return &switchTransport{sb: s, from: from}
}

// Partition declares the complete reachability topology: two nodes can exchange
// RPCs only if they appear in the same group. Nodes left out of every group are
// cut off from everyone.
func (s *Switchboard) Partition(groups [][]raft.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := make(map[raft.NodeID]int, len(s.handlers))
	for i, grp := range groups {
		for _, id := range grp {
			g[id] = i
		}
	}
	s.group = g
}

// Heal removes all partitions and un-isolates every node. It deliberately does
// not clear pauses, drop rate or latency: those are separate faults and a test
// that heals the network usually still wants the lossy link it configured.
func (s *Switchboard) Heal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.group = nil
	s.isolated = make(map[raft.NodeID]bool)
}

// Isolate cuts one node off from every peer in both directions, without
// disturbing any partition groups already in effect.
func (s *Switchboard) Isolate(id raft.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isolated[id] = true
}

// Rejoin undoes Isolate.
func (s *Switchboard) Rejoin(id raft.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.isolated, id)
}

// Pause simulates a process that is no longer running: RPCs sent to it fail and
// RPCs it tries to send are dropped. It fails fast rather than blocking the
// caller, because a blocked call would pin a Raft goroutine for the whole pause
// and the consensus layer cannot distinguish a slow peer from a dead one
// anyway. The distinction from Isolate is that a paused node also stops serving
// its own handler, so its state is frozen in place.
func (s *Switchboard) Pause(id raft.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused[id] = true
}

// Resume undoes Pause.
func (s *Switchboard) Resume(id raft.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.paused, id)
}

// SetLatency sets the inclusive bounds of a random delay applied before
// delivery. Zero bounds mean no delay. Latency is what makes election timeouts
// and heartbeat intervals meaningful in a test; with instant delivery a
// split-vote race never happens.
func (s *Switchboard) SetLatency(min, max time.Duration) {
	if min < 0 {
		min = 0
	}
	if max < min {
		max = min
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latMin, s.latMax = min, max
}

// SetDropRate sets the fraction of RPCs that vanish: the handler is never
// invoked and the caller gets ErrDropped.
func (s *Switchboard) SetDropRate(p float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropRate = clampProbability(p)
}

// SetDuplicateRate sets the fraction of RPCs delivered twice, with the second
// reply discarded.
//
// Duplicate injection is not padding: Raft's RPCs are specified to be
// idempotent, and real networks and retry loops deliver duplicates routinely. A
// handler that, say, appends entries without first checking whether it already
// holds them, or that counts a repeated vote reply twice toward a quorum, is
// correct against a clean network and broken against a real one. Forcing
// duplicates in every cluster test turns that class of bug from a rare
// production mystery into a deterministic failure.
func (s *Switchboard) SetDuplicateRate(p float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dupRate = clampProbability(p)
}

func clampProbability(p float64) float64 {
	if p < 0 {
		return 0
	}
	if p > 1 {
		return 1
	}
	return p
}

// Stats returns a copy of the message counters.
func (s *Switchboard) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.stats
	out.ByType = make(map[string]uint64, len(s.stats.ByType))
	for k, v := range s.stats.ByType {
		out.ByType[k] = v
	}
	out.ByPair = make(map[Pair]uint64, len(s.stats.ByPair))
	for k, v := range s.stats.ByPair {
		out.ByPair[k] = v
	}
	return out
}

// ResetStats zeroes the counters, so a benchmark can exclude cluster startup
// from its message-cost measurement.
func (s *Switchboard) ResetStats() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats = Stats{
		ByType: make(map[string]uint64),
		ByPair: make(map[Pair]uint64),
	}
}

// String renders the per-pair counts in a stable order for benchmark output.
func (st Stats) String() string {
	pairs := make([]Pair, 0, len(st.ByPair))
	for p := range st.ByPair {
		pairs = append(pairs, p)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].From != pairs[j].From {
			return pairs[i].From < pairs[j].From
		}
		return pairs[i].To < pairs[j].To
	})
	out := fmt.Sprintf("attempts=%d delivered=%d dropped=%d blocked=%d duplicated=%d",
		st.Attempts, st.Delivered, st.Dropped, st.Blocked, st.Duplicated)
	types := make([]string, 0, len(st.ByType))
	for t := range st.ByType {
		types = append(types, t)
	}
	sort.Strings(types)
	for _, t := range types {
		out += fmt.Sprintf("\n  %s: %d", t, st.ByType[t])
	}
	for _, p := range pairs {
		out += fmt.Sprintf("\n  %s->%s: %d", p.From, p.To, st.ByPair[p])
	}
	return out
}

// reachable reports whether from can send to to under the current topology.
// Must be called with the mutex held.
func (s *Switchboard) reachable(from, to raft.NodeID) bool {
	// A node can always reach itself: self-delivery is a local call, not
	// something the network could have interfered with.
	if from == to {
		return true
	}
	if s.isolated[from] || s.isolated[to] {
		return false
	}
	if s.group == nil {
		return true
	}
	gf, okf := s.group[from]
	gt, okt := s.group[to]
	return okf && okt && gf == gt
}

// delivery is the decision the switchboard made about one RPC.
type delivery struct {
	h         raft.RPCHandler
	delay     time.Duration
	duplicate bool
}

// decide applies every fault rule under the lock and returns what should
// happen. Sleeping for the injected latency is left to the caller so the lock
// is not held across a delay, which would serialize the whole cluster and make
// concurrent fault injection impossible.
//
// The order of the random draws is part of the reproducibility contract: drop
// first, duplicate second, latency third, and only for an RPC that got as far
// as being deliverable.
func (s *Switchboard) decide(from, to raft.NodeID, t msgType) (delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stats.Attempts++
	s.stats.ByType[t.String()]++
	s.stats.ByPair[Pair{From: from, To: to}]++

	h, ok := s.handlers[to]
	if !ok {
		return delivery{}, fmt.Errorf("%w: %s", ErrUnknownPeer, to)
	}
	if s.paused[from] {
		s.stats.Dropped++
		return delivery{}, fmt.Errorf("%w: sender %s", ErrPeerStopped, from)
	}
	if s.paused[to] {
		s.stats.Dropped++
		return delivery{}, fmt.Errorf("%w: %s", ErrPeerStopped, to)
	}
	if !s.reachable(from, to) {
		s.stats.Blocked++
		return delivery{}, fmt.Errorf("%w: %s -> %s", ErrUnreachable, from, to)
	}
	if s.dropRate > 0 && s.rng.Float64() < s.dropRate {
		s.stats.Dropped++
		return delivery{}, fmt.Errorf("%w: %s -> %s", ErrDropped, from, to)
	}

	d := delivery{h: h}
	if s.dupRate > 0 && s.rng.Float64() < s.dupRate {
		d.duplicate = true
		s.stats.Duplicated++
	}
	if s.latMax > 0 {
		if s.latMax > s.latMin {
			d.delay = s.latMin + time.Duration(s.rng.Int64N(int64(s.latMax-s.latMin)+1))
		} else {
			d.delay = s.latMin
		}
	}
	s.stats.Delivered++
	return d, nil
}

type switchTransport struct {
	sb   *Switchboard
	from raft.NodeID
}

func (t *switchTransport) RequestVote(to raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	d, err := t.sb.decide(t.from, to, msgRequestVoteArgs)
	if err != nil {
		return nil, err
	}
	sleep(d.delay)
	deliver := func() (*raft.RequestVoteReply, error) {
		a, err := copyRequestVoteArgs(args)
		if err != nil {
			return nil, err
		}
		return copyRequestVoteReply(d.h.HandleRequestVote(a))
	}
	reply, err := deliver()
	if d.duplicate {
		// The retransmission's reply is thrown away on purpose: the caller must
		// behave correctly knowing only about the first one.
		_, _ = deliver()
	}
	return reply, err
}

func (t *switchTransport) AppendEntries(to raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	d, err := t.sb.decide(t.from, to, msgAppendEntriesArgs)
	if err != nil {
		return nil, err
	}
	sleep(d.delay)
	deliver := func() (*raft.AppendEntriesReply, error) {
		a, err := copyAppendEntriesArgs(args)
		if err != nil {
			return nil, err
		}
		return copyAppendEntriesReply(d.h.HandleAppendEntries(a))
	}
	reply, err := deliver()
	if d.duplicate {
		_, _ = deliver()
	}
	return reply, err
}

func (t *switchTransport) InstallSnapshot(to raft.NodeID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	d, err := t.sb.decide(t.from, to, msgInstallSnapshotArgs)
	if err != nil {
		return nil, err
	}
	sleep(d.delay)
	deliver := func() (*raft.InstallSnapshotReply, error) {
		a, err := copyInstallSnapshotArgs(args)
		if err != nil {
			return nil, err
		}
		return copyInstallSnapshotReply(d.h.HandleInstallSnapshot(a))
	}
	reply, err := deliver()
	if d.duplicate {
		_, _ = deliver()
	}
	return reply, err
}

func sleep(d time.Duration) {
	if d > 0 {
		time.Sleep(d)
	}
}

// The copy helpers push arguments and replies through the same codec the TCP
// transport uses. Passing the caller's pointer straight into the handler would
// let the two nodes share backing arrays, so a handler that retained an
// Entries slice or mutated Data would appear to work in every in-memory test
// and corrupt state the moment the same code ran over a socket. Round-tripping
// also means the in-memory tests exercise the wire format on every RPC.

func copyRequestVoteArgs(a *raft.RequestVoteArgs) (*raft.RequestVoteArgs, error) {
	return decodeRequestVoteArgs(encodeRequestVoteArgs(a))
}

func copyRequestVoteReply(r *raft.RequestVoteReply) (*raft.RequestVoteReply, error) {
	if r == nil {
		return nil, errNilReply
	}
	return decodeRequestVoteReply(encodeRequestVoteReply(r))
}

func copyAppendEntriesArgs(a *raft.AppendEntriesArgs) (*raft.AppendEntriesArgs, error) {
	return decodeAppendEntriesArgs(encodeAppendEntriesArgs(a))
}

func copyAppendEntriesReply(r *raft.AppendEntriesReply) (*raft.AppendEntriesReply, error) {
	if r == nil {
		return nil, errNilReply
	}
	return decodeAppendEntriesReply(encodeAppendEntriesReply(r))
}

func copyInstallSnapshotArgs(a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotArgs, error) {
	return decodeInstallSnapshotArgs(encodeInstallSnapshotArgs(a))
}

func copyInstallSnapshotReply(r *raft.InstallSnapshotReply) (*raft.InstallSnapshotReply, error) {
	if r == nil {
		return nil, errNilReply
	}
	return decodeInstallSnapshotReply(encodeInstallSnapshotReply(r))
}

// errNilReply surfaces a handler that returned no reply at all. raft.RPCHandler
// has no error return, so a nil reply is the only way it can fail, and silently
// handing nil back to the caller would turn a handler bug into a nil
// dereference deep inside the consensus layer.
var errNilReply = errors.New("cairn/transport: handler returned nil reply")
