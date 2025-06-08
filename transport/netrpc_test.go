package transport

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/LegendaryPenguin/cairn/raft"
)

func newLoopback(t *testing.T, h raft.RPCHandler, cfg NetConfig) (*NetTransport, *Listener) {
	t.Helper()
	l, err := Listen("127.0.0.1:0", h, cfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	tr := NewNetTransport(map[raft.NodeID]string{"peer": l.Addr().String()}, cfg)
	t.Cleanup(func() {
		_ = tr.Close()
		_ = l.Close()
	})
	return tr, l
}

func TestNetTransportRoundTripsEveryRPCOverASocket(t *testing.T) {
	h := newRecorder("peer")
	h.voteReply = raft.RequestVoteReply{Term: 77, VoteGranted: true}
	h.appendReply = raft.AppendEntriesReply{Term: 78, Success: false, ConflictTerm: 12, ConflictIndex: 345}
	h.installReply = raft.InstallSnapshotReply{Term: 79}
	tr, _ := newLoopback(t, h, NetConfig{})

	voteArgs := &raft.RequestVoteArgs{Term: 77, CandidateID: "candidate-1", LastLogIndex: 900, LastLogTerm: 76, PreVote: true}
	gotVote, err := tr.RequestVote("peer", voteArgs)
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if *gotVote != h.voteReply {
		t.Errorf("vote reply: got %+v want %+v", *gotVote, h.voteReply)
	}

	payload := bytes.Repeat([]byte{0x00, 0xFF, 0x41}, 100_000)
	appendArgs := &raft.AppendEntriesArgs{
		Term: 78, LeaderID: "leader-2", PrevLogIndex: 900, PrevLogTerm: 76, LeaderCommit: 899,
		Entries: []raft.LogEntry{
			{Index: 901, Term: 78, Kind: raft.EntryCommand, Data: []byte("put a 1")},
			{Index: 902, Term: 78, Kind: raft.EntryNoop, Data: nil},
			{Index: 903, Term: 78, Kind: raft.EntryConfig, Data: payload},
		},
	}
	gotAppend, err := tr.AppendEntries("peer", appendArgs)
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if *gotAppend != h.appendReply {
		t.Errorf("append reply: got %+v want %+v", *gotAppend, h.appendReply)
	}

	// A heartbeat is the empty-Entries case and travels the same path.
	if _, err := tr.AppendEntries("peer", &raft.AppendEntriesArgs{Term: 78, LeaderID: "leader-2", PrevLogIndex: 903, PrevLogTerm: 78, LeaderCommit: 903}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	snapArgs := &raft.InstallSnapshotArgs{
		Term: 79, LeaderID: "leader-2", LastIncludedIndex: 903, LastIncludedTerm: 78,
		Offset: 65536, Data: bytes.Repeat([]byte("state"), 50_000), Done: true,
	}
	gotSnap, err := tr.InstallSnapshot("peer", snapArgs)
	if err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	if *gotSnap != h.installReply {
		t.Errorf("install reply: got %+v want %+v", *gotSnap, h.installReply)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.votes) != 1 || h.votes[0] != *voteArgs {
		t.Errorf("vote args: got %+v want %+v", h.votes, *voteArgs)
	}

	if len(h.appends) != 2 {
		t.Fatalf("appends: got %d want 2", len(h.appends))
	}
	gotEntries := h.appends[0].Entries
	if len(gotEntries) != len(appendArgs.Entries) {
		t.Fatalf("entries: got %d want %d", len(gotEntries), len(appendArgs.Entries))
	}
	for i, want := range appendArgs.Entries {
		if gotEntries[i].Index != want.Index || gotEntries[i].Term != want.Term || gotEntries[i].Kind != want.Kind {
			t.Errorf("entry %d header: got %+v want %+v", i, gotEntries[i], want)
		}
		if !bytes.Equal(gotEntries[i].Data, want.Data) {
			t.Errorf("entry %d data: got %d bytes want %d", i, len(gotEntries[i].Data), len(want.Data))
		}
	}
	if h.appends[1].Entries != nil {
		t.Errorf("heartbeat arrived with %d entries", len(h.appends[1].Entries))
	}

	if len(h.installs) != 1 {
		t.Fatalf("installs: got %d want 1", len(h.installs))
	}
	gotSnapArgs := h.installs[0]
	if gotSnapArgs.Term != snapArgs.Term || gotSnapArgs.LeaderID != snapArgs.LeaderID ||
		gotSnapArgs.LastIncludedIndex != snapArgs.LastIncludedIndex ||
		gotSnapArgs.LastIncludedTerm != snapArgs.LastIncludedTerm ||
		gotSnapArgs.Offset != snapArgs.Offset || gotSnapArgs.Done != snapArgs.Done {
		t.Errorf("snapshot header: got %+v want %+v", gotSnapArgs, *snapArgs)
	}
	if !bytes.Equal(gotSnapArgs.Data, snapArgs.Data) {
		t.Errorf("snapshot data: got %d bytes want %d", len(gotSnapArgs.Data), len(snapArgs.Data))
	}
}

func TestNetTransportPoolsConnections(t *testing.T) {
	tr, l := newLoopback(t, newRecorder("peer"), NetConfig{})
	for i := 0; i < 20; i++ {
		if _, err := tr.AppendEntries("peer", &raft.AppendEntriesArgs{Term: 1, LeaderID: "a"}); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	tr.mu.Lock()
	idle := len(tr.idle["peer"])
	tr.mu.Unlock()
	if idle != 1 {
		t.Errorf("idle connections to peer: %d, want 1 (sequential calls should reuse)", idle)
	}
	l.mu.Lock()
	live := len(l.conns)
	l.mu.Unlock()
	if live != 1 {
		t.Errorf("server-side connections: %d, want 1", live)
	}
}

func TestNetTransportConcurrentCalls(t *testing.T) {
	tr, _ := newLoopback(t, newRecorder("peer"), NetConfig{})
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if _, err := tr.RequestVote("peer", &raft.RequestVoteArgs{Term: raft.Term(i), CandidateID: "a"}); err != nil {
					errs <- err
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent call: %v", err)
	}
}

// A peer that accepts the request and then vanishes must surface an error. The
// consensus layer retries on its own tick, but only if the call returns.
func TestNetTransportPeerVanishesMidCall(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			// Consume the request header, then drop the connection without a
			// reply, which is what a crashing process looks like.
			var hdr [4]byte
			_, _ = io.ReadFull(c, hdr[:])
			_ = c.Close()
		}
	}()

	tr := NewNetTransport(map[raft.NodeID]string{"gone": ln.Addr().String()},
		NetConfig{CallTimeout: 5 * time.Second})
	defer tr.Close()

	done := make(chan error, 1)
	go func() {
		_, err := tr.AppendEntries("gone", &raft.AppendEntriesArgs{Term: 1, LeaderID: "a"})
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error from a peer that closed mid-call")
		}
	case <-time.After(3 * time.Second):
		// Well short of CallTimeout: a closed connection must fail on the spot,
		// not wait out the deadline.
		t.Fatal("call hung after the peer went away")
	}
}

// A peer that is reachable but never answers must hit the per-call deadline.
func TestNetTransportSilentPeerHitsDeadline(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	accepted := make(chan net.Conn, 4)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				close(accepted)
				return
			}
			accepted <- c
		}
	}()
	// The listener must be closed before draining, since the accept loop only
	// closes the channel once Accept fails.
	defer func() {
		_ = ln.Close()
		for c := range accepted {
			_ = c.Close()
		}
	}()

	tr := NewNetTransport(map[raft.NodeID]string{"mute": ln.Addr().String()},
		NetConfig{CallTimeout: 150 * time.Millisecond})
	defer tr.Close()

	start := time.Now()
	_, err = tr.RequestVote("mute", &raft.RequestVoteArgs{Term: 1, CandidateID: "a"})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Errorf("error %v is not a timeout", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("call took %v, far beyond the 150ms deadline", elapsed)
	}
}

func TestNetTransportDialFailure(t *testing.T) {
	// Bind and immediately release a port so the address is routable but has
	// nothing listening.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	tr := NewNetTransport(map[raft.NodeID]string{"dead": addr}, NetConfig{DialTimeout: 250 * time.Millisecond})
	defer tr.Close()

	if _, err := tr.AppendEntries("dead", &raft.AppendEntriesArgs{Term: 1, LeaderID: "a"}); err == nil {
		t.Fatal("expected a dial error")
	}
}

func TestNetTransportUnknownPeerAndClosed(t *testing.T) {
	tr, _ := newLoopback(t, newRecorder("peer"), NetConfig{})
	if _, err := tr.RequestVote("nobody", &raft.RequestVoteArgs{}); !errors.Is(err, ErrUnknownPeer) {
		t.Errorf("unknown peer: got %v, want ErrUnknownPeer", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.RequestVote("peer", &raft.RequestVoteArgs{}); !errors.Is(err, ErrTransportClosed) {
		t.Errorf("after Close: got %v, want ErrTransportClosed", err)
	}
}

// A stale pooled connection is the normal consequence of a peer restarting, and
// must cost one silent retry rather than a failed RPC.
func TestNetTransportRetriesStalePooledConnection(t *testing.T) {
	h := newRecorder("peer")
	tr, l := newLoopback(t, h, NetConfig{})

	if _, err := tr.AppendEntries("peer", &raft.AppendEntriesArgs{Term: 1, LeaderID: "a"}); err != nil {
		t.Fatal(err)
	}

	// Kill the server side of the pooled connection behind the client's back.
	l.mu.Lock()
	for c := range l.conns {
		_ = c.Close()
	}
	l.mu.Unlock()

	// Give the close time to arrive so the pooled connection is genuinely dead.
	time.Sleep(50 * time.Millisecond)

	if _, err := tr.AppendEntries("peer", &raft.AppendEntriesArgs{Term: 2, LeaderID: "a"}); err != nil {
		t.Fatalf("stale pooled connection was not retried: %v", err)
	}
	if _, appends, _ := h.counts(); appends < 2 {
		t.Errorf("handler saw %d appends, want at least 2", appends)
	}
}

func TestListenerRejectsGarbage(t *testing.T) {
	l, err := Listen("127.0.0.1:0", newRecorder("peer"), NetConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// A valid frame carrying an unknown type byte: the listener must drop the
	// connection rather than guess at the body.
	f, err := frame(msgType(200), []byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(f); err != nil {
		t.Fatal(err)
	}
	if err := c.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Error("listener replied to an unknown message type")
	}
}

func TestListenerCloseIsIdempotent(t *testing.T) {
	l, err := Listen("127.0.0.1:0", newRecorder("peer"), NetConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// The listener satisfies nothing in package raft directly, but the transport
// must satisfy raft.Transport or the consensus layer cannot use it.
var (
	_ raft.Transport = (*NetTransport)(nil)
	_ raft.Transport = (*switchTransport)(nil)
)
