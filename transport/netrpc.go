package transport

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/LegendaryPenguin/cairn/raft"
)

// ErrTransportClosed means the transport or listener has been shut down.
var ErrTransportClosed = errors.New("cairn/transport: closed")

// NetConfig tunes the TCP transport. Zero fields take the defaults below.
type NetConfig struct {
	// DialTimeout bounds connection establishment. It is separate from
	// CallTimeout because a peer whose host is gone fails to dial quickly
	// (refused) or slowly (packets into a void), and the leader should give up
	// on an unroutable peer well before it would give up on a slow one.
	DialTimeout time.Duration

	// CallTimeout bounds one request/response exchange on an established
	// connection. raft.Transport forbids blocking indefinitely, so every call
	// carries a deadline.
	CallTimeout time.Duration

	// MaxIdlePerPeer caps pooled idle connections per peer. Pooling matters
	// because a leader sends a heartbeat to every follower several times a
	// second and a TCP handshake per heartbeat would dominate both latency and
	// the port table.
	MaxIdlePerPeer int
}

func DefaultNetConfig() NetConfig {
	return NetConfig{
		DialTimeout:    500 * time.Millisecond,
		CallTimeout:    2 * time.Second,
		MaxIdlePerPeer: 4,
	}
}

func (c *NetConfig) withDefaults() NetConfig {
	d := DefaultNetConfig()
	out := *c
	if out.DialTimeout <= 0 {
		out.DialTimeout = d.DialTimeout
	}
	if out.CallTimeout <= 0 {
		out.CallTimeout = d.CallTimeout
	}
	if out.MaxIdlePerPeer <= 0 {
		out.MaxIdlePerPeer = d.MaxIdlePerPeer
	}
	return out
}

// NetTransport is a raft.Transport over TCP. It is safe for concurrent use by
// as many goroutines as the consensus layer cares to run.
type NetTransport struct {
	cfg NetConfig

	mu     sync.Mutex
	addrs  map[raft.NodeID]string
	idle   map[raft.NodeID][]*clientConn
	closed bool
}

// NewNetTransport builds a transport that reaches the given peers at the given
// "host:port" addresses.
func NewNetTransport(peers map[raft.NodeID]string, cfg NetConfig) *NetTransport {
	t := &NetTransport{
		cfg:   cfg.withDefaults(),
		addrs: make(map[raft.NodeID]string, len(peers)),
		idle:  make(map[raft.NodeID][]*clientConn),
	}
	for id, addr := range peers {
		t.addrs[id] = addr
	}
	return t
}

// SetPeer adds or updates a peer address. Existing pooled connections to that
// peer are dropped, since they point at the old address.
func (t *NetTransport) SetPeer(id raft.NodeID, addr string) {
	t.mu.Lock()
	old := t.idle[id]
	delete(t.idle, id)
	t.addrs[id] = addr
	t.mu.Unlock()
	for _, c := range old {
		c.close()
	}
}

// RemovePeer forgets a peer and closes its pooled connections.
func (t *NetTransport) RemovePeer(id raft.NodeID) {
	t.mu.Lock()
	old := t.idle[id]
	delete(t.idle, id)
	delete(t.addrs, id)
	t.mu.Unlock()
	for _, c := range old {
		c.close()
	}
}

// Close releases every pooled connection. Calls after Close fail immediately.
func (t *NetTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	all := t.idle
	t.idle = make(map[raft.NodeID][]*clientConn)
	t.mu.Unlock()
	for _, conns := range all {
		for _, c := range conns {
			c.close()
		}
	}
	return nil
}

type clientConn struct {
	nc net.Conn
	br *bufio.Reader
}

func (c *clientConn) close() {
	_ = c.nc.Close()
}

// get returns a pooled connection if one is available, otherwise dials. The
// bool reports whether the connection was reused, which the caller needs in
// order to decide whether a failure is worth one retry.
func (t *NetTransport) get(to raft.NodeID) (*clientConn, bool, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, false, ErrTransportClosed
	}
	addr, ok := t.addrs[to]
	if !ok {
		t.mu.Unlock()
		return nil, false, fmt.Errorf("%w: %s", ErrUnknownPeer, to)
	}
	if pool := t.idle[to]; len(pool) > 0 {
		c := pool[len(pool)-1]
		t.idle[to] = pool[:len(pool)-1]
		t.mu.Unlock()
		return c, true, nil
	}
	timeout := t.cfg.DialTimeout
	t.mu.Unlock()

	nc, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, false, fmt.Errorf("cairn/transport: dial %s (%s): %w", to, addr, err)
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		// Raft messages are small and latency-sensitive; waiting to coalesce
		// them with a following write adds delay to every heartbeat.
		_ = tc.SetNoDelay(true)
	}
	return &clientConn{nc: nc, br: bufio.NewReader(nc)}, false, nil
}

// put returns a healthy connection to the pool, closing it if the pool is full
// or the transport has shut down.
func (t *NetTransport) put(to raft.NodeID, c *clientConn) {
	t.mu.Lock()
	if t.closed || len(t.idle[to]) >= t.cfg.MaxIdlePerPeer {
		t.mu.Unlock()
		c.close()
		return
	}
	t.idle[to] = append(t.idle[to], c)
	t.mu.Unlock()
}

// roundTrip sends one request and reads one reply, retrying once on a fresh
// connection if a pooled one failed.
//
// The retry is confined to reused connections: a pooled connection can have
// been closed by the peer while it sat idle, and that failure says nothing about
// the peer's health, so failing the RPC would make every leader restart cost a
// spurious round of election timeouts. A freshly dialed connection that fails
// is reporting something real and the error is returned. Retransmitting is safe
// in any case because Raft RPCs are idempotent, which the duplicate injection
// in the in-memory transport exists to keep true.
func (t *NetTransport) roundTrip(to raft.NodeID, reqType msgType, body []byte) (msgType, []byte, error) {
	req, err := frame(reqType, body)
	if err != nil {
		return 0, nil, err
	}
	for attempt := 0; ; attempt++ {
		c, reused, err := t.get(to)
		if err != nil {
			return 0, nil, err
		}
		rt, rb, err := t.exchange(c, req)
		if err == nil {
			t.put(to, c)
			return rt, rb, nil
		}
		c.close()
		if reused && attempt == 0 {
			continue
		}
		return 0, nil, fmt.Errorf("cairn/transport: call %s to %s: %w", reqType, to, err)
	}
}

func (t *NetTransport) exchange(c *clientConn, req []byte) (msgType, []byte, error) {
	// One deadline covers write and read together, so a peer that accepts the
	// bytes and then stops responding still fails inside CallTimeout.
	if err := c.nc.SetDeadline(time.Now().Add(t.cfg.CallTimeout)); err != nil {
		return 0, nil, err
	}
	if _, err := c.nc.Write(req); err != nil {
		return 0, nil, err
	}
	return readFrame(c.br)
}

func (t *NetTransport) RequestVote(to raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	rt, rb, err := t.roundTrip(to, msgRequestVoteArgs, encodeRequestVoteArgs(args))
	if err != nil {
		return nil, err
	}
	if rt != msgRequestVoteReply {
		return nil, fmt.Errorf("%w: expected RequestVoteReply, got %s", ErrMalformed, rt)
	}
	return decodeRequestVoteReply(rb)
}

func (t *NetTransport) AppendEntries(to raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	rt, rb, err := t.roundTrip(to, msgAppendEntriesArgs, encodeAppendEntriesArgs(args))
	if err != nil {
		return nil, err
	}
	if rt != msgAppendEntriesReply {
		return nil, fmt.Errorf("%w: expected AppendEntriesReply, got %s", ErrMalformed, rt)
	}
	return decodeAppendEntriesReply(rb)
}

func (t *NetTransport) InstallSnapshot(to raft.NodeID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	rt, rb, err := t.roundTrip(to, msgInstallSnapshotArgs, encodeInstallSnapshotArgs(args))
	if err != nil {
		return nil, err
	}
	if rt != msgInstallSnapshotReply {
		return nil, fmt.Errorf("%w: expected InstallSnapshotReply, got %s", ErrMalformed, rt)
	}
	return decodeInstallSnapshotReply(rb)
}

// --- framing ---

func readFrame(r *bufio.Reader) (msgType, []byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n < 1 {
		return 0, nil, fmt.Errorf("%w: zero-length frame", ErrMalformed)
	}
	if n > maxFrameSize {
		// The stream cannot be resynchronized after this, so the caller must
		// drop the connection rather than try to skip the frame.
		return 0, nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, err
	}
	return msgType(buf[0]), buf[1:], nil
}

// --- server side ---

// Listener accepts connections and dispatches decoded RPCs to a
// raft.RPCHandler.
type Listener struct {
	ln      net.Listener
	handler raft.RPCHandler
	cfg     NetConfig

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

// Listen binds addr (use "127.0.0.1:0" in tests to get an ephemeral port) and
// starts serving. Call Addr for the chosen address.
func Listen(addr string, h raft.RPCHandler, cfg NetConfig) (*Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	l := &Listener{
		ln:      ln,
		handler: h,
		cfg:     cfg.withDefaults(),
		conns:   make(map[net.Conn]struct{}),
	}
	l.wg.Add(1)
	go l.accept()
	return l, nil
}

func (l *Listener) Addr() net.Addr { return l.ln.Addr() }

func (l *Listener) accept() {
	defer l.wg.Done()
	for {
		nc, err := l.ln.Accept()
		if err != nil {
			return
		}
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			_ = nc.Close()
			return
		}
		l.conns[nc] = struct{}{}
		l.mu.Unlock()

		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			l.serve(nc)
		}()
	}
}

// serve handles requests on one connection until it fails or closes. There is
// no read deadline between requests: a pooled client connection is legitimately
// idle for as long as the leader has nothing to say, and timing it out would
// force a redial on the next heartbeat.
func (l *Listener) serve(nc net.Conn) {
	defer func() {
		l.mu.Lock()
		delete(l.conns, nc)
		l.mu.Unlock()
		_ = nc.Close()
	}()

	br := bufio.NewReader(nc)
	for {
		if err := nc.SetReadDeadline(time.Time{}); err != nil {
			return
		}
		reqType, body, err := readFrame(br)
		if err != nil {
			return
		}
		respType, resp, err := l.dispatch(reqType, body)
		if err != nil {
			// A malformed request leaves the stream position untrustworthy, so
			// the connection goes rather than risk misreading the next frame.
			return
		}
		out, err := frame(respType, resp)
		if err != nil {
			return
		}
		if err := nc.SetWriteDeadline(time.Now().Add(l.cfg.CallTimeout)); err != nil {
			return
		}
		if _, err := nc.Write(out); err != nil {
			return
		}
	}
}

func (l *Listener) dispatch(t msgType, body []byte) (msgType, []byte, error) {
	switch t {
	case msgRequestVoteArgs:
		args, err := decodeRequestVoteArgs(body)
		if err != nil {
			return 0, nil, err
		}
		reply := l.handler.HandleRequestVote(args)
		if reply == nil {
			return 0, nil, errNilReply
		}
		return msgRequestVoteReply, encodeRequestVoteReply(reply), nil

	case msgAppendEntriesArgs:
		args, err := decodeAppendEntriesArgs(body)
		if err != nil {
			return 0, nil, err
		}
		reply := l.handler.HandleAppendEntries(args)
		if reply == nil {
			return 0, nil, errNilReply
		}
		return msgAppendEntriesReply, encodeAppendEntriesReply(reply), nil

	case msgInstallSnapshotArgs:
		args, err := decodeInstallSnapshotArgs(body)
		if err != nil {
			return 0, nil, err
		}
		reply := l.handler.HandleInstallSnapshot(args)
		if reply == nil {
			return 0, nil, errNilReply
		}
		return msgInstallSnapshotReply, encodeInstallSnapshotReply(reply), nil
	}
	return 0, nil, fmt.Errorf("%w: %d", ErrUnknownMessage, uint8(t))
}

// Close stops accepting, tears down live connections so blocked reads return,
// and waits for the serving goroutines to finish.
func (l *Listener) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	conns := make([]net.Conn, 0, len(l.conns))
	for c := range l.conns {
		conns = append(conns, c)
	}
	l.mu.Unlock()

	err := l.ln.Close()
	for _, c := range conns {
		_ = c.Close()
	}
	l.wg.Wait()
	return err
}
