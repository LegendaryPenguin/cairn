// Command cairn-node runs one member of a Cairn cluster: durable log, the
// key-value state machine, the TCP consensus transport, and the HTTP client API.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/LegendaryPenguin/cairn/kv"
	"github.com/LegendaryPenguin/cairn/raft"
	"github.com/LegendaryPenguin/cairn/server"
	"github.com/LegendaryPenguin/cairn/storage"
	"github.com/LegendaryPenguin/cairn/transport"
)

func main() {
	var (
		id                = flag.String("id", "", "this node's ID; must appear in -peers")
		peerList          = flag.String("peers", "", "comma-separated id=host:port for every member, including this one")
		dataDir           = flag.String("data", "", "directory for the log and snapshots")
		httpAddr          = flag.String("http", "127.0.0.1:8080", "listen address for the client HTTP API")
		snapshotThreshold = flag.Uint64("snapshot-threshold", 8192, "applied entries past the last snapshot before compacting")
		heartbeat         = flag.Duration("heartbeat", 50*time.Millisecond, "leader heartbeat interval")
		electionMin       = flag.Duration("election-min", 300*time.Millisecond, "lower bound of the randomized election timeout")
		electionMax       = flag.Duration("election-max", 600*time.Millisecond, "upper bound of the randomized election timeout")
		preVote           = flag.Bool("pre-vote", false, "run the pre-vote probe before incrementing the term")
		opTimeout         = flag.Duration("op-timeout", 2*time.Second, "deadline for one client write or linearizable read")
	)
	flag.Parse()

	if err := run(*id, *peerList, *dataDir, *httpAddr, *snapshotThreshold,
		*heartbeat, *electionMin, *electionMax, *opTimeout, *preVote); err != nil {
		log.SetFlags(0)
		log.Fatalf("cairn-node: %v", err)
	}
}

func run(id, peerList, dataDir, httpAddr string, snapshotThreshold uint64,
	heartbeat, electionMin, electionMax, opTimeout time.Duration, preVote bool) error {

	if id == "" {
		return errors.New("-id is required")
	}
	if dataDir == "" {
		return errors.New("-data is required")
	}
	peers, err := parsePeers(peerList)
	if err != nil {
		return err
	}
	selfAddr, ok := peers[raft.NodeID(id)]
	if !ok {
		return fmt.Errorf("-peers must include this node (%s)", id)
	}

	logger := log.New(os.Stderr, "["+id+"] ", log.LstdFlags|log.Lmicroseconds)

	store, err := storage.Open(dataDir)
	if err != nil {
		return fmt.Errorf("open storage %s: %w", dataDir, err)
	}
	// From here on every error path has to close storage, or the WAL is left with
	// an open file handle and the next start may read a partially flushed tail.
	defer func() {
		if cerr := store.Close(); cerr != nil {
			logger.Printf("closing storage: %v", cerr)
		}
	}()

	sm := kv.New()
	trans := transport.NewNetTransport(peers, transport.NetConfig{})
	defer trans.Close()

	cfg := raft.Config{
		ID:                 raft.NodeID(id),
		Peers:              sortedIDs(peers),
		HeartbeatInterval:  heartbeat,
		ElectionTimeoutMin: electionMin,
		ElectionTimeoutMax: electionMax,
		SnapshotThreshold:  snapshotThreshold,
		PreVote:            preVote,
	}
	node, err := raft.NewNode(cfg, store, sm, trans)
	if err != nil {
		return fmt.Errorf("build node: %w", err)
	}
	node.SetObserver(&roleLogger{log: logger})

	// The listener has to exist before Start so a peer that is already
	// campaigning gets an answer rather than a refused connection, which would
	// cost an extra election timeout on every cluster boot.
	raftLn, err := transport.Listen(selfAddr, node, transport.NetConfig{})
	if err != nil {
		return fmt.Errorf("listen for consensus traffic on %s: %w", selfAddr, err)
	}
	defer raftLn.Close()
	logger.Printf("consensus listening on %s, members %v", raftLn.Addr(), cfg.Peers)

	node.Start()
	defer node.Stop()

	srv := server.New(node, sm, opTimeout)
	httpLn, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return fmt.Errorf("listen for the client API on %s: %w", httpAddr, err)
	}
	httpSrv := &http.Server{Handler: server.NewHTTPHandler(srv).Routes()}
	serveErr := make(chan error, 1)
	go func() {
		err := httpSrv.Serve(httpLn)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveErr <- err
	}()
	logger.Printf("client API listening on http://%s", httpLn.Addr())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigs:
		logger.Printf("received %s, shutting down", sig)
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("client API: %w", err)
		}
		return errors.New("client API stopped unexpectedly")
	}

	// Close the client API first: once it is gone no new writes enter the node,
	// so the subsequent node.Stop has a fixed amount of in-flight work to drain.
	if err := httpSrv.Close(); err != nil {
		logger.Printf("closing client API: %v", err)
	}
	<-serveErr
	logger.Printf("stopped")
	return nil
}

// parsePeers reads the -peers flag: "n1=127.0.0.1:7001,n2=127.0.0.1:7002".
//
// Malformed input is rejected rather than skipped. A silently dropped member
// turns into a cluster whose nodes disagree about the size of the quorum, which
// is a safety bug that will not show up until an election.
func parsePeers(s string) (map[raft.NodeID]string, error) {
	fields := strings.Split(s, ",")
	peers := make(map[raft.NodeID]string, len(fields))
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		name, addr, found := strings.Cut(f, "=")
		if !found {
			return nil, fmt.Errorf("peer %q must be in id=host:port form", f)
		}
		name = strings.TrimSpace(name)
		addr = strings.TrimSpace(addr)
		if name == "" {
			return nil, fmt.Errorf("peer %q has an empty id", f)
		}
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("peer %q: %w", f, err)
		}
		if prev, dup := peers[raft.NodeID(name)]; dup {
			return nil, fmt.Errorf("peer %s listed twice (%s and %s)", name, prev, addr)
		}
		peers[raft.NodeID(name)] = addr
	}
	if len(peers) == 0 {
		return nil, errors.New("-peers is required")
	}
	return peers, nil
}

// sortedIDs gives every node the same peer ordering. Config.Quorum only cares
// about the count, but a stable order makes log lines from different members
// directly comparable.
func sortedIDs(peers map[raft.NodeID]string) []raft.NodeID {
	ids := make([]raft.NodeID, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// roleLogger turns Raft state transitions into log lines so that running a
// cluster shows elections as they happen.
type roleLogger struct {
	log *log.Logger
}

func (o *roleLogger) OnRoleChange(id raft.NodeID, term raft.Term, role raft.Role) {
	o.log.Printf("role=%s term=%d", role, term)
}

// OnCommit and OnApply fire once per entry. Logging them would bury the role
// changes under heartbeat-rate noise, so they are deliberately silent.
func (o *roleLogger) OnCommit(raft.NodeID, raft.Index) {}
func (o *roleLogger) OnApply(raft.NodeID, raft.Index)  {}
