// Command cairnctl is the client for a Cairn cluster.
//
// Two behaviours in here are not conveniences, they are the client half of the
// cluster's linearizability guarantee:
//
//   - A 307 is followed to the leader, because only the leader can commit a
//     write or establish a read index.
//   - A 503 carrying "unknown": true is retried with the SAME sequence number.
//     An unknown outcome means the entry may yet commit under a later leader;
//     reissuing it under a fresh sequence number would defeat the state
//     machine's dedup table and apply the command twice.
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// maxUnknownRetries bounds how long an unknown outcome is chased before the
	// operator is told the truth: still unknown.
	maxUnknownRetries = 10

	// maxRedirects bounds leader chasing. More than a couple of hops means the
	// cluster is churning leaders, not that the client guessed badly.
	maxRedirects = 5
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "cairnctl: "+err.Error())
		os.Exit(1)
	}
}

type addrList []string

func (a *addrList) String() string { return strings.Join(*a, ",") }

// Set accepts both a repeated flag and a comma-separated value so that a shell
// loop and a single pasted cluster description both work.
func (a *addrList) Set(v string) error {
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			*a = append(*a, normalizeAddr(part))
		}
	}
	return nil
}

func normalizeAddr(s string) string {
	if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
		return strings.TrimSuffix(s, "/")
	}
	return "http://" + strings.TrimSuffix(s, "/")
}

func run(args []string) error {
	fs := flag.NewFlagSet("cairnctl", flag.ContinueOnError)
	var addrs addrList
	fs.Var(&addrs, "addr", "server address; repeatable or comma-separated so a redirect can be followed")
	statePath := fs.String("state", defaultStatePath(), "file holding this client's ID and sequence number")
	stale := fs.Bool("stale", false, "for get: read local applied state without confirming leadership")
	create := fs.Bool("create", false, "for cas: require the key to be absent instead of matching <expected>")
	timeout := fs.Duration("timeout", 10*time.Second, "per-request timeout")
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), usage)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return errors.New("see -h for usage")
	}

	rest := fs.Args()
	if len(rest) == 0 {
		fs.Usage()
		return errors.New("a subcommand is required")
	}
	// Go's flag package stops at the first non-flag argument, so a flag typed
	// after the subcommand would otherwise be read as a key. Parsing the tail a
	// second time accepts both "cairnctl -stale get k" and "cairnctl get k -stale".
	if err := fs.Parse(rest[1:]); err != nil {
		return errors.New("see -h for usage")
	}
	rest = append(rest[:1:1], fs.Args()...)
	if len(addrs) == 0 {
		addrs = addrList{normalizeAddr("127.0.0.1:8080")}
	}

	c := &client{
		addrs: addrs,
		http:  &http.Client{Timeout: *timeout},
	}

	switch rest[0] {
	case "get":
		if len(rest) != 2 {
			return errors.New("usage: get <key>")
		}
		return c.get(rest[1], *stale)

	case "status":
		if len(rest) != 1 {
			return errors.New("usage: status")
		}
		return c.status()

	case "put", "del", "cas", "members":
		// Everything below here mutates replicated state and therefore needs a
		// durable (clientID, seq) pair before the first byte goes on the wire.
		st, err := loadState(*statePath)
		if err != nil {
			return err
		}
		c.state = st
		switch rest[0] {
		case "put":
			if len(rest) != 3 {
				return errors.New("usage: put <key> <value>")
			}
			return c.put(rest[1], []byte(rest[2]))
		case "del":
			if len(rest) != 2 {
				return errors.New("usage: del <key>")
			}
			return c.del(rest[1])
		case "cas":
			if *create {
				if len(rest) != 3 {
					return errors.New("usage: cas -create <key> <new>")
				}
				return c.cas(rest[1], nil, []byte(rest[2]), false)
			}
			if len(rest) != 4 {
				return errors.New("usage: cas <key> <expected> <new>")
			}
			return c.cas(rest[1], []byte(rest[2]), []byte(rest[3]), true)
		default:
			if len(rest) != 3 || (rest[1] != "add" && rest[1] != "remove") {
				return errors.New("usage: members add|remove <id>")
			}
			return c.members(rest[1], rest[2])
		}
	}
	return fmt.Errorf("unknown subcommand %q", rest[0])
}

const usage = `cairnctl drives a Cairn cluster over its HTTP API.

Flags may be given before the subcommand or immediately after it, but not
after a key or value.

  cairnctl [flags] put <key> <value>
  cairnctl [flags] get [-stale] <key>
  cairnctl [flags] del <key>
  cairnctl [flags] cas <key> <expected> <new>
  cairnctl [flags] cas -create <key> <new>
  cairnctl [flags] status
  cairnctl [flags] members add <id>
  cairnctl [flags] members remove <id>

Flags:
`

// --- client state -----------------------------------------------------------

// state is the durable half of the dedup contract.
//
// Seq must be advanced and flushed before a command is sent, never after. If two
// different commands ever shared a sequence number, the state machine would
// answer the second with the first one's result. Pending* records the command a
// sequence number was spent on so that an identical command issued after the
// process died can resume on that same number instead of burning a new one.
type state struct {
	path string

	ClientID    string `json:"clientId"`
	Seq         uint64 `json:"seq"`
	PendingSeq  uint64 `json:"pendingSeq,omitempty"`
	PendingHash string `json:"pendingHash,omitempty"`
}

func defaultStatePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".cairn-client"
	}
	return filepath.Join(home, ".cairn-client")
}

func loadState(path string) (*state, error) {
	s := &state{path: path}
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(raw, s); err != nil {
			return nil, fmt.Errorf("client state %s is corrupt: %w", path, err)
		}
	case errors.Is(err, os.ErrNotExist):
	default:
		return nil, fmt.Errorf("read client state %s: %w", path, err)
	}
	if s.ClientID == "" {
		id, err := newClientID()
		if err != nil {
			return nil, err
		}
		s.ClientID = id
	}
	s.path = path
	return s, nil
}

func newClientID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate a client ID: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// begin returns the sequence number to use for the command described by hash,
// resuming a previously spent one if this is the same command retried.
func (s *state) begin(hash string) (uint64, error) {
	if s.PendingSeq != 0 && s.PendingHash == hash {
		return s.PendingSeq, nil
	}
	s.Seq++
	s.PendingSeq = s.Seq
	s.PendingHash = hash
	return s.Seq, s.flush()
}

// settle clears the pending marker once the outcome is known either way. An
// outcome that stays unknown leaves the marker in place on purpose.
func (s *state) settle() error {
	s.PendingSeq = 0
	s.PendingHash = ""
	return s.flush()
}

func (s *state) flush() error {
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	// Write-and-rename so a crash mid-write cannot leave a truncated file that
	// would be read as "no client state" and restart the sequence at one.
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o600); err != nil {
		return fmt.Errorf("write client state: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("replace client state: %w", err)
	}
	return nil
}

// --- transport --------------------------------------------------------------

type client struct {
	addrs []string
	http  *http.Client
	state *state
}

type apiError struct {
	Error   string `json:"error"`
	Leader  string `json:"leader,omitempty"`
	Unknown bool   `json:"unknown,omitempty"`
}

type statusResponse struct {
	ID          string   `json:"ID"`
	Role        string   `json:"Role"`
	Term        uint64   `json:"Term"`
	Leader      string   `json:"Leader"`
	CommitIndex uint64   `json:"CommitIndex"`
	Keys        int      `json:"Keys"`
	Peers       []string `json:"Peers"`
}

// call issues one request and decodes a successful response into out.
//
// It handles the two cluster-level outcomes itself: a redirect is retried
// against the leader, and an unknown outcome is retried against the same node,
// both without ever changing the request body or headers, because the sequence
// number in those headers is what makes the retry safe.
func (c *client) call(method, path string, body []byte, hdr map[string]string, out any) error {
	addr := c.addrs[0]
	redirects, unknowns := 0, 0

	for {
		code, respBody, err := c.do(method, addr, path, body, hdr)
		if err != nil {
			// A node that cannot be reached at all is indistinguishable from one
			// that is down; move on to the next address we were given.
			next, ok := c.nextAddr(addr)
			if !ok {
				return err
			}
			fmt.Fprintf(os.Stderr, "cairnctl: %v; trying %s\n", err, next)
			addr = next
			continue
		}

		if code >= 200 && code < 300 {
			if out == nil {
				return nil
			}
			return json.Unmarshal(respBody, out)
		}

		var ae apiError
		_ = json.Unmarshal(respBody, &ae)
		msg := ae.Error
		if msg == "" {
			msg = strings.TrimSpace(string(respBody))
		}

		switch {
		case code == http.StatusTemporaryRedirect:
			redirects++
			if redirects > maxRedirects {
				return fmt.Errorf("gave up after %d redirects: %s", maxRedirects, msg)
			}
			leaderAddr, err := c.resolveLeader(ae.Leader)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "cairnctl: %s is not the leader, following to %s (%s)\n",
				addr, ae.Leader, leaderAddr)
			addr = leaderAddr

		case code == http.StatusServiceUnavailable && ae.Unknown:
			unknowns++
			if unknowns > maxUnknownRetries {
				return fmt.Errorf("outcome still unknown after %d retries: %s "+
					"(the write may yet commit; rerun the same command to settle it)",
					maxUnknownRetries, msg)
			}
			// Same address, same headers: the entry may already be in the
			// leader's log, and the dedup table makes a duplicate submission a
			// no-op. A linearizable read can also time out this way, and it has
			// no sequence number, hence the nil check rather than a format verb.
			if c.state != nil {
				fmt.Fprintf(os.Stderr, "cairnctl: outcome unknown (%s), retrying with seq %d\n",
					msg, c.state.PendingSeq)
			} else {
				fmt.Fprintf(os.Stderr, "cairnctl: outcome unknown (%s), retrying\n", msg)
			}
			time.Sleep(backoff(unknowns))

		case code == http.StatusServiceUnavailable:
			// No leader known here. Another member may have one.
			next, ok := c.nextAddr(addr)
			if !ok {
				return fmt.Errorf("cluster unavailable: %s", msg)
			}
			fmt.Fprintf(os.Stderr, "cairnctl: %s: %s; trying %s\n", addr, msg, next)
			addr = next
			time.Sleep(100 * time.Millisecond)

		default:
			return fmt.Errorf("%s %s: %s (HTTP %d)", method, path, msg, code)
		}
	}
}

func backoff(attempt int) time.Duration {
	d := 100 * time.Millisecond * time.Duration(attempt)
	if d > time.Second {
		d = time.Second
	}
	return d
}

func (c *client) do(method, addr, path string, body []byte, hdr map[string]string) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, addr+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	// The 307 carries a node ID, not a URL, so Go's own redirect follower cannot
	// act on it; returning it to the caller is the only way to resolve the hint.
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, raw, nil
}

// nextAddr rotates to the next configured address, reporting false once every
// address has been tried.
func (c *client) nextAddr(current string) (string, bool) {
	if len(c.addrs) < 2 {
		return "", false
	}
	for i, a := range c.addrs {
		if a == current {
			return c.addrs[(i+1)%len(c.addrs)], true
		}
	}
	return c.addrs[0], true
}

// resolveLeader maps a leader node ID onto an HTTP address.
//
// The redirect hint is a Raft node ID because that is the only name the
// consensus layer knows; the mapping to a client-API address lives with the
// operator, i.e. in the -addr flags. Asking each known address who it is is the
// honest way to bridge the two namespaces.
func (c *client) resolveLeader(leaderID string) (string, error) {
	if leaderID == "" {
		return "", errors.New("redirected but no leader is known yet")
	}
	for _, a := range c.addrs {
		code, raw, err := c.do(http.MethodGet, a, "/status", nil, nil)
		if err != nil || code != http.StatusOK {
			continue
		}
		var st statusResponse
		if json.Unmarshal(raw, &st) == nil && st.ID == leaderID {
			return a, nil
		}
	}
	return "", fmt.Errorf("leader is %s but none of the -addr values serve it", leaderID)
}

// writeHeaders carries the dedup identity on every mutating request.
func (c *client) writeHeaders(seq uint64) map[string]string {
	return map[string]string{
		"X-Cairn-Client": c.state.ClientID,
		"X-Cairn-Seq":    strconv.FormatUint(seq, 10),
	}
}

// --- subcommands ------------------------------------------------------------

type resultResponse struct {
	Op      string `json:"op"`
	Swapped bool   `json:"swapped"`
	Existed bool   `json:"existed"`
	Prior   string `json:"prior,omitempty"`
}

func (c *client) put(key string, value []byte) error {
	seq, err := c.state.begin("put\x00" + key + "\x00" + string(value))
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]string{"Value": base64.StdEncoding.EncodeToString(value)})
	if err != nil {
		return err
	}
	var res resultResponse
	if err := c.call(http.MethodPut, "/kv/"+escape(key), body, c.writeHeaders(seq), &res); err != nil {
		return err
	}
	if err := c.state.settle(); err != nil {
		return err
	}
	fmt.Printf("put %s (seq %d)", key, seq)
	if res.Existed {
		fmt.Printf(", replaced %s", quoteB64(res.Prior))
	}
	fmt.Println()
	return nil
}

func (c *client) del(key string) error {
	seq, err := c.state.begin("del\x00" + key)
	if err != nil {
		return err
	}
	var res resultResponse
	if err := c.call(http.MethodDelete, "/kv/"+escape(key), nil, c.writeHeaders(seq), &res); err != nil {
		return err
	}
	if err := c.state.settle(); err != nil {
		return err
	}
	if res.Existed {
		fmt.Printf("deleted %s (seq %d), was %s\n", key, seq, quoteB64(res.Prior))
	} else {
		fmt.Printf("%s was already absent (seq %d)\n", key, seq)
	}
	return nil
}

func (c *client) cas(key string, expected, value []byte, expectExists bool) error {
	hash := fmt.Sprintf("cas\x00%s\x00%t\x00%s\x00%s", key, expectExists, expected, value)
	seq, err := c.state.begin(hash)
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]any{
		"expected":     base64.StdEncoding.EncodeToString(expected),
		"value":        base64.StdEncoding.EncodeToString(value),
		"expectExists": expectExists,
	})
	if err != nil {
		return err
	}
	var res resultResponse
	if err := c.call(http.MethodPost, "/kv/"+escape(key)+"/cas", body, c.writeHeaders(seq), &res); err != nil {
		return err
	}
	if err := c.state.settle(); err != nil {
		return err
	}
	if res.Swapped {
		fmt.Printf("cas %s succeeded (seq %d)\n", key, seq)
		return nil
	}
	// A refused CAS is a legitimate answer about the data, not a cluster fault,
	// but the exit code still has to be non-zero so a script notices.
	if expectExists {
		fmt.Printf("cas %s refused (seq %d): current value is %s\n", key, seq, quoteB64(res.Prior))
	} else {
		fmt.Printf("cas %s refused (seq %d): key already exists with %s\n", key, seq, quoteB64(res.Prior))
	}
	return errors.New("compare-and-swap precondition not met")
}

func (c *client) get(key string, stale bool) error {
	path := "/kv/" + escape(key)
	if stale {
		path += "?stale=true"
	}
	var resp struct {
		Found bool   `json:"found"`
		Value string `json:"value"`
	}
	// A read is not deduplicated and so carries no sequence number; c.state is
	// nil here, which is why call must not touch it except on the unknown path,
	// and reads never report unknown.
	if err := c.call(http.MethodGet, path, nil, nil, &resp); err != nil {
		return err
	}
	if !resp.Found {
		fmt.Printf("%s: not found\n", key)
		return errors.New("key not found")
	}
	raw, err := base64.StdEncoding.DecodeString(resp.Value)
	if err != nil {
		return fmt.Errorf("server returned a malformed value: %w", err)
	}
	mode := "linearizable"
	if stale {
		mode = "stale"
	}
	fmt.Printf("%s = %s (%s read)\n", key, raw, mode)
	return nil
}

func (c *client) status() error {
	var st statusResponse
	if err := c.call(http.MethodGet, "/status", nil, nil, &st); err != nil {
		return err
	}
	printStatus(st)
	return nil
}

func (c *client) members(action, id string) error {
	// Membership changes go through Raft like any other write, but the HTTP
	// layer derives no dedup identity from them; the sequence number is still
	// advanced so the audit trail of what this client did stays contiguous.
	seq, err := c.state.begin("members\x00" + action + "\x00" + id)
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]string{"Node": id})
	if err != nil {
		return err
	}
	var st statusResponse
	if err := c.call(http.MethodPost, "/membership/"+action, body, c.writeHeaders(seq), &st); err != nil {
		return err
	}
	if err := c.state.settle(); err != nil {
		return err
	}
	fmt.Printf("membership %s %s committed\n", action, id)
	printStatus(st)
	return nil
}

func printStatus(st statusResponse) {
	leader := st.Leader
	if leader == "" {
		leader = "(none)"
	}
	fmt.Printf("node        %s\n", st.ID)
	fmt.Printf("role        %s\n", st.Role)
	fmt.Printf("term        %d\n", st.Term)
	fmt.Printf("leader      %s\n", leader)
	fmt.Printf("commitIndex %d\n", st.CommitIndex)
	fmt.Printf("keys        %d\n", st.Keys)
	fmt.Printf("members     %s\n", strings.Join(st.Peers, " "))
}

func quoteB64(s string) string {
	if s == "" {
		return `""`
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return strconv.Quote(s)
	}
	return strconv.Quote(string(raw))
}

// escape percent-encodes a key for use as a single path segment. ServeMux's
// {key} wildcard matches one segment, so a key containing a slash has to be
// encoded or it would be routed as a different endpoint.
func escape(key string) string {
	var b strings.Builder
	for i := 0; i < len(key); i++ {
		ch := key[i]
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' ||
			ch == '-' || ch == '_' || ch == '.' || ch == '~' {
			b.WriteByte(ch)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", ch)
	}
	return b.String()
}
