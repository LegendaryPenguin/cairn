package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/LegendaryPenguin/cairn/kv"
	"github.com/LegendaryPenguin/cairn/raft"
)

// HTTPHandler exposes the client API over HTTP so the CLI and the benchmarks can
// drive a real cluster without linking against the server package.
//
// Values are base64-encoded in JSON rather than sent as raw strings, because the
// store holds arbitrary bytes and silently mangling a non-UTF-8 value into
// U+FFFD would be a data-loss bug that only shows up for binary payloads.
type HTTPHandler struct {
	srv *Server
}

func NewHTTPHandler(srv *Server) *HTTPHandler { return &HTTPHandler{srv: srv} }

func (h *HTTPHandler) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", h.status)
	mux.HandleFunc("GET /kv/{key}", h.get)
	mux.HandleFunc("PUT /kv/{key}", h.put)
	mux.HandleFunc("DELETE /kv/{key}", h.del)
	mux.HandleFunc("POST /kv/{key}/cas", h.cas)
	mux.HandleFunc("POST /membership/add", h.addVoter)
	mux.HandleFunc("POST /membership/remove", h.removeVoter)
	return mux
}

type valueResponse struct {
	Found bool   `json:"found"`
	Value string `json:"value,omitempty"` // base64
}

type resultResponse struct {
	Op      string `json:"op"`
	Swapped bool   `json:"swapped"`
	Existed bool   `json:"existed"`
	Prior   string `json:"prior,omitempty"` // base64
}

type errorResponse struct {
	Error string `json:"error"`
	// Leader is set on a redirect so the client can retry against the right node
	// instead of polling every member.
	Leader string `json:"leader,omitempty"`
	// Unknown marks an outcome the cluster genuinely cannot report: the write may
	// still commit later. A client seeing this must retry with the SAME sequence
	// number rather than treating it as a failure.
	Unknown bool `json:"unknown,omitempty"`
}

func (h *HTTPHandler) status(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.srv.Status())
}

func (h *HTTPHandler) get(w http.ResponseWriter, r *http.Request) {
	mode := Linearizable
	// Opting out has to be explicit. Defaulting to a stale read would make the
	// fast path the silent default and hide the correctness cost.
	if r.URL.Query().Get("stale") == "true" {
		mode = Stale
	}

	v, ok, err := h.srv.Get(r.PathValue("key"), mode)
	if err != nil {
		writeError(w, err)
		return
	}
	resp := valueResponse{Found: ok}
	if ok {
		resp.Value = base64.StdEncoding.EncodeToString(v)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *HTTPHandler) put(w http.ResponseWriter, r *http.Request) {
	clientID, seq, err := clientSeq(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	var body struct{ Value string }
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "malformed body: " + err.Error()})
		return
	}
	val, err := base64.StdEncoding.DecodeString(body.Value)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "value must be base64: " + err.Error()})
		return
	}

	res, err := h.srv.Put(clientID, seq, r.PathValue("key"), val)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toResultResponse(res))
}

func (h *HTTPHandler) del(w http.ResponseWriter, r *http.Request) {
	clientID, seq, err := clientSeq(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	res, err := h.srv.Delete(clientID, seq, r.PathValue("key"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toResultResponse(res))
}

func (h *HTTPHandler) cas(w http.ResponseWriter, r *http.Request) {
	clientID, seq, err := clientSeq(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	var body struct {
		Expected     string `json:"expected"`
		Value        string `json:"value"`
		ExpectExists bool   `json:"expectExists"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "malformed body: " + err.Error()})
		return
	}
	expected, err1 := base64.StdEncoding.DecodeString(body.Expected)
	value, err2 := base64.StdEncoding.DecodeString(body.Value)
	if err1 != nil || err2 != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "expected and value must be base64"})
		return
	}

	res, err := h.srv.CompareAndSwap(clientID, seq, r.PathValue("key"), expected, value, body.ExpectExists)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toResultResponse(res))
}

func (h *HTTPHandler) addVoter(w http.ResponseWriter, r *http.Request) {
	h.membership(w, r, true)
}

func (h *HTTPHandler) removeVoter(w http.ResponseWriter, r *http.Request) {
	h.membership(w, r, false)
}

func (h *HTTPHandler) membership(w http.ResponseWriter, r *http.Request, add bool) {
	var body struct{ Node string }
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Node == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "node is required"})
		return
	}
	var err error
	if add {
		err = h.srv.AddVoter(raft.NodeID(body.Node))
	} else {
		err = h.srv.RemoveVoter(raft.NodeID(body.Node))
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, h.srv.Status())
}

func clientSeq(r *http.Request) (string, uint64, error) {
	id := r.Header.Get("X-Cairn-Client")
	if id == "" {
		// Refusing rather than generating an ID is deliberate: a server-generated
		// client ID would make every retry look like a new client and silently
		// defeat deduplication, which is the one thing standing between a retried
		// write and a double apply.
		return "", 0, errors.New("X-Cairn-Client header is required for dedup")
	}
	raw := r.Header.Get("X-Cairn-Seq")
	if raw == "" {
		return "", 0, errors.New("X-Cairn-Seq header is required for dedup")
	}
	seq, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return "", 0, errors.New("X-Cairn-Seq must be a uint64")
	}
	return id, seq, nil
}

func toResultResponse(r *kv.Result) resultResponse {
	out := resultResponse{Op: r.Op.String(), Swapped: r.Swapped, Existed: r.Existed}
	if r.Prior != nil {
		out.Prior = base64.StdEncoding.EncodeToString(r.Prior)
	}
	return out
}

// writeError maps the consensus layer's error taxonomy onto HTTP status codes.
//
// The distinction that matters most is 503-with-Unknown versus a plain failure:
// a timed-out write may still commit, and a client that treats it as a clean
// failure and reissues under a new sequence number can double-apply it.
func writeError(w http.ResponseWriter, err error) {
	var nle *raft.NotLeaderError
	if errors.As(err, &nle) {
		writeJSON(w, http.StatusTemporaryRedirect, errorResponse{
			Error:  err.Error(),
			Leader: string(nle.LeaderHint),
		})
		return
	}
	switch {
	case errors.Is(err, raft.ErrNoLeader):
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
	case errors.Is(err, raft.ErrTimeout), errors.Is(err, raft.ErrLeadershipLost):
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error(), Unknown: true})
	case errors.Is(err, raft.ErrShutdown):
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error(), Unknown: true})
	default:
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
