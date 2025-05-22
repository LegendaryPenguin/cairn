package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"github.com/LegendaryPenguin/cairn/raft"
)

func TestRoundTripRequestVoteArgs(t *testing.T) {
	cases := []raft.RequestVoteArgs{
		{},
		{Term: 1, CandidateID: "n1", LastLogIndex: 0, LastLogTerm: 0},
		{Term: ^raft.Term(0), CandidateID: "node-with-a-long-id", LastLogIndex: ^raft.Index(0), LastLogTerm: 7, PreVote: true},
		{Term: 4, CandidateID: "", LastLogIndex: 9, LastLogTerm: 3, PreVote: false},
	}
	for _, want := range cases {
		got, err := decodeRequestVoteArgs(encodeRequestVoteArgs(&want))
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if !reflect.DeepEqual(*got, want) {
			t.Errorf("round trip mismatch:\n got %+v\nwant %+v", *got, want)
		}
	}
}

func TestRoundTripRequestVoteReply(t *testing.T) {
	cases := []raft.RequestVoteReply{
		{},
		{Term: 12, VoteGranted: true},
		{Term: ^raft.Term(0), VoteGranted: false},
	}
	for _, want := range cases {
		got, err := decodeRequestVoteReply(encodeRequestVoteReply(&want))
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if !reflect.DeepEqual(*got, want) {
			t.Errorf("round trip mismatch:\n got %+v\nwant %+v", *got, want)
		}
	}
}

func TestRoundTripAppendEntriesArgs(t *testing.T) {
	large := bytes.Repeat([]byte{0xAB, 0x00, 0x7F}, 400_000) // 1.2 MB

	cases := []struct {
		name string
		args raft.AppendEntriesArgs
	}{
		{
			// A heartbeat: the empty-Entries case is the single most common
			// message in a healthy cluster.
			name: "heartbeat with nil entries",
			args: raft.AppendEntriesArgs{Term: 3, LeaderID: "n1", PrevLogIndex: 10, PrevLogTerm: 2, LeaderCommit: 9},
		},
		{
			name: "entry with nil data",
			args: raft.AppendEntriesArgs{
				Term: 3, LeaderID: "n1", PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 1,
				Entries: []raft.LogEntry{{Index: 2, Term: 3, Kind: raft.EntryNoop, Data: nil}},
			},
		},
		{
			name: "entry with empty data",
			args: raft.AppendEntriesArgs{
				Term: 3, LeaderID: "n1",
				Entries: []raft.LogEntry{{Index: 1, Term: 1, Kind: raft.EntryCommand, Data: []byte{}}},
			},
		},
		{
			name: "many entries mixed kinds",
			args: raft.AppendEntriesArgs{
				Term: 99, LeaderID: "leader-7", PrevLogIndex: 41, PrevLogTerm: 98, LeaderCommit: 40,
				Entries: []raft.LogEntry{
					{Index: 42, Term: 98, Kind: raft.EntryCommand, Data: []byte("set k v")},
					{Index: 43, Term: 99, Kind: raft.EntryNoop},
					{Index: 44, Term: 99, Kind: raft.EntryConfig, Data: []byte{0, 1, 2, 255}},
				},
			},
		},
		{
			name: "large payload",
			args: raft.AppendEntriesArgs{
				Term: 5, LeaderID: "n2", PrevLogIndex: 1, PrevLogTerm: 1,
				Entries: []raft.LogEntry{{Index: 2, Term: 5, Kind: raft.EntryCommand, Data: large}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeAppendEntriesArgs(encodeAppendEntriesArgs(&tc.args))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.Term != tc.args.Term || got.LeaderID != tc.args.LeaderID ||
				got.PrevLogIndex != tc.args.PrevLogIndex || got.PrevLogTerm != tc.args.PrevLogTerm ||
				got.LeaderCommit != tc.args.LeaderCommit {
				t.Fatalf("header mismatch:\n got %+v\nwant %+v", *got, tc.args)
			}
			if len(got.Entries) != len(tc.args.Entries) {
				t.Fatalf("entry count: got %d want %d", len(got.Entries), len(tc.args.Entries))
			}
			for i, want := range tc.args.Entries {
				g := got.Entries[i]
				if g.Index != want.Index || g.Term != want.Term || g.Kind != want.Kind {
					t.Errorf("entry %d header: got %+v want %+v", i, g, want)
				}
				if !bytes.Equal(g.Data, want.Data) {
					t.Errorf("entry %d data differs (got %d bytes, want %d)", i, len(g.Data), len(want.Data))
				}
			}
		})
	}
}

// Nil and empty are the same message on the wire, and both decode to nil. The
// Raft layer treats "no entries" and "zero entries" identically, so this is the
// documented behaviour rather than a lossy round trip.
func TestEmptySlicesDecodeAsNil(t *testing.T) {
	args := raft.AppendEntriesArgs{Term: 1, LeaderID: "n1", Entries: []raft.LogEntry{}}
	got, err := decodeAppendEntriesArgs(encodeAppendEntriesArgs(&args))
	if err != nil {
		t.Fatal(err)
	}
	if got.Entries != nil {
		t.Errorf("empty Entries decoded as %#v, want nil", got.Entries)
	}

	snap := raft.InstallSnapshotArgs{Term: 1, LeaderID: "n1", Data: []byte{}}
	gotSnap, err := decodeInstallSnapshotArgs(encodeInstallSnapshotArgs(&snap))
	if err != nil {
		t.Fatal(err)
	}
	if gotSnap.Data != nil {
		t.Errorf("empty Data decoded as %#v, want nil", gotSnap.Data)
	}
}

func TestRoundTripAppendEntriesReply(t *testing.T) {
	cases := []raft.AppendEntriesReply{
		{},
		{Term: 4, Success: true},
		{Term: 4, Success: false, ConflictTerm: 3, ConflictIndex: 17},
		{Term: ^raft.Term(0), Success: false, ConflictTerm: ^raft.Term(0), ConflictIndex: ^raft.Index(0)},
	}
	for _, want := range cases {
		got, err := decodeAppendEntriesReply(encodeAppendEntriesReply(&want))
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if !reflect.DeepEqual(*got, want) {
			t.Errorf("round trip mismatch:\n got %+v\nwant %+v", *got, want)
		}
	}
}

func TestRoundTripInstallSnapshotArgs(t *testing.T) {
	large := bytes.Repeat([]byte("snapshot-chunk"), 100_000)
	cases := []raft.InstallSnapshotArgs{
		{},
		{Term: 7, LeaderID: "n3", LastIncludedIndex: 100, LastIncludedTerm: 6, Offset: 0, Data: nil, Done: false},
		{Term: 7, LeaderID: "n3", LastIncludedIndex: 100, LastIncludedTerm: 6, Offset: 4096, Data: []byte{1}, Done: true},
		{Term: 8, LeaderID: "n4", LastIncludedIndex: ^raft.Index(0), LastIncludedTerm: 9, Offset: ^uint64(0), Data: large, Done: true},
	}
	for _, want := range cases {
		got, err := decodeInstallSnapshotArgs(encodeInstallSnapshotArgs(&want))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Term != want.Term || got.LeaderID != want.LeaderID ||
			got.LastIncludedIndex != want.LastIncludedIndex || got.LastIncludedTerm != want.LastIncludedTerm ||
			got.Offset != want.Offset || got.Done != want.Done {
			t.Errorf("header mismatch:\n got %+v\nwant %+v", *got, want)
		}
		if !bytes.Equal(got.Data, want.Data) {
			t.Errorf("data differs: got %d bytes, want %d", len(got.Data), len(want.Data))
		}
	}
}

func TestRoundTripInstallSnapshotReply(t *testing.T) {
	for _, want := range []raft.InstallSnapshotReply{{}, {Term: 42}, {Term: ^raft.Term(0)}} {
		got, err := decodeInstallSnapshotReply(encodeInstallSnapshotReply(&want))
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if *got != want {
			t.Errorf("round trip mismatch: got %+v want %+v", *got, want)
		}
	}
}

func TestFrameLayout(t *testing.T) {
	body := encodeRequestVoteReply(&raft.RequestVoteReply{Term: 1, VoteGranted: true})
	f, err := frame(msgRequestVoteReply, body)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.BigEndian.Uint32(f[:4]); int(got) != len(body)+1 {
		t.Errorf("length prefix %d, want %d", got, len(body)+1)
	}
	if f[4] != byte(msgRequestVoteReply) {
		t.Errorf("type byte %d, want %d", f[4], msgRequestVoteReply)
	}
	if !bytes.Equal(f[5:], body) {
		t.Error("body not preserved in frame")
	}
}

func TestDecodeRejectsCorruptInput(t *testing.T) {
	good := encodeAppendEntriesArgs(&raft.AppendEntriesArgs{
		Term: 1, LeaderID: "n1",
		Entries: []raft.LogEntry{{Index: 1, Term: 1, Data: []byte("x")}},
	})

	if _, err := decodeAppendEntriesArgs(good[:len(good)-3]); err == nil {
		t.Error("truncated frame decoded without error")
	}
	if _, err := decodeAppendEntriesArgs(append(append([]byte{}, good...), 0xFF)); !errors.Is(err, ErrMalformed) {
		t.Errorf("trailing bytes: got %v, want ErrMalformed", err)
	}
	if _, err := decodeRequestVoteArgs(nil); err == nil {
		t.Error("empty input decoded without error")
	}

	// A bogus entry count must be rejected against the remaining frame size
	// rather than trusted into a huge allocation.
	bogus := encodeAppendEntriesArgs(&raft.AppendEntriesArgs{Term: 1, LeaderID: "n1"})
	binary.BigEndian.PutUint32(bogus[len(bogus)-4:], 1<<30)
	if _, err := decodeAppendEntriesArgs(bogus); !errors.Is(err, ErrMalformed) {
		t.Errorf("bogus entry count: got %v, want ErrMalformed", err)
	}

	// Only 0 and 1 are valid booleans; anything else means the layouts disagree.
	badBool := encodeRequestVoteArgs(&raft.RequestVoteArgs{Term: 1, CandidateID: "n1"})
	badBool[len(badBool)-1] = 9
	if _, err := decodeRequestVoteArgs(badBool); !errors.Is(err, ErrMalformed) {
		t.Errorf("bad bool byte: got %v, want ErrMalformed", err)
	}
}

// The decoded Data must not alias the frame, or a reused read buffer would
// silently rewrite log entries already handed to the consensus layer.
func TestDecodedDataDoesNotAliasFrame(t *testing.T) {
	buf := encodeInstallSnapshotArgs(&raft.InstallSnapshotArgs{Term: 1, LeaderID: "n", Data: []byte("payload")})
	got, err := decodeInstallSnapshotArgs(buf)
	if err != nil {
		t.Fatal(err)
	}
	for i := range buf {
		buf[i] = 0
	}
	if string(got.Data) != "payload" {
		t.Errorf("decoded data aliased the frame: %q", got.Data)
	}
}
