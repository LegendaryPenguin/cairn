package main

import (
	"testing"

	"github.com/LegendaryPenguin/cairn/raft"
)

func TestParsePeersValid(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want map[raft.NodeID]string
	}{
		{
			name: "single",
			in:   "n1=127.0.0.1:7001",
			want: map[raft.NodeID]string{"n1": "127.0.0.1:7001"},
		},
		{
			name: "three members",
			in:   "n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003",
			want: map[raft.NodeID]string{
				"n1": "127.0.0.1:7001",
				"n2": "127.0.0.1:7002",
				"n3": "127.0.0.1:7003",
			},
		},
		{
			name: "surrounding whitespace and a trailing comma",
			in:   " n1 = 127.0.0.1:7001 , n2=127.0.0.1:7002, ",
			want: map[raft.NodeID]string{"n1": "127.0.0.1:7001", "n2": "127.0.0.1:7002"},
		},
		{
			name: "hostnames and ipv6 literals",
			in:   "a=host.example:9000,b=[::1]:9001",
			want: map[raft.NodeID]string{"a": "host.example:9000", "b": "[::1]:9001"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePeers(tc.in)
			if err != nil {
				t.Fatalf("parsePeers(%q) = error %v", tc.in, err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("parsePeers(%q) = %v, want %v", tc.in, got, tc.want)
			}
			for id, addr := range tc.want {
				if got[id] != addr {
					t.Errorf("peer %s = %q, want %q", id, got[id], addr)
				}
			}
		})
	}
}

// A malformed peer list must fail the process rather than quietly yield a
// smaller cluster: dropping a member changes the quorum size, and the two halves
// of a cluster that disagree about quorum can both elect a leader.
func TestParsePeersRejectsMalformedInput(t *testing.T) {
	cases := []struct{ name, in string }{
		{"empty", ""},
		{"only separators", " , , "},
		{"missing equals", "n1 127.0.0.1:7001"},
		{"missing id", "=127.0.0.1:7001"},
		{"missing port", "n1=127.0.0.1"},
		{"empty address", "n1="},
		{"duplicate id", "n1=127.0.0.1:7001,n1=127.0.0.1:7002"},
		{"one bad entry among good ones", "n1=127.0.0.1:7001,n2=oops,n3=127.0.0.1:7003"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := parsePeers(tc.in); err == nil {
				t.Fatalf("parsePeers(%q) = %v, want an error", tc.in, got)
			}
		})
	}
}

func TestSortedIDsIsDeterministic(t *testing.T) {
	peers := map[raft.NodeID]string{
		"n3": "127.0.0.1:7003",
		"n1": "127.0.0.1:7001",
		"n2": "127.0.0.1:7002",
	}
	want := []raft.NodeID{"n1", "n2", "n3"}
	for range 10 {
		got := sortedIDs(peers)
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("sortedIDs = %v, want %v", got, want)
			}
		}
	}
}
