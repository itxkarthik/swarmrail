package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/itxkarthik/swarmrail/internal/peer"
	"github.com/itxkarthik/swarmrail/internal/tracker"
)

func TestDiscoverPeerConnectionsRetriesAfterNoUsableConnections(t *testing.T) {
	first := tracker.Peer{IP: net.IPv4(127, 0, 0, 1), Port: 6001}
	second := tracker.Peer{IP: net.IPv4(127, 0, 0, 2), Port: 6002}
	announceCalls := 0
	connectCalls := 0

	peers, connections, report, err := discoverPeerConnections(
		context.Background(),
		true,
		func(ctx context.Context) ([]tracker.Peer, error) {
			announceCalls++
			remaining := time.Until(deadline(t, ctx))
			if announceCalls == 1 {
				if remaining > 4*time.Second {
					t.Fatalf("first deadline = %s, want about 3s", remaining)
				}
				return []tracker.Peer{first}, nil
			}
			if remaining < 8*time.Second {
				t.Fatalf("fallback deadline = %s, want about 10s", remaining)
			}
			return []tracker.Peer{first, second}, nil
		},
		func(discovered []tracker.Peer) ([]*peer.PeerConnection, peer.ConnectReport) {
			connectCalls++
			if connectCalls == 1 {
				return nil, peer.ConnectReport{Attempted: len(discovered)}
			}
			if len(discovered) != 2 {
				t.Fatalf("fallback received %d merged peers, want 2", len(discovered))
			}
			return []*peer.PeerConnection{{}}, peer.ConnectReport{Attempted: len(discovered), Handshaken: 1}
		},
		func(string) {},
	)

	if err != nil {
		t.Fatalf("discoverPeerConnections() error = %v", err)
	}
	if announceCalls != 2 || connectCalls != 2 {
		t.Fatalf("announce calls = %d, connect calls = %d; want 2, 2", announceCalls, connectCalls)
	}
	if len(peers) != 2 || len(connections) != 1 {
		t.Fatalf("peers = %d, connections = %d; want 2, 1", len(peers), len(connections))
	}
	if report.Attempted != 3 || report.Handshaken != 1 {
		t.Fatalf("attempted = %d, handshaken = %d; want 3, 1", report.Attempted, report.Handshaken)
	}
}

func TestDiscoverPeerConnectionsStopsAfterSuccessfulFirstPass(t *testing.T) {
	announceCalls := 0
	connectCalls := 0
	_, connections, _, err := discoverPeerConnections(
		context.Background(),
		true,
		func(context.Context) ([]tracker.Peer, error) {
			announceCalls++
			return []tracker.Peer{{IP: net.IPv4(127, 0, 0, 1), Port: 6001}}, nil
		},
		func([]tracker.Peer) ([]*peer.PeerConnection, peer.ConnectReport) {
			connectCalls++
			return []*peer.PeerConnection{{}}, peer.ConnectReport{Handshaken: 1}
		},
		func(string) {},
	)

	if err != nil {
		t.Fatalf("discoverPeerConnections() error = %v", err)
	}
	if announceCalls != 1 || connectCalls != 1 || len(connections) != 1 {
		t.Fatalf("announce calls = %d, connect calls = %d, connections = %d; want 1, 1, 1",
			announceCalls, connectCalls, len(connections))
	}
}

func TestDiscoverPeerConnectionsSkipsFallbackWhenWebseedAvailable(t *testing.T) {
	announceCalls := 0
	_, connections, _, _ := discoverPeerConnections(
		context.Background(),
		false,
		func(context.Context) ([]tracker.Peer, error) {
			announceCalls++
			return nil, nil
		},
		func([]tracker.Peer) ([]*peer.PeerConnection, peer.ConnectReport) {
			t.Fatal("connect called without discovered peers")
			return nil, peer.ConnectReport{}
		},
		func(string) {},
	)

	if announceCalls != 1 || len(connections) != 0 {
		t.Fatalf("announce calls = %d, connections = %d; want 1, 0", announceCalls, len(connections))
	}
}

func deadline(t *testing.T, ctx context.Context) time.Time {
	t.Helper()
	value, ok := ctx.Deadline()
	if !ok {
		t.Fatal("announce context has no deadline")
	}
	return value
}
