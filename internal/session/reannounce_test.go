package session

import (
	"context"
	"net"
	"testing"
	"time"

	"go-torrent/internal/peer"
	"go-torrent/internal/piece"
	"go-torrent/internal/torrent"
	"go-torrent/internal/tracker"
)

func TestReannounceIntervalHonorsLongestTrackerInterval(t *testing.T) {
	results := []tracker.AnnounceResult{
		{URL: "udp://one", Interval: 10 * time.Minute},
		{URL: "udp://two", Interval: 30 * time.Minute},
		{URL: "udp://failed", Interval: time.Hour, Err: context.DeadlineExceeded},
	}
	if got, want := reannounceInterval(results), 30*time.Minute; got != want {
		t.Fatalf("reannounceInterval() = %s, want %s", got, want)
	}
}

func TestReannounceIntervalAppliesSafetyFloor(t *testing.T) {
	results := []tracker.AnnounceResult{{URL: "http://tracker", Interval: 5 * time.Second}}
	if got := reannounceInterval(results); got != minimumReannounceInterval {
		t.Fatalf("reannounceInterval() = %s, want floor %s", got, minimumReannounceInterval)
	}
}

func TestPeriodicReannounceAddsOnlyNewPeersAndUsesCurrentProgress(t *testing.T) {
	tf := &torrent.TorrentFile{Name: "test", Length: 100, PieceLength: 100, PieceHashes: make([][20]byte, 1)}
	m := piece.NewManager(tf, nil)
	known := tracker.Peer{IP: net.IPv4(127, 0, 0, 1), Port: 6001}
	newPeer := tracker.Peer{IP: net.IPv4(127, 0, 0, 2), Port: 6002}

	ctx, cancel := context.WithCancel(context.Background())
	waits := 0
	announceCalls := 0
	connectCalls := 0
	config := Config{
		Torrent:        tf,
		Port:           6881,
		Manager:        m,
		InitialPeers:   []tracker.Peer{known},
		InitialResults: []tracker.AnnounceResult{{URL: "udp://tracker", Interval: time.Minute}},
		Announce: func(_ context.Context, request tracker.AnnounceRequest) (tracker.DiscoveryResult, error) {
			announceCalls++
			if request.Event != tracker.EventNone || request.Downloaded != 0 || request.Left != 100 {
				t.Fatalf("announce request = %#v", request)
			}
			return tracker.DiscoveryResult{
				Peers:   []tracker.Peer{known, newPeer},
				Results: []tracker.AnnounceResult{{URL: "udp://tracker", Interval: 2 * time.Minute}},
			}, nil
		},
	}
	runPeriodicReannounceWith(
		ctx, config,
		func(candidates []tracker.Peer) ([]*peer.PeerConnection, peer.ConnectReport) {
			connectCalls++
			if len(candidates) != 1 || trackerPeerKey(candidates[0]) != trackerPeerKey(newPeer) {
				t.Fatalf("connection candidates = %#v, want only new peer", candidates)
			}
			return []*peer.PeerConnection{}, peer.ConnectReport{Attempted: 1}
		},
		func(_ context.Context, interval time.Duration) bool {
			waits++
			if waits == 1 {
				if interval != time.Minute {
					t.Fatalf("first interval = %s, want 1m", interval)
				}
				return true
			}
			if interval != 2*time.Minute {
				t.Fatalf("next interval = %s, want 2m", interval)
			}
			cancel()
			return false
		},
	)

	if announceCalls != 1 || connectCalls != 1 {
		t.Fatalf("announce calls = %d, connect calls = %d; want 1, 1", announceCalls, connectCalls)
	}
}
