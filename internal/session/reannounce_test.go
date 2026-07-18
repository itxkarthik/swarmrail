package session

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/itxkarthik/GoTorrent/internal/peer"
	"github.com/itxkarthik/GoTorrent/internal/piece"
	"github.com/itxkarthik/GoTorrent/internal/torrent"
	"github.com/itxkarthik/GoTorrent/internal/tracker"
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
		func(_ context.Context, interval time.Duration, _ <-chan struct{}) reannounceWake {
			waits++
			if waits == 1 {
				if interval != time.Minute {
					t.Fatalf("first interval = %s, want 1m", interval)
				}
				return wakePeriodic
			}
			if interval != 2*time.Minute {
				t.Fatalf("next interval = %s, want 2m", interval)
			}
			cancel()
			return wakeStopped
		},
	)

	if announceCalls != 1 || connectCalls != 1 {
		t.Fatalf("announce calls = %d, connect calls = %d; want 1, 1", announceCalls, connectCalls)
	}
}

func TestManualReannounceWakesPeriodicLoopImmediately(t *testing.T) {
	tf := &torrent.TorrentFile{Name: "test", Length: 100, PieceLength: 100, PieceHashes: make([][20]byte, 1)}
	m := piece.NewManager(tf, nil)
	controller := NewReannounceController(time.Minute)
	announceCalls := 0
	waits := 0
	ctx := context.Background()
	runPeriodicReannounceWith(ctx, Config{
		Torrent: tf, Manager: m, Controller: controller,
		InitialResults: []tracker.AnnounceResult{{URL: "udp://tracker", Interval: time.Hour}},
		Announce: func(context.Context, tracker.AnnounceRequest) (tracker.DiscoveryResult, error) {
			announceCalls++
			return tracker.DiscoveryResult{}, nil
		},
	}, func([]tracker.Peer) ([]*peer.PeerConnection, peer.ConnectReport) {
		return nil, peer.ConnectReport{}
	}, func(_ context.Context, interval time.Duration, requests <-chan struct{}) reannounceWake {
		waits++
		if waits == 1 {
			if interval != time.Hour || requests == nil {
				t.Fatalf("wait interval=%s requests=%v, want 1h and manual channel", interval, requests)
			}
			return wakeManual
		}
		return wakeStopped
	})
	if announceCalls != 1 {
		t.Fatalf("manual announce calls = %d, want 1", announceCalls)
	}
}

func TestReannounceControllerEnforcesCooldown(t *testing.T) {
	now := time.Unix(100, 0)
	controller := NewReannounceController(time.Minute)
	controller.now = func() time.Time { return now }
	controller.MarkAttempt()
	controller.FinishAttempt()

	result := controller.Request()
	if result.Accepted || result.RetryAfter != time.Minute {
		t.Fatalf("Request() = %+v, want rejected with 1m retry", result)
	}
	now = now.Add(61 * time.Second)
	result = controller.Request()
	if !result.Accepted || result.RetryAfter != 0 {
		t.Fatalf("Request() = %+v, want accepted", result)
	}
	select {
	case <-controller.Requests():
	default:
		t.Fatal("accepted request was not queued")
	}
}

func TestReannounceControllerRejectsWhileAttemptActive(t *testing.T) {
	controller := NewReannounceController(0)
	if result := controller.Request(); !result.Accepted {
		t.Fatalf("first Request() = %+v, want accepted", result)
	}
	if result := controller.Request(); result.Accepted {
		t.Fatalf("second Request() = %+v, want rejected while active", result)
	}
	<-controller.Requests()
	controller.FinishAttempt()
	if result := controller.Request(); !result.Accepted {
		t.Fatalf("Request() after FinishAttempt = %+v, want accepted", result)
	}
}
