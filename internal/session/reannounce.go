package session

import (
	"context"
	"fmt"
	"time"

	"github.com/itxkarthik/GoTorrent/internal/peer"
	"github.com/itxkarthik/GoTorrent/internal/piece"
	"github.com/itxkarthik/GoTorrent/internal/torrent"
	"github.com/itxkarthik/GoTorrent/internal/tracker"
)

const (
	minimumReannounceInterval = time.Minute
	failedReannounceInterval  = 5 * time.Minute
	periodicAnnounceTimeout   = 10 * time.Second
)

type AnnounceFunc func(context.Context, tracker.AnnounceRequest) (tracker.DiscoveryResult, error)

type Config struct {
	Torrent        *torrent.TorrentFile
	PeerID         [20]byte
	Port           uint16
	Manager        *piece.Manager
	InitialPeers   []tracker.Peer
	InitialResults []tracker.AnnounceResult
	Announce       AnnounceFunc
	Controller     *ReannounceController
}

type periodicConnectFunc func([]tracker.Peer) ([]*peer.PeerConnection, peer.ConnectReport)
type reannounceWake uint8

const (
	wakeStopped reannounceWake = iota
	wakePeriodic
	wakeManual
)

type reannounceWaitFunc func(context.Context, time.Duration, <-chan struct{}) reannounceWake

func RunPeriodicReannounce(ctx context.Context, config Config) {
	runPeriodicReannounceWith(
		ctx, config,
		func(peers []tracker.Peer) ([]*peer.PeerConnection, peer.ConnectReport) {
			return peer.ConnectToPeersWithContext(ctx, config.Torrent, peers, config.PeerID)
		},
		waitForReannounce,
	)
}

func runPeriodicReannounceWith(
	ctx context.Context,
	config Config,
	connect periodicConnectFunc,
	wait reannounceWaitFunc,
) {
	known := make(map[string]struct{}, len(config.InitialPeers))
	markKnownPeers(known, config.InitialPeers)
	interval := reannounceInterval(config.InitialResults)

	for {
		var requests <-chan struct{}
		if config.Controller != nil {
			requests = config.Controller.Requests()
		}
		wake := wait(ctx, interval, requests)
		if wake == wakeStopped {
			return
		}
		if config.Controller != nil && wake == wakePeriodic {
			config.Controller.MarkAttempt()
		}

		completed := config.Manager.CompletedBytes()
		request := tracker.AnnounceRequest{
			InfoHash:   config.Torrent.InfoHash,
			PeerID:     config.PeerID,
			Port:       config.Port,
			Downloaded: completed,
			Left:       config.Manager.TotalLength() - completed,
			Event:      tracker.EventNone,
		}

		config.Manager.SetStatus("Reannouncing to trackers...")
		announceCtx, cancelAnnounce := context.WithTimeout(ctx, periodicAnnounceTimeout)
		discovery, err := config.Announce(announceCtx, request)
		cancelAnnounce()
		if config.Controller != nil {
			config.Controller.FinishAttempt()
		}
		if err != nil {
			config.Manager.SetStatus(fmt.Sprintf("Tracker reannounce failed; retrying in %s", failedReannounceInterval))
			interval = failedReannounceInterval
		} else {
			interval = reannounceInterval(discovery.Results)
			newPeers := filterNewPeers(known, discovery.Peers)
			if len(newPeers) > 0 {
				config.Manager.SetStatus(fmt.Sprintf("Connecting to %d new peer(s)...", len(newPeers)))
				connections, _ := connect(newPeers)
				config.Manager.AddPeers(connections)
			}
			if peerCount := len(config.Manager.Peers()); peerCount > 0 {
				config.Manager.SetStatus(fmt.Sprintf("Downloading with %d peer(s)", peerCount))
			} else {
				config.Manager.SetStatus(fmt.Sprintf("Downloading with %d webseed(s)", config.Manager.WebSeedCount()))
			}
		}
	}
}

func waitForReannounce(ctx context.Context, interval time.Duration, requests <-chan struct{}) reannounceWake {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-timer.C:
		return wakePeriodic
	case <-requests:
		return wakeManual
	case <-ctx.Done():
		return wakeStopped
	}
}

func reannounceInterval(results []tracker.AnnounceResult) time.Duration {
	interval := time.Duration(0)
	for _, result := range results {
		if result.Err == nil && result.Interval > interval {
			interval = result.Interval
		}
	}
	if interval == 0 {
		interval = tracker.DefaultAnnounceInterval
	}
	return max(interval, minimumReannounceInterval)
}

func MergeAnnounceResults(existing, latest []tracker.AnnounceResult) []tracker.AnnounceResult {
	byURL := make(map[string]tracker.AnnounceResult, len(existing)+len(latest))
	order := make([]string, 0, len(existing)+len(latest))
	for _, result := range append(existing, latest...) {
		if _, seen := byURL[result.URL]; !seen {
			order = append(order, result.URL)
		}
		byURL[result.URL] = result
	}
	merged := make([]tracker.AnnounceResult, 0, len(order))
	for _, url := range order {
		merged = append(merged, byURL[url])
	}
	return merged
}

func filterNewPeers(known map[string]struct{}, candidates []tracker.Peer) []tracker.Peer {
	newPeers := make([]tracker.Peer, 0, len(candidates))
	for _, candidate := range candidates {
		key := trackerPeerKey(candidate)
		if _, seen := known[key]; seen {
			continue
		}
		known[key] = struct{}{}
		newPeers = append(newPeers, candidate)
	}
	return newPeers
}

func markKnownPeers(known map[string]struct{}, peers []tracker.Peer) {
	for _, candidate := range peers {
		known[trackerPeerKey(candidate)] = struct{}{}
	}
}

func trackerPeerKey(candidate tracker.Peer) string {
	return fmt.Sprintf("%s:%d", candidate.IP.String(), candidate.Port)
}
