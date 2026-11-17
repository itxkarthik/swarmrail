package tracker

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"
)

const defaultAnnounceTimeout = 5 * time.Second

const DefaultAnnounceInterval = 30 * time.Minute

type AnnounceEvent uint32

const (
	EventNone AnnounceEvent = iota
	EventCompleted
	EventStarted
	EventStopped
)

func (e AnnounceEvent) HTTPValue() string {
	switch e {
	case EventCompleted:
		return "completed"
	case EventStarted:
		return "started"
	case EventStopped:
		return "stopped"
	default:
		return ""
	}
}

type AnnounceRequest struct {
	InfoHash   [20]byte
	PeerID     [20]byte
	Port       uint16
	Uploaded   uint64
	Downloaded uint64
	Left       uint64
	Event      AnnounceEvent
	// Key identifies this client across IP changes. Zero means the
	// process-wide session key.
	Key uint32
}

type AnnounceResponse struct {
	Peers    []Peer
	Interval time.Duration
}

// AnnounceResult records the outcome of one tracker announce.
type AnnounceResult struct {
	URL      string
	Peers    []Peer
	Interval time.Duration
	Err      error
}

type DiscoveryResult struct {
	Peers   []Peer
	Results []AnnounceResult
}

// DiscoverPeers announces to all trackers concurrently and returns the unique
// peers reported by every successful tracker. The caller controls the maximum
// discovery duration through ctx.
func DiscoverPeers(ctx context.Context, announceURLs []string, infoHash [20]byte, peerID [20]byte, port uint16, totalLength int64) ([]Peer, []AnnounceResult, error) {
	discovery, err := Discover(ctx, announceURLs, AnnounceRequest{
		InfoHash: infoHash,
		PeerID:   peerID,
		Port:     port,
		Left:     uint64(totalLength),
		Event:    EventStarted,
	})
	return discovery.Peers, discovery.Results, err
}

func Discover(ctx context.Context, announceURLs []string, request AnnounceRequest) (DiscoveryResult, error) {
	if len(announceURLs) == 0 {
		return DiscoveryResult{}, fmt.Errorf("no tracker URLs available")
	}

	type indexedResult struct {
		index  int
		result AnnounceResult
	}

	resultsCh := make(chan indexedResult, len(announceURLs))
	for i, announceURL := range announceURLs {
		go func() {
			response, err := announceOne(ctx, announceURL, request)
			resultsCh <- indexedResult{
				index:  i,
				result: AnnounceResult{URL: announceURL, Peers: response.Peers, Interval: response.Interval, Err: err},
			}
		}()
	}

	results := make([]AnnounceResult, len(announceURLs))
	for range announceURLs {
		result := <-resultsCh
		results[result.index] = result.result
	}

	seen := make(map[string]struct{})
	var peers []Peer
	var announceErrors []error
	successes := 0
	for _, result := range results {
		if result.Err != nil {
			announceErrors = append(announceErrors, fmt.Errorf("%s: %w", result.URL, result.Err))
			continue
		}
		successes++
		for _, peer := range result.Peers {
			key := fmt.Sprintf("%s:%d", peer.IP.String(), peer.Port)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			peers = append(peers, peer)
		}
	}

	if successes == 0 {
		return DiscoveryResult{Results: results}, fmt.Errorf("all %d trackers failed: %w", len(announceURLs), errors.Join(announceErrors...))
	}
	return DiscoveryResult{Peers: peers, Results: results}, nil
}

func announceOne(ctx context.Context, announceURL string, request AnnounceRequest) (AnnounceResponse, error) {
	parsed, err := url.Parse(announceURL)
	if err != nil {
		return AnnounceResponse{}, fmt.Errorf("parsing tracker URL: %w", err)
	}

	switch parsed.Scheme {
	case "http", "https":
		return announceHTTP(ctx, announceURL, request)
	case "udp":
		return announceUDP(ctx, parsed.Host, request)
	default:
		return AnnounceResponse{}, fmt.Errorf("unsupported tracker scheme %q", parsed.Scheme)
	}
}
