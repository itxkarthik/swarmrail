package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/itxkarthik/GoTorrent/internal/peer"
	"github.com/itxkarthik/GoTorrent/internal/piece"
	"github.com/itxkarthik/GoTorrent/internal/session"
	"github.com/itxkarthik/GoTorrent/internal/torrent"
	"github.com/itxkarthik/GoTorrent/internal/tracker"
	"github.com/itxkarthik/GoTorrent/internal/tui"
)

const (
	trackerDiscoveryTimeout = 3 * time.Second
	trackerFallbackTimeout  = 10 * time.Second
)

type announcePeersFunc func(context.Context) ([]tracker.Peer, error)
type connectPeersFunc func([]tracker.Peer) ([]*peer.PeerConnection, peer.ConnectReport)

// announceToTracker announces to every tracker concurrently and combines their peers.
// When quiet is true, per-tracker diagnostics are not printed to stderr.
func announceToTracker(ctx context.Context, announceURLs []string, request tracker.AnnounceRequest, quiet bool) (tracker.DiscoveryResult, error) {
	discovery, err := tracker.Discover(ctx, announceURLs, request)
	if !quiet {
		for i, result := range discovery.Results {
			if result.Err != nil {
				fmt.Fprintf(os.Stderr, "  tracker %d/%d: %s - %v\n", i+1, len(discovery.Results), result.URL, result.Err)
				continue
			}
			fmt.Fprintf(os.Stderr, "  tracker %d/%d: %s - OK (%d peers, interval %s)\n",
				i+1, len(discovery.Results), result.URL, len(result.Peers), result.Interval)
		}
	}
	return discovery, err
}

// discoverPeerConnections performs a fast discovery pass and, when requested,
// one slower fallback pass if none of the first peers can be connected.
func discoverPeerConnections(
	allowFallback bool,
	announce announcePeersFunc,
	connect connectPeersFunc,
	setStatus func(string),
) ([]tracker.Peer, []*peer.PeerConnection, peer.ConnectReport, error) {
	announceAttempt := func(timeout time.Duration) ([]tracker.Peer, error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return announce(ctx)
	}

	setStatus("Connecting trackers...")
	peers, firstErr := announceAttempt(trackerDiscoveryTimeout)
	connections, report := connectDiscoveredPeers(peers, connect, setStatus)
	if len(connections) > 0 || !allowFallback {
		return peers, connections, report, firstErr
	}

	setStatus("Retrying trackers with extended timeout...")
	retryPeers, retryErr := announceAttempt(trackerFallbackTimeout)
	peers = mergeTrackerPeers(peers, retryPeers)
	connections, retryReport := connectDiscoveredPeers(peers, connect, setStatus)
	report = mergeConnectReports(report, retryReport)
	if len(peers) > 0 {
		return peers, connections, report, nil
	}
	return peers, connections, report, errors.Join(firstErr, retryErr)
}

func connectDiscoveredPeers(peers []tracker.Peer, connect connectPeersFunc, setStatus func(string)) ([]*peer.PeerConnection, peer.ConnectReport) {
	if len(peers) == 0 {
		return nil, peer.ConnectReport{}
	}
	setStatus(fmt.Sprintf("Connecting to %d peers...", len(peers)))
	return connect(peers)
}

func mergeTrackerPeers(groups ...[]tracker.Peer) []tracker.Peer {
	seen := make(map[string]struct{})
	var merged []tracker.Peer
	for _, peers := range groups {
		for _, candidate := range peers {
			key := fmt.Sprintf("%s:%d", candidate.IP.String(), candidate.Port)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			merged = append(merged, candidate)
		}
	}
	return merged
}

func mergeConnectReports(reports ...peer.ConnectReport) peer.ConnectReport {
	var merged peer.ConnectReport
	for _, report := range reports {
		merged.Attempted += report.Attempted
		merged.Dialed += report.Dialed
		merged.Handshaken += report.Handshaken
		merged.Failures = append(merged.Failures, report.Failures...)
	}
	return merged
}

func run() error {
	port := flag.Int("port", 6881, "listen port for incoming connections")
	output := flag.String("output", "", "output directory (default: torrent name)")
	tuiMode := flag.Bool("tui", false, "terminal UI mode")
	flag.Parse()

	if flag.NArg() < 1 {
		return fmt.Errorf("Usage: gotorrent [flags] <torrent-file>\nFlags:\n  --port   listen port (default: 6881)\n  --output output directory\n  --tui    terminal UI mode")
	}

	torrentPath := flag.Arg(0)
	torrentData, err := os.ReadFile(torrentPath)
	if err != nil {
		return fmt.Errorf("reading torrent file: %w", err)
	}
	if !*tuiMode {
		fmt.Printf("Read %d bytes from %s\n", len(torrentData), torrentPath)
	}

	var tf torrent.TorrentFile
	if err := tf.Parse(torrentData); err != nil {
		return fmt.Errorf("parsing torrent file: %w", err)
	}

	peerID := peer.GeneratePeerID()
	outputPath := *output
	if outputPath == "" {
		outputPath = "."
	}

	m := piece.NewManager(&tf, nil)
	statePath := piece.StateFilePath(outputPath, tf.Name)

	// Try to resume from saved state
	if state, err := piece.LoadResume(statePath, tf.InfoHash); err == nil {
		completed := piece.CountCompleted(state.Completed)
		if !*tuiMode {
			fmt.Printf("Resuming from %d/%d completed pieces\n", completed, len(tf.PieceHashes))
		}
		m.SetCompleted(state.Completed)
	} else if !os.IsNotExist(err) && !*tuiMode {
		fmt.Printf("Warning: ignoring invalid resume state: %v\n", err)
	}

	m.EnablePeriodicSave(statePath, tf.InfoHash)

	if *tuiMode {
		return runTUIFlow(m, statePath, outputPath, &tf, peerID, uint16(*port))
	}

	var (
		peers           []tracker.Peer
		connections     []*peer.PeerConnection
		connectReport   peer.ConnectReport
		announceResults []tracker.AnnounceResult
	)
	if len(tf.AnnounceList) > 0 {
		fmt.Fprintf(os.Stderr, "  primary: %s\n", tf.Announce)
		announceEvent := tracker.EventStarted
		peers, connections, connectReport, err = discoverPeerConnections(
			len(tf.URLList) == 0 && len(tf.HTTPSeeds) == 0,
			func(ctx context.Context) ([]tracker.Peer, error) {
				completed := m.CompletedBytes()
				discovery, announceErr := announceToTracker(ctx, tf.AnnounceList, tracker.AnnounceRequest{
					InfoHash:   tf.InfoHash,
					PeerID:     peerID,
					Port:       uint16(*port),
					Downloaded: completed,
					Left:       m.TotalLength() - completed,
					Event:      announceEvent,
				}, false)
				announceEvent = tracker.EventNone
				announceResults = session.MergeAnnounceResults(announceResults, discovery.Results)
				return discovery.Peers, announceErr
			},
			func(discovered []tracker.Peer) ([]*peer.PeerConnection, peer.ConnectReport) {
				return peer.ConnectToPeersWithID(&tf, discovered, peerID)
			},
			func(status string) { fmt.Println(status) },
		)
		if err != nil && len(peers) == 0 {
			if len(tf.URLList) == 0 && len(tf.HTTPSeeds) == 0 {
				return fmt.Errorf("announcing to trackers after fallback: %w", err)
			}
			fmt.Fprintf(os.Stderr, "Warning: tracker announce failed; falling back to webseeds: %v\n", err)
		}
	} else if len(tf.URLList) == 0 && len(tf.HTTPSeeds) == 0 {
		return fmt.Errorf("torrent has no tracker announce URL or webseed URL")
	}
	fmt.Printf("Got %d peers from tracker\n", len(peers))

	fmt.Printf("Connected to %d peers\n", len(connections))
	if len(connections) == 0 && len(peers) > 0 {
		fmt.Fprintf(os.Stderr, "Peer connection summary: %d dialed, %d handshaken, %d usable\n",
			connectReport.Dialed, connectReport.Handshaken, len(connections))
	}
	if len(connections) == 0 && len(tf.URLList) == 0 && len(tf.HTTPSeeds) == 0 {
		return fmt.Errorf("trackers returned %d peers after fallback, but none connected (%d dialed, %d completed handshake)",
			len(peers), connectReport.Dialed, connectReport.Handshaken)
	}

	m.SetPeers(connections)
	defer closePeerConnections(m.Peers)

	var cancelReannounce context.CancelFunc
	var reannounceDone chan struct{}
	if len(tf.AnnounceList) > 0 {
		var reannounceCtx context.Context
		reannounceCtx, cancelReannounce = context.WithCancel(context.Background())
		reannounceDone = make(chan struct{})
		go func() {
			defer close(reannounceDone)
			session.RunPeriodicReannounce(reannounceCtx, session.Config{
				Torrent:        &tf,
				PeerID:         peerID,
				Port:           uint16(*port),
				Manager:        m,
				InitialPeers:   peers,
				InitialResults: announceResults,
				Announce: func(ctx context.Context, request tracker.AnnounceRequest) (tracker.DiscoveryResult, error) {
					return announceToTracker(ctx, tf.AnnounceList, request, false)
				},
			})
		}()
		defer func() {
			cancelReannounce()
			<-reannounceDone
		}()
	}
	fmt.Printf("Progress %d/%d pieces (%.1f%%)\n",
		piece.CountCompleted(m.Completed()), len(tf.PieceHashes),
		float64(piece.CountCompleted(m.Completed()))/float64(len(tf.PieceHashes))*100)

	// Signal handler for graceful shutdown (CLI mode only)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nSaving state and exiting...")
		if err := piece.SaveResume(statePath, tf.InfoHash, m.Completed()); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving resume state: %v\n", err)
		} else {
			fmt.Printf("Resume state saved to %s\n", statePath)
		}
		os.Exit(0)
	}()

	if err := m.Download(outputPath, true); err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	piece.RemoveResume(statePath)
	fmt.Println("Download complete!")
	return nil
}

func runTUIFlow(m *piece.Manager, statePath, outputPath string, tf *torrent.TorrentFile, peerID [20]byte, port uint16) error {
	m.SetLogWriter(io.Discard) // suppress stderr progress in TUI mode
	doneCh := make(chan error, 1)
	m.SetStatus("Connecting trackers...")

	go func() {
		var (
			peers           []tracker.Peer
			connections     []*peer.PeerConnection
			connectReport   peer.ConnectReport
			announceErr     error
			announceResults []tracker.AnnounceResult
		)
		if len(tf.AnnounceList) > 0 {
			announceEvent := tracker.EventStarted
			peers, connections, connectReport, announceErr = discoverPeerConnections(
				len(tf.URLList) == 0 && len(tf.HTTPSeeds) == 0,
				func(ctx context.Context) ([]tracker.Peer, error) {
					completed := m.CompletedBytes()
					discovery, err := announceToTracker(ctx, tf.AnnounceList, tracker.AnnounceRequest{
						InfoHash:   tf.InfoHash,
						PeerID:     peerID,
						Port:       port,
						Downloaded: completed,
						Left:       m.TotalLength() - completed,
						Event:      announceEvent,
					}, true)
					announceEvent = tracker.EventNone
					announceResults = session.MergeAnnounceResults(announceResults, discovery.Results)
					return discovery.Peers, err
				},
				func(discovered []tracker.Peer) ([]*peer.PeerConnection, peer.ConnectReport) {
					return peer.ConnectToPeersWithID(tf, discovered, peerID)
				},
				m.SetStatus,
			)
		}

		if len(peers) == 0 && len(tf.URLList) == 0 && len(tf.HTTPSeeds) == 0 {
			m.SetStatus("No peers or webseeds available")
			if announceErr != nil {
				doneCh <- fmt.Errorf("tracker discovery failed after fallback: %w", announceErr)
			} else {
				doneCh <- fmt.Errorf("no peers or webseeds available after tracker fallback")
			}
			return
		}

		m.SetPeers(connections)
		defer closePeerConnections(m.Peers)

		if len(connections) == 0 && len(tf.URLList) == 0 && len(tf.HTTPSeeds) == 0 {
			m.SetStatus("No peers connected")
			doneCh <- fmt.Errorf("trackers returned %d peers after fallback, but none connected (%d dialed, %d completed handshake)",
				len(peers), connectReport.Dialed, connectReport.Handshaken)
			return
		}

		if len(connections) == 0 {
			m.SetStatus(fmt.Sprintf("Downloading with %d webseed(s)", len(tf.URLList)+len(tf.HTTPSeeds)))
		} else {
			m.SetStatus(fmt.Sprintf("Downloading with %d peer(s)", len(connections)))
		}

		var cancelReannounce context.CancelFunc
		var reannounceDone chan struct{}
		if len(tf.AnnounceList) > 0 {
			var reannounceCtx context.Context
			reannounceCtx, cancelReannounce = context.WithCancel(context.Background())
			reannounceDone = make(chan struct{})
			go func() {
				defer close(reannounceDone)
				session.RunPeriodicReannounce(reannounceCtx, session.Config{
					Torrent:        tf,
					PeerID:         peerID,
					Port:           port,
					Manager:        m,
					InitialPeers:   peers,
					InitialResults: announceResults,
					Announce: func(ctx context.Context, request tracker.AnnounceRequest) (tracker.DiscoveryResult, error) {
						return announceToTracker(ctx, tf.AnnounceList, request, true)
					},
				})
			}()
			defer func() {
				cancelReannounce()
				<-reannounceDone
			}()
		}

		err := m.Download(outputPath, true)
		if err == nil {
			piece.RemoveResume(statePath)
		}
		doneCh <- err
	}()

	return tui.Run(m, doneCh)
}

func closePeerConnections(peers func() []*peer.PeerConnection) {
	for _, connection := range peers() {
		connection.Close()
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
