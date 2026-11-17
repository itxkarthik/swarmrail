package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/itxkarthik/swarmrail/internal/peer"
	"github.com/itxkarthik/swarmrail/internal/piece"
	"github.com/itxkarthik/swarmrail/internal/session"
	"github.com/itxkarthik/swarmrail/internal/torrent"
	"github.com/itxkarthik/swarmrail/internal/tracker"
	"github.com/itxkarthik/swarmrail/internal/tui"
)

const (
	trackerDiscoveryTimeout = 3 * time.Second
	trackerFallbackTimeout  = 10 * time.Second
	// trackerEventTimeout bounds the completed and stopped announces so an
	// unreachable tracker cannot hold up completion or shutdown.
	trackerEventTimeout = 2 * time.Second
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

// sendTrackerEvent announces a lifecycle event (completed or stopped) to every
// tracker. "stopped" is sent precisely when ctx has been cancelled, so the
// announce runs on a context that keeps ctx's values but not its
// cancellation, bounded by trackerEventTimeout.
func sendTrackerEvent(ctx context.Context, tf *torrent.TorrentFile, m *piece.Manager, peerID [20]byte, port uint16, event tracker.AnnounceEvent, quiet bool) {
	if len(tf.AnnounceList) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), trackerEventTimeout)
	defer cancel()
	completed := m.CompletedBytes()
	_, _ = announceToTracker(ctx, tf.AnnounceList, tracker.AnnounceRequest{
		InfoHash:   tf.InfoHash,
		PeerID:     peerID,
		Port:       port,
		Downloaded: completed,
		Uploaded:   m.UploadedBytes(),
		Left:       m.TotalLength() - completed,
		Event:      event,
	}, quiet)
}

// isComplete reports whether every piece is verified.
func isComplete(m *piece.Manager) bool {
	return piece.CountCompleted(m.Completed()) == m.TotalPieces()
}

// discoverPeerConnections performs a fast discovery pass and, when requested,
// one slower fallback pass if none of the first peers can be connected.
func discoverPeerConnections(
	parent context.Context,
	allowFallback bool,
	announce announcePeersFunc,
	connect connectPeersFunc,
	setStatus func(string),
) ([]tracker.Peer, []*peer.PeerConnection, peer.ConnectReport, error) {
	announceAttempt := func(timeout time.Duration) ([]tracker.Peer, error) {
		ctx, cancel := context.WithTimeout(parent, timeout)
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
	port := flag.Int("port", 6881, "listen port for incoming connections (0 picks a free port)")
	output := flag.String("output", "", "output directory (default: current directory)")
	tuiMode := flag.Bool("tui", false, "terminal UI mode")
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "Usage: swarmrail [flags] <torrent-file>")
		fmt.Fprintln(flag.CommandLine.Output(), "\nFlags:")
		flag.PrintDefaults()
	}
	flag.Parse()
	ctx, stop := signalNotifyContext(context.Background())
	defer stop()

	if flag.NArg() < 1 {
		flag.Usage()
		return errUsage
	}
	if *port < 0 || *port > 65535 {
		return fmt.Errorf("invalid listen port %d: must be between 0 and 65535", *port)
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
	statePath := piece.StateFilePath(outputPath, tf.Name, tf.InfoHash)

	// Resume only from pieces that still match the payload on disk.
	if state, report, err := piece.LoadVerifiedResume(outputPath, &tf); err == nil {
		completed := piece.CountCompleted(state.Completed)
		if !*tuiMode {
			fmt.Printf("Resuming from %d/%d completed pieces\n", completed, len(tf.PieceHashes))
			if report.ResetPieces > 0 {
				fmt.Printf("Reset %d stale resume piece(s) for redownload\n", report.ResetPieces)
			}
		}
		m.SetCompleted(state.Completed)
	} else if !os.IsNotExist(err) && !*tuiMode {
		fmt.Printf("Warning: ignoring invalid resume state: %v\n", err)
	}

	m.EnablePeriodicSave(statePath, tf.InfoHash)
	peerListener, err := session.NewPeerListener(net.JoinHostPort("", fmt.Sprintf("%d", *port)), &tf, peerID, m)
	if err != nil {
		return fmt.Errorf("listening for inbound peers on port %d: %w", *port, err)
	}
	listenCtx, stopListener := context.WithCancel(ctx)
	listenerDone := make(chan error, 1)
	go func() { listenerDone <- peerListener.Run(listenCtx) }()
	defer func() {
		stopListener()
		<-listenerDone
	}()
	announcePort := peerListener.Port()
	if !*tuiMode {
		fmt.Printf("Listening for inbound peers on %s\n", peerListener.Addr())
	}

	if *tuiMode {
		return runTUIFlow(ctx, m, statePath, outputPath, &tf, peerID, announcePort, peerListener)
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
			ctx,
			len(tf.URLList) == 0 && len(tf.HTTPSeeds) == 0,
			func(ctx context.Context) ([]tracker.Peer, error) {
				completed := m.CompletedBytes()
				discovery, announceErr := announceToTracker(ctx, tf.AnnounceList, tracker.AnnounceRequest{
					InfoHash:   tf.InfoHash,
					PeerID:     peerID,
					Port:       announcePort,
					Downloaded: completed,
					Uploaded:   m.UploadedBytes(),
					Left:       m.TotalLength() - completed,
					Event:      announceEvent,
				}, false)
				announceEvent = tracker.EventNone
				announceResults = session.MergeAnnounceResults(announceResults, discovery.Results)
				return discovery.Peers, announceErr
			},
			func(discovered []tracker.Peer) ([]*peer.PeerConnection, peer.ConnectReport) {
				return peer.ConnectToPeersWithContext(ctx, &tf, discovered, peerID, m.BitfieldMessage)
			},
			func(status string) { fmt.Println(status) },
		)
		if err != nil && len(peers) == 0 {
			if len(tf.URLList) == 0 && len(tf.HTTPSeeds) == 0 {
				fmt.Fprintf(os.Stderr, "Warning: tracker discovery failed; waiting for inbound peers: %v\n", err)
			} else {
				fmt.Fprintf(os.Stderr, "Warning: tracker announce failed; falling back to webseeds: %v\n", err)
			}
		}
	} else if len(tf.URLList) == 0 && len(tf.HTTPSeeds) == 0 {
		return fmt.Errorf("torrent has no tracker announce URL or webseed URL")
	}
	// BEP 3: tell trackers we are leaving so they stop handing out our
	// address. Deferred before the reannounce loop's cleanup so that loop
	// has stopped by the time this runs.
	defer sendTrackerEvent(ctx, &tf, m, peerID, announcePort, tracker.EventStopped, false)
	fmt.Printf("Got %d peers from tracker\n", len(peers))

	fmt.Printf("Connected to %d peers\n", len(connections))
	if len(connections) == 0 && len(peers) > 0 {
		fmt.Fprintf(os.Stderr, "Peer connection summary: %d dialed, %d handshaken, %d usable\n",
			connectReport.Dialed, connectReport.Handshaken, len(connections))
	}
	m.AddPeers(connections)
	defer closePeerConnections(m.Peers)

	var cancelReannounce context.CancelFunc
	var reannounceDone chan struct{}
	if len(tf.AnnounceList) > 0 {
		var reannounceCtx context.Context
		reannounceCtx, cancelReannounce = context.WithCancel(ctx)
		reannounceDone = make(chan struct{})
		go func() {
			defer close(reannounceDone)
			session.RunPeriodicReannounce(reannounceCtx, session.Config{
				Torrent:        &tf,
				PeerID:         peerID,
				Port:           announcePort,
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

	alreadyComplete := isComplete(m)
	if err := m.Download(ctx, outputPath, true); err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	if !alreadyComplete {
		sendTrackerEvent(ctx, &tf, m, peerID, announcePort, tracker.EventCompleted, false)
	}

	piece.RemoveResume(statePath)
	fmt.Println("Download complete!")
	return nil
}

func runTUIFlow(parent context.Context, m *piece.Manager, statePath, outputPath string, tf *torrent.TorrentFile, peerID [20]byte, port uint16, peerListener *session.PeerListener) error {
	m.SetLogWriter(io.Discard) // suppress stderr progress in TUI mode
	doneCh := make(chan error, 1)
	workerResult := make(chan error, 1)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var controller *session.ReannounceController
	if len(tf.AnnounceList) > 0 {
		controller = session.NewReannounceController(time.Minute)
	}
	m.SetStatus("Connecting trackers...")

	go func() {
		// Report the result only after every deferred cleanup below (the
		// stopped announce, closing peers, stopping reannounce) has run, so
		// the process cannot exit before shutdown finishes.
		var downloadErr error
		defer func() { workerResult <- downloadErr }()
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
				ctx,
				len(tf.URLList) == 0 && len(tf.HTTPSeeds) == 0,
				func(ctx context.Context) ([]tracker.Peer, error) {
					controller.MarkAttempt()
					defer controller.FinishAttempt()
					completed := m.CompletedBytes()
					discovery, err := announceToTracker(ctx, tf.AnnounceList, tracker.AnnounceRequest{
						InfoHash:   tf.InfoHash,
						PeerID:     peerID,
						Port:       port,
						Downloaded: completed,
						Uploaded:   m.UploadedBytes(),
						Left:       m.TotalLength() - completed,
						Event:      announceEvent,
					}, true)
					announceEvent = tracker.EventNone
					announceResults = session.MergeAnnounceResults(announceResults, discovery.Results)
					return discovery.Peers, err
				},
				func(discovered []tracker.Peer) ([]*peer.PeerConnection, peer.ConnectReport) {
					return peer.ConnectToPeersWithContext(ctx, tf, discovered, peerID, m.BitfieldMessage)
				},
				m.SetStatus,
			)
		}

		if len(peers) == 0 && len(tf.URLList) == 0 && len(tf.HTTPSeeds) == 0 && announceErr != nil {
			m.SetStatus("Tracker discovery failed; waiting for inbound peers")
		}

		defer sendTrackerEvent(ctx, tf, m, peerID, port, tracker.EventStopped, true)
		m.AddPeers(connections)
		defer closePeerConnections(m.Peers)

		if len(connections) == 0 && len(tf.URLList) == 0 && len(tf.HTTPSeeds) == 0 {
			m.SetStatus(fmt.Sprintf("Waiting for inbound peers (%d candidates, %d handshakes)", len(peers), connectReport.Handshaken))
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
			reannounceCtx, cancelReannounce = context.WithCancel(ctx)
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
					Controller: controller,
				})
			}()
			defer func() {
				cancelReannounce()
				<-reannounceDone
			}()
		}

		alreadyComplete := isComplete(m)
		err := m.Download(ctx, outputPath, true)
		if err == nil {
			if !alreadyComplete {
				sendTrackerEvent(ctx, tf, m, peerID, port, tracker.EventCompleted, true)
			}
			piece.RemoveResume(statePath)
		}
		downloadErr = err
		doneCh <- err
	}()

	tuiErr := tui.Run(m, doneCh, controller, cancel, peerListener)
	cancel()
	workerErr := <-workerResult
	if tuiErr != nil {
		return tuiErr
	}
	if workerErr != nil && !errors.Is(workerErr, context.Canceled) {
		return workerErr
	}
	return nil
}

var signalNotifyContext = func(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
}

func closePeerConnections(peers func() []*peer.PeerConnection) {
	for _, connection := range peers() {
		connection.Close()
	}
}

// errUsage reports invalid command-line arguments after usage was printed.
var errUsage = errors.New("invalid arguments")

func main() {
	if err := run(); err != nil {
		if errors.Is(err, errUsage) {
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
