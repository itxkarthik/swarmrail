package peer

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/itxkarthik/swarmrail/internal/torrent"
	"github.com/itxkarthik/swarmrail/internal/tracker"
)

const peerIDPrefix = "-SR0001-"

var errSelfConnection = errors.New("connected to ourselves")

// ConnectFailure records where an outbound peer connection failed.
type ConnectFailure struct {
	Address string
	Stage   string
	Err     error
}

// ConnectReport summarizes one batch of outbound peer connection attempts.
type ConnectReport struct {
	Attempted  int
	Dialed     int
	Handshaken int
	Failures   []ConnectFailure
}

// GeneratePeerID creates a 20-byte BitTorrent peer ID with SwarmRail's client prefix.
func GeneratePeerID() [20]byte {
	var id [20]byte
	copy(id[:], peerIDPrefix) // 8 bytes: "-SR0001-"

	// Fill remaining 12 bytes with random printable chars
	var buf [12]byte
	_, _ = rand.Read(buf[:])
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	for i, b := range buf {
		id[8+i] = chars[int(b)%len(chars)]
	}
	return id
}

// countSetBits returns the number of true values in a bool slice.
func countSetBits(bits []bool) int {
	n := 0
	for _, b := range bits {
		if b {
			n++
		}
	}
	return n
}

// ConnectToPeers opens TCP connections to each peer and performs the
// BitTorrent handshake. It sends no bitfield and is kept for callers that
// have no pieces yet.
//
// Returns only the peers that successfully completed the full handshake flow.
// Unreachable or slow peers are skipped silently.
func ConnectToPeers(tf *torrent.TorrentFile, peers []tracker.Peer) []*PeerConnection {
	connections, _ := ConnectToPeersWithID(tf, peers, GeneratePeerID())
	return connections
}

// ConnectToPeersWithID connects to peers using the same peer ID that was sent
// to the tracker and returns diagnostics for failed connection stages.
func ConnectToPeersWithID(tf *torrent.TorrentFile, peers []tracker.Peer, localPeerID [20]byte) ([]*PeerConnection, ConnectReport) {
	return ConnectToPeersWithContext(context.Background(), tf, peers, localPeerID, nil)
}

// ConnectToPeersWithContext cancels outstanding dials and handshakes when ctx
// ends, which is required for periodic discovery shutdown.
//
// localBitfield, if not nil, is called once per connection right after the
// handshake; a non-nil result is written as the first message (BEP 3).
// Interested is not sent here: the download worker sends it once the remote
// shows a piece we lack.
func ConnectToPeersWithContext(ctx context.Context, tf *torrent.TorrentFile, peers []tracker.Peer, localPeerID [20]byte, localBitfield func() *Message) ([]*PeerConnection, ConnectReport) {
	var (
		mu          sync.Mutex
		connections []*PeerConnection
		wg          sync.WaitGroup
		report      = ConnectReport{Attempted: len(peers)}
	)
	recordFailure := func(address, stage string, err error) {
		mu.Lock()
		report.Failures = append(report.Failures, ConnectFailure{Address: address, Stage: stage, Err: err})
		mu.Unlock()
	}

	for _, p := range peers {
		wg.Go(func() {
			// TCP connect with timeout
			addr := net.JoinHostPort(p.IP.String(), fmt.Sprintf("%d", p.Port))
			dialer := net.Dialer{Timeout: 10 * time.Second}
			conn, err := dialer.DialContext(ctx, "tcp", addr)
			if err != nil {
				recordFailure(addr, "dial", err)
				return
			}
			mu.Lock()
			report.Dialed++
			mu.Unlock()

			// BitTorrent handshake
			stopCancel := context.AfterFunc(ctx, func() { conn.Close() })
			remoteID, err := Handshake(conn, tf.InfoHash, localPeerID)
			cancelStopped := stopCancel()
			if err != nil {
				conn.Close()
				recordFailure(addr, "handshake", err)
				return
			}
			if !cancelStopped && ctx.Err() != nil {
				conn.Close()
				recordFailure(addr, "handshake", ctx.Err())
				return
			}
			mu.Lock()
			report.Handshaken++
			mu.Unlock()

			if remoteID == localPeerID {
				conn.Close()
				recordFailure(addr, "handshake", errSelfConnection)
				return
			}

			// Configure before starting so the remote's bitfield cannot race
			// with initialisation.
			pc := NewPeerConnection(conn, remoteID)
			pc.ConfigureTorrent(len(tf.PieceHashes), tf.PieceLength, tf.Length)
			pc.SetBitfield(make([]bool, len(tf.PieceHashes)))
			var first *Message
			if localBitfield != nil {
				first = localBitfield()
			}
			pc.Start(first)

			// Wait briefly for the peer's initial bitfield or Have. BEP 3 peers
			// with no pieces may send neither, so a timeout is not an error.
			select {
			case <-pc.StateChanged():
			case <-time.After(10 * time.Second):
				// No bitfield is valid; continue and learn availability from Have.
			case <-pc.Done():
				recordFailure(addr, "initial message", fmt.Errorf("connection closed"))
				return
			case <-ctx.Done():
				pc.Close()
				recordFailure(addr, "initial message", ctx.Err())
				return
			}

			// Add to connections (thread-safe)
			mu.Lock()
			connections = append(connections, pc)
			mu.Unlock()
		})
	}

	wg.Wait()
	return connections, report
}
