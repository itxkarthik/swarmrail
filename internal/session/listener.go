package session

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/itxkarthik/swarmrail/internal/peer"
	"github.com/itxkarthik/swarmrail/internal/piece"
	"github.com/itxkarthik/swarmrail/internal/torrent"
)

const maxInboundHandshakes = 64

type ListenerSnapshot struct {
	Address          string
	Running          bool
	Accepted         uint64
	Rejected         uint64
	ActiveHandshakes int
	LastError        string
}

// PeerListener accepts peers for one torrent and injects validated connections
// into the active piece manager.
type PeerListener struct {
	listener net.Listener
	torrent  *torrent.TorrentFile
	peerID   [20]byte
	manager  *piece.Manager
	sem      chan struct{}

	mu       sync.Mutex
	snapshot ListenerSnapshot
	wg       sync.WaitGroup
}

func NewPeerListener(address string, tf *torrent.TorrentFile, peerID [20]byte, manager *piece.Manager) (*PeerListener, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	return &PeerListener{
		listener: listener,
		torrent:  tf,
		peerID:   peerID,
		manager:  manager,
		sem:      make(chan struct{}, maxInboundHandshakes),
		snapshot: ListenerSnapshot{Address: listener.Addr().String()},
	}, nil
}

func (l *PeerListener) Addr() net.Addr { return l.listener.Addr() }

func (l *PeerListener) Port() uint16 {
	if address, ok := l.listener.Addr().(*net.TCPAddr); ok {
		return uint16(address.Port)
	}
	return 0
}

func (l *PeerListener) Snapshot() ListenerSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.snapshot
}

func (l *PeerListener) Run(ctx context.Context) error {
	l.mu.Lock()
	l.snapshot.Running = true
	l.mu.Unlock()
	closed := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = l.listener.Close()
		case <-closed:
		}
	}()
	defer func() {
		close(closed)
		_ = l.listener.Close()
		l.wg.Wait()
		l.mu.Lock()
		l.snapshot.Running = false
		l.mu.Unlock()
	}()

	for {
		conn, err := l.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			l.recordError(err)
			return err
		}
		select {
		case l.sem <- struct{}{}:
			l.wg.Add(1)
			go l.handle(ctx, conn)
		default:
			l.reject(conn, errors.New("inbound handshake limit reached"))
		}
	}
}

func (l *PeerListener) handle(ctx context.Context, conn net.Conn) {
	defer l.wg.Done()
	defer func() { <-l.sem }()
	l.mu.Lock()
	l.snapshot.ActiveHandshakes++
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.snapshot.ActiveHandshakes--
		l.mu.Unlock()
	}()

	stopCancel := context.AfterFunc(ctx, func() { _ = conn.Close() })
	remoteID, err := peer.AcceptHandshake(conn, l.torrent.InfoHash, l.peerID)
	if err != nil {
		stopCancel()
		l.reject(conn, err)
		return
	}
	if !stopCancel() && ctx.Err() != nil {
		l.reject(conn, ctx.Err())
		return
	}
	if remoteID == l.peerID {
		l.reject(conn, errors.New("self connection"))
		return
	}
	connection := peer.NewInboundPeerConnection(conn, remoteID)
	connection.ConfigureTorrent(len(l.torrent.PieceHashes), l.torrent.PieceLength, l.torrent.Length)
	connection.SetBitfield(make([]bool, len(l.torrent.PieceHashes)))
	connection.Start()
	if !connection.SendInterested() || l.manager.AddPeers([]*peer.PeerConnection{connection}) != 1 {
		connection.Close()
		l.recordRejected(errors.New("duplicate or closed inbound peer"))
		return
	}
	l.mu.Lock()
	l.snapshot.Accepted++
	l.snapshot.LastError = ""
	l.mu.Unlock()
}

func (l *PeerListener) reject(conn net.Conn, err error) {
	_ = conn.Close()
	l.recordRejected(err)
}

func (l *PeerListener) recordRejected(err error) {
	l.mu.Lock()
	l.snapshot.Rejected++
	l.snapshot.LastError = err.Error()
	l.mu.Unlock()
}

func (l *PeerListener) recordError(err error) {
	l.mu.Lock()
	l.snapshot.LastError = err.Error()
	l.mu.Unlock()
}
