package session

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/itxkarthik/swarmrail/internal/peer"
	"github.com/itxkarthik/swarmrail/internal/piece"
	"github.com/itxkarthik/swarmrail/internal/torrent"
)

func TestPeerListenerAcceptsInboundPeer(t *testing.T) {
	tf := &torrent.TorrentFile{Name: "inbound", Length: 4, PieceLength: 4, PieceHashes: make([][20]byte, 1), InfoHash: [20]byte{1}}
	manager := piece.NewManager(tf, nil)
	localID := [20]byte{2}
	listener, err := NewPeerListener("127.0.0.1:0", tf, localID, manager)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- listener.Run(ctx) }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	remoteID := [20]byte{3}
	if _, err := peer.Handshake(conn, tf.InfoHash, remoteID); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for len(manager.Peers()) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	peers := manager.Peers()
	if len(peers) != 1 || peers[0].Direction() != peer.DirectionInbound {
		t.Fatalf("peers = %#v, want one inbound peer", peers)
	}
	snapshot := listener.Snapshot()
	if snapshot.Accepted != 1 || snapshot.Rejected != 0 {
		t.Fatalf("snapshot = %+v", snapshot)
	}

	conn.Close()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestPeerListenerRejectsSelfConnection(t *testing.T) {
	tf := &torrent.TorrentFile{Name: "self", Length: 4, PieceLength: 4, PieceHashes: make([][20]byte, 1), InfoHash: [20]byte{1}}
	manager := piece.NewManager(tf, nil)
	localID := [20]byte{2}
	listener, err := NewPeerListener("127.0.0.1:0", tf, localID, manager)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- listener.Run(ctx) }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = peer.Handshake(conn, tf.InfoHash, localID)
	conn.Close()
	deadline := time.Now().Add(time.Second)
	for listener.Snapshot().Rejected != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(manager.Peers()) != 0 || listener.Snapshot().Rejected != 1 {
		t.Fatalf("peers=%d snapshot=%+v", len(manager.Peers()), listener.Snapshot())
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPeerListenerCancellationClosesPendingHandshake(t *testing.T) {
	tf := &torrent.TorrentFile{Name: "cancel", Length: 4, PieceLength: 4, PieceHashes: make([][20]byte, 1), InfoHash: [20]byte{1}}
	listener, err := NewPeerListener("127.0.0.1:0", tf, [20]byte{2}, piece.NewManager(tf, nil))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- listener.Run(ctx) }()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	deadline := time.Now().Add(time.Second)
	for listener.Snapshot().ActiveHandshakes != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("listener shutdown waited for handshake deadline")
	}
}

func TestInboundPeerCompletesWaitingDownload(t *testing.T) {
	data := []byte("data")
	tf := &torrent.TorrentFile{Name: "inbound.bin", Length: 4, PieceLength: 4, PieceHashes: [][20]byte{sha1.Sum(data)}, InfoHash: [20]byte{1}}
	manager := piece.NewManager(tf, nil)
	manager.SetLogWriter(io.Discard)
	listener, err := NewPeerListener("127.0.0.1:0", tf, [20]byte{2}, manager)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	listenerDone := make(chan error, 1)
	go func() { listenerDone <- listener.Run(ctx) }()
	downloadDone := make(chan error, 1)
	dir := t.TempDir()
	go func() { downloadDone <- manager.Download(ctx, dir, false) }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := peer.Handshake(conn, tf.InfoHash, [20]byte{3}); err != nil {
		t.Fatal(err)
	}
	if err := peer.WriteMessage(conn, &peer.Message{ID: peer.MsgBitfield, Payload: []byte{0x80}}); err != nil {
		t.Fatal(err)
	}
	if err := peer.WriteMessage(conn, &peer.Message{ID: peer.MsgUnchoke}); err != nil {
		t.Fatal(err)
	}
	for {
		msg, err := peer.ReadMessage(conn)
		if err != nil {
			t.Fatal(err)
		}
		if msg == nil || msg.ID != peer.MsgRequest {
			continue
		}
		index := binary.BigEndian.Uint32(msg.Payload[0:4])
		begin := binary.BigEndian.Uint32(msg.Payload[4:8])
		if err := peer.WriteMessage(conn, peer.BuildPiece(index, begin, data)); err != nil {
			t.Fatal(err)
		}
		break
	}
	select {
	case err := <-downloadDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("inbound peer did not complete waiting download")
	}
	written, err := os.ReadFile(filepath.Join(dir, tf.Name))
	if err != nil || string(written) != string(data) {
		t.Fatalf("written payload = %q, err=%v", written, err)
	}
	cancel()
	if err := <-listenerDone; err != nil {
		t.Fatal(err)
	}
}
