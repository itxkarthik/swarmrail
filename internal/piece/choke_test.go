package piece

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
	"github.com/itxkarthik/swarmrail/internal/torrent"
)

// scriptedPeer is the remote side of a net.Pipe connection. Requests read
// from the client are delivered on requests, so a test can decide when and
// how to answer them.
type scriptedPeer struct {
	t        *testing.T
	conn     net.Conn
	requests chan peer.PieceWork
	cancels  chan peer.PieceWork
	control  chan peer.MessageID // every other message the client sends
}

func newScriptedPeer(t *testing.T, tf *torrent.TorrentFile) (*peer.PeerConnection, *scriptedPeer) {
	t.Helper()
	client, server := net.Pipe()
	pc := peer.NewPeerConnection(client, [20]byte{9})
	pc.ConfigureTorrent(len(tf.PieceHashes), tf.PieceLength, tf.Length)
	pc.SetBitfield(make([]bool, len(tf.PieceHashes)))
	pc.Start(nil)
	sp := &scriptedPeer{
		t:        t,
		conn:     server,
		requests: make(chan peer.PieceWork, 64),
		cancels:  make(chan peer.PieceWork, 64),
		control:  make(chan peer.MessageID, 64),
	}
	go func() {
		for {
			msg, err := peer.ReadMessage(server)
			if err != nil {
				return
			}
			if msg == nil {
				continue
			}
			if msg.ID != peer.MsgRequest && msg.ID != peer.MsgCancel {
				sp.control <- msg.ID
				continue
			}
			work := peer.PieceWork{
				Index:  int(binary.BigEndian.Uint32(msg.Payload[0:4])),
				Begin:  int(binary.BigEndian.Uint32(msg.Payload[4:8])),
				Length: int(binary.BigEndian.Uint32(msg.Payload[8:12])),
			}
			if msg.ID == peer.MsgCancel {
				sp.cancels <- work
			} else {
				sp.requests <- work
			}
		}
	}()
	t.Cleanup(func() {
		pc.Close()
		server.Close()
	})
	return pc, sp
}

func (sp *scriptedPeer) send(msg *peer.Message) {
	sp.t.Helper()
	if err := peer.WriteMessage(sp.conn, msg); err != nil {
		sp.t.Fatalf("scripted peer write: %v", err)
	}
}

func (sp *scriptedPeer) nextRequest(timeout time.Duration) (peer.PieceWork, bool) {
	select {
	case work := <-sp.requests:
		return work, true
	case <-time.After(timeout):
		return peer.PieceWork{}, false
	}
}

func (sp *scriptedPeer) serve(work peer.PieceWork, payload []byte) {
	sp.t.Helper()
	off := work.Index*blockSize*2 + work.Begin // tests use two-block pieces
	sp.send(peer.BuildPiece(uint32(work.Index), uint32(work.Begin), payload[off:off+work.Length]))
}

// twoBlockTorrent returns a one-piece torrent whose piece spans two blocks.
func twoBlockTorrent() (*torrent.TorrentFile, []byte) {
	payload := patterned(2 * blockSize)
	return &torrent.TorrentFile{
		Name:        "choke.bin",
		Length:      int64(len(payload)),
		PieceLength: int64(len(payload)),
		PieceHashes: [][20]byte{sha1.Sum(payload)},
	}, payload
}

func startDownload(t *testing.T, m *Manager) (string, <-chan error) {
	t.Helper()
	m.SetLogWriter(io.Discard)
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- m.Download(ctx, dir, false) }()
	return dir, done
}

func waitDownload(t *testing.T, done <-chan error, dir string, tf *torrent.TorrentFile, payload []byte) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("download did not complete")
	}
	got, err := os.ReadFile(filepath.Join(dir, tf.Name))
	if err != nil || string(got) != string(payload) {
		t.Fatalf("downloaded payload mismatch (err=%v)", err)
	}
}

// Before the fix, the worker treated "still choked" as fatal and exited, so a
// peer that unchoked after the worker started never received a request.
func TestWorkerWaitsForLateUnchoke(t *testing.T) {
	tf, payload := twoBlockTorrent()
	pc, remote := newScriptedPeer(t, tf)
	remote.send(&peer.Message{ID: peer.MsgBitfield, Payload: []byte{0x80}})

	m := NewManager(tf, []*peer.PeerConnection{pc})
	dir, done := startDownload(t, m)

	time.Sleep(300 * time.Millisecond)
	if _, ok := remote.nextRequest(0); ok {
		t.Fatal("request sent while choked")
	}
	remote.send(&peer.Message{ID: peer.MsgUnchoke})
	for range 2 {
		work, ok := remote.nextRequest(2 * time.Second)
		if !ok {
			t.Fatal("no request after a late unchoke")
		}
		remote.serve(work, payload)
	}
	waitDownload(t, done, dir, tf, payload)
}

// BEP 3: a choke discards outstanding requests. Before the fix the worker
// waited forever for them and the piece stayed in progress.
func TestWorkerRerequestsAfterChokeMidPiece(t *testing.T) {
	tf, payload := twoBlockTorrent()
	pc, remote := newScriptedPeer(t, tf)
	remote.send(&peer.Message{ID: peer.MsgBitfield, Payload: []byte{0x80}})
	remote.send(&peer.Message{ID: peer.MsgUnchoke})

	m := NewManager(tf, []*peer.PeerConnection{pc})
	dir, done := startDownload(t, m)

	first, ok := remote.nextRequest(2 * time.Second)
	if !ok {
		t.Fatal("no initial request")
	}
	if _, ok := remote.nextRequest(2 * time.Second); !ok {
		t.Fatal("second block was not pipelined")
	}
	remote.serve(first, payload)
	remote.send(&peer.Message{ID: peer.MsgChoke}) // drops the second request
	time.Sleep(100 * time.Millisecond)
	remote.send(&peer.Message{ID: peer.MsgUnchoke})

	served := 0
	for served < 2 {
		work, ok := remote.nextRequest(2 * time.Second)
		if !ok {
			break
		}
		remote.serve(work, payload)
		served++
	}
	if served == 0 {
		t.Fatal("blocks were not re-requested after unchoke")
	}
	waitDownload(t, done, dir, tf, payload)
}

// A peer that unchokes but never answers must not hold the piece forever.
func TestWorkerReleasesPieceFromSnubbingPeer(t *testing.T) {
	tf, _ := twoBlockTorrent()
	pc, remote := newScriptedPeer(t, tf)
	remote.send(&peer.Message{ID: peer.MsgBitfield, Payload: []byte{0x80}})
	remote.send(&peer.Message{ID: peer.MsgUnchoke})

	m := NewManager(tf, []*peer.PeerConnection{pc})
	m.requestTimeout = 200 * time.Millisecond
	startDownload(t, m)

	if _, ok := remote.nextRequest(2 * time.Second); !ok {
		t.Fatal("no request sent")
	}
	select {
	case <-remote.cancels:
	case <-time.After(2 * time.Second):
		t.Fatal("snubbed requests were not cancelled")
	}
	// The worker keeps going and asks again rather than exiting.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-remote.requests:
			return
		case <-remote.cancels:
		case <-deadline:
			t.Fatal("worker did not retry after snub timeout")
		}
	}
}

// Replies that arrive after a choke or cancel must be dropped, not fed to the
// next piece.
func TestUnrequestedBlocksAreDropped(t *testing.T) {
	tf, payload := twoBlockTorrent()
	pc, remote := newScriptedPeer(t, tf)
	remote.send(&peer.Message{ID: peer.MsgUnchoke})
	remote.send(peer.BuildPiece(0, 0, payload[:blockSize])) // never requested

	select {
	case msg := <-pc.Incoming():
		t.Fatalf("unrequested block delivered: index %d", binary.BigEndian.Uint32(msg.Payload))
	case <-time.After(200 * time.Millisecond):
	}
}

// BEP 3: interested means the remote has a piece we lack.
func TestWorkerSendsInterestedOnlyWhenPeerHasNeededPiece(t *testing.T) {
	tf := &torrent.TorrentFile{
		Name: "interest.bin", Length: 8, PieceLength: 4,
		PieceHashes: [][20]byte{sha1.Sum([]byte("aaaa")), sha1.Sum([]byte("bbbb"))},
	}
	pc, remote := newScriptedPeer(t, tf)
	remote.send(&peer.Message{ID: peer.MsgBitfield, Payload: []byte{0x80}}) // has piece 0 only

	m := NewManager(tf, []*peer.PeerConnection{pc})
	m.SetCompleted([]bool{true, false})
	startDownload(t, m)

	// Our catch-up Have for piece 0 is fine; Interested is not.
	quiet := time.After(300 * time.Millisecond)
	for waiting := true; waiting; {
		select {
		case id := <-remote.control:
			if id == peer.MsgInterested {
				t.Fatal("sent Interested to a peer with nothing we need")
			}
		case <-quiet:
			waiting = false
		}
	}

	remote.send(peer.BuildHave(1))
	deadline := time.After(2 * time.Second)
	for {
		select {
		case id := <-remote.control:
			if id == peer.MsgInterested {
				return
			}
		case <-deadline:
			t.Fatal("no Interested after the peer announced a needed piece")
		}
	}
}

// A choke immediately followed by an unchoke still discards every request
// sent before it. The worker must notice even though the peer is unchoked by
// the time it wakes, instead of waiting out the snub timeout.
func TestWorkerRerequestsAfterQuickChokeUnchoke(t *testing.T) {
	tf, payload := twoBlockTorrent()
	pc, remote := newScriptedPeer(t, tf)
	remote.send(&peer.Message{ID: peer.MsgBitfield, Payload: []byte{0x80}})
	remote.send(&peer.Message{ID: peer.MsgUnchoke})

	m := NewManager(tf, []*peer.PeerConnection{pc})
	dir, done := startDownload(t, m)

	for range 2 {
		if _, ok := remote.nextRequest(2 * time.Second); !ok {
			t.Fatal("initial requests not sent")
		}
	}
	remote.send(&peer.Message{ID: peer.MsgChoke})
	remote.send(&peer.Message{ID: peer.MsgUnchoke})

	for served := 0; served < 2; served++ {
		work, ok := remote.nextRequest(2 * time.Second)
		if !ok {
			t.Fatal("requests were not re-sent after a quick choke/unchoke")
		}
		remote.serve(work, payload)
	}
	waitDownload(t, done, dir, tf, payload)
}

// Pieces completed after a connection's bitfield was captured, but before it
// joined the manager, must still be announced to it.
func TestNewPeerIsToldAboutPiecesMissingFromItsBitfield(t *testing.T) {
	tf := &torrent.TorrentFile{
		Name: "late.bin", Length: 8, PieceLength: 4,
		PieceHashes: [][20]byte{sha1.Sum([]byte("aaaa")), sha1.Sum([]byte("bbbb"))},
	}
	pc, remote := newScriptedPeer(t, tf) // started with no bitfield

	m := NewManager(tf, []*peer.PeerConnection{pc})
	m.SetCompleted([]bool{true, false}) // completed after the snapshot
	startDownload(t, m)

	deadline := time.After(2 * time.Second)
	for {
		select {
		case id := <-remote.control:
			if id == peer.MsgHave {
				return
			}
		case <-deadline:
			t.Fatal("peer was never told about piece 0")
		}
	}
}

// A piece completed by another source while this peer is silent must release
// the worker at once: it cancels its duplicate requests instead of holding
// them until the snub timeout.
func TestWorkerCancelsWhenPieceCompletedElsewhere(t *testing.T) {
	tf, _ := twoBlockTorrent()
	pc, remote := newScriptedPeer(t, tf)
	remote.send(&peer.Message{ID: peer.MsgBitfield, Payload: []byte{0x80}})
	remote.send(&peer.Message{ID: peer.MsgUnchoke})

	m := NewManager(tf, []*peer.PeerConnection{pc})
	startDownload(t, m) // default 60 s snub timeout

	for range 2 {
		if _, ok := remote.nextRequest(2 * time.Second); !ok {
			t.Fatal("initial requests not sent")
		}
	}
	m.markCompleted(0) // e.g. a webseed finished it; this peer stays silent

	select {
	case <-remote.cancels:
	case <-time.After(2 * time.Second):
		t.Fatal("worker kept its requests after the piece completed elsewhere")
	}
}

// A choked worker re-evaluates its interest when another source completes
// the only piece it needed from that peer.
func TestChokedWorkerDropsInterestWhenNeedCompletedElsewhere(t *testing.T) {
	tf, _ := twoBlockTorrent()
	pc, remote := newScriptedPeer(t, tf)
	remote.send(&peer.Message{ID: peer.MsgBitfield, Payload: []byte{0x80}}) // stays choking

	m := NewManager(tf, []*peer.PeerConnection{pc})
	startDownload(t, m)

	waitFor := func(want peer.MessageID) {
		t.Helper()
		deadline := time.After(2 * time.Second)
		for {
			select {
			case id := <-remote.control:
				if id == want {
					return
				}
			case <-deadline:
				t.Fatalf("message %d not sent", want)
			}
		}
	}
	waitFor(peer.MsgInterested)
	m.markCompleted(0)
	waitFor(peer.MsgNotInterested)
}
