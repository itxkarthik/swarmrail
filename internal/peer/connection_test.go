package peer

import (
	"net"
	"testing"
	"time"
)

func TestNewPeerConnection(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	var peerID [20]byte
	copy(peerID[:], []byte("testpeerid12345678"))

	pc := NewPeerConnection(conn1, peerID)
	if pc == nil {
		t.Fatal("NewPeerConnection returned nil")
	}
	if pc.choked.Load() != true {
		t.Fatal("expected PeerConnection to start choked")
	}
	if pc.peerID != peerID {
		t.Fatalf("peerID mismatch")
	}
}

func TestPeerConnection_StartClose(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	var peerID [20]byte
	copy(peerID[:], []byte("testpeerid12345678"))

	pc := NewPeerConnection(conn1, peerID)
	pc.Start()

	// Give goroutines time to start
	time.Sleep(50 * time.Millisecond)

	// Close should not panic (idempotent via sync.Once)
	pc.Close()
	pc.Close() // second call must be safe
}

func TestPeerConnection_StartBeforeClose(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	var peerID [20]byte
	copy(peerID[:], []byte("testpeerid12345678"))

	pc := NewPeerConnection(conn1, peerID)
	pc.Start()
	defer pc.Close()

	// Connection should be alive
	if pc.conn == nil {
		t.Fatal("connection is nil")
	}
}

func TestPeerConnection_ReaderReceivesMessage(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	var peerID [20]byte
	copy(peerID[:], []byte("testpeerid12345678"))

	pc := NewPeerConnection(conn1, peerID)
	pc.ConfigureTorrent(50, 16384, 50*16384)
	pc.SetBitfield(make([]bool, 50))
	pc.Start()
	defer pc.Close()

	// Send a Have message through conn2 (the mock peer side)
	msg := &Message{ID: MsgHave, Payload: []byte{0, 0, 0, 42}}
	if err := WriteMessage(conn2, msg); err != nil {
		t.Fatalf("WriteMessage error: %v", err)
	}

	// Have is folded into connection state, not forwarded to Incoming.
	select {
	case <-pc.StateChanged():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for state change after Have")
	}
	if !pc.Bitfield()[42] {
		t.Fatal("Have did not mark piece 42 as available")
	}
	select {
	case got := <-pc.Incoming():
		t.Fatalf("Have was forwarded to Incoming: %+v", got)
	default:
	}
}

func TestPeerConnection_ReaderHandlesChoke(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	var peerID [20]byte
	copy(peerID[:], []byte("testpeerid12345678"))

	pc := NewPeerConnection(conn1, peerID)
	pc.Start()
	defer pc.Close()

	if !pc.choked.Load() {
		t.Fatal("expected to start choked")
	}

	// Send Unchoke
	if err := WriteMessage(conn2, &Message{ID: MsgUnchoke}); err != nil {
		t.Fatalf("WriteMessage error: %v", err)
	}

	// Give the reader time to process and update state
	time.Sleep(100 * time.Millisecond)

	if pc.choked.Load() {
		t.Fatal("expected to be unchoked after receiving unchoke message")
	}

	// Send Choke
	if err := WriteMessage(conn2, &Message{ID: MsgChoke}); err != nil {
		t.Fatalf("WriteMessage error: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if !pc.choked.Load() {
		t.Fatal("expected to be choked after receiving choke message")
	}
}

func TestPeerConnection_ReaderHandlesUnchoke(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	var peerID [20]byte
	copy(peerID[:], []byte("testpeerid12345678"))

	pc := NewPeerConnection(conn1, peerID)
	pc.Start()
	defer pc.Close()

	if !pc.choked.Load() {
		t.Fatal("expected to start choked")
	}

	// Send Unchoke
	if err := WriteMessage(conn2, &Message{ID: MsgUnchoke}); err != nil {
		t.Fatalf("WriteMessage error: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if pc.choked.Load() {
		t.Fatal("expected to be unchoked")
	}

	// The unchoke is signalled as a state change.
	select {
	case <-pc.StateChanged():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for unchoke state change")
	}
}

func TestPeerConnection_ReaderKeepalive(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	var peerID [20]byte
	copy(peerID[:], []byte("testpeerid12345678"))

	pc := NewPeerConnection(conn1, peerID)
	pc.ConfigureTorrent(2, 16384, 2*16384)
	pc.Start()
	defer pc.Close()

	// Send keep-alive (length=0)
	keepalive := []byte{0, 0, 0, 0}
	if _, err := conn2.Write(keepalive); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	// Keep-alive should be silently consumed — NOT delivered on Incoming
	// Send a real message after it to verify the reader is still alive
	time.Sleep(50 * time.Millisecond)
	if err := WriteMessage(conn2, &Message{ID: MsgHave, Payload: []byte{0, 0, 0, 1}}); err != nil {
		t.Fatalf("WriteMessage error: %v", err)
	}

	select {
	case <-pc.StateChanged():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout — reader likely stalled after keep-alive")
	}
}

func TestPeerConnection_AssignWorkWhenChoked(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	var peerID [20]byte
	copy(peerID[:], []byte("testpeerid12345678"))

	pc := NewPeerConnection(conn1, peerID)
	pc.Start()
	defer pc.Close()

	// Should not accept work when choked
	ok := pc.AssignWork(PieceWork{Index: 5, Begin: 0, Length: 16384})
	if ok {
		t.Fatal("AssignWork should return false when choked")
	}
}

func TestPeerConnection_AssignWorkWhenUnchoked(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	var peerID [20]byte
	copy(peerID[:], []byte("testpeerid12345678"))

	pc := NewPeerConnection(conn1, peerID)
	pc.Start()
	defer pc.Close()

	// Send unchoke
	if err := WriteMessage(conn2, &Message{ID: MsgUnchoke}); err != nil {
		t.Fatalf("WriteMessage error: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Should accept work now
	ok := pc.AssignWork(PieceWork{Index: 5, Begin: 0, Length: 16384})
	if !ok {
		t.Fatal("AssignWork should return true when unchoked")
	}
}

func TestPeerConnection_SetAndGetBitfield(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	var peerID [20]byte
	copy(peerID[:], []byte("testpeerid12345678"))

	pc := NewPeerConnection(conn1, peerID)
	pc.Start()
	defer pc.Close()

	bitfield := []bool{true, false, true, true, false}
	pc.SetBitfield(bitfield)

	got := pc.Bitfield()
	if len(got) != len(bitfield) {
		t.Fatalf("bitfield length: got %d, want %d", len(got), len(bitfield))
	}
	for i := range bitfield {
		if got[i] != bitfield[i] {
			t.Fatalf("bitfield[%d]: got %v, want %v", i, got[i], bitfield[i])
		}
	}
}

func TestPeerConnection_CloseTwice(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	var peerID [20]byte
	copy(peerID[:], []byte("testpeerid12345678"))

	pc := NewPeerConnection(conn1, peerID)
	pc.Start()

	// Close twice — must not panic
	pc.Close()
	pc.Close()
}

func TestPeerConnection_ReaderStopsOnClose(t *testing.T) {
	conn1, conn2 := net.Pipe()

	var peerID [20]byte
	copy(peerID[:], []byte("testpeerid12345678"))

	pc := NewPeerConnection(conn1, peerID)
	pc.Start()

	// Close the connection — reader should notice and exit
	conn2.Close()

	// Give reader time to detect the closed connection
	time.Sleep(200 * time.Millisecond)

	// Close should still be safe
	pc.Close()
}

func TestPeerConnectionCountsSuccessfullyUploadedPayload(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	pc := NewInboundPeerConnection(client, [20]byte{1})
	pc.ConfigureTorrent(1, 4, 4)
	pc.SetPieceDataHandler(func(PieceWork) ([]byte, bool) { return []byte("data"), true })
	pc.Start()
	if err := WriteMessage(server, BuildRequest(0, 0, 4)); err != nil {
		t.Fatal(err)
	}
	msg, err := ReadMessage(server)
	if err != nil {
		t.Fatal(err)
	}
	if msg.ID != MsgPiece || string(msg.Payload[8:]) != "data" {
		t.Fatalf("response = %+v", msg)
	}
	deadline := time.Now().Add(time.Second)
	for pc.UploadedBytes() != 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := pc.UploadedBytes(); got != 4 {
		t.Fatalf("UploadedBytes() = %d, want 4", got)
	}
	pc.Close()
}

func TestPeerConnectionTracksLateBitfieldAndSignalsAvailability(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	pc := NewInboundPeerConnection(client, [20]byte{1})
	pc.ConfigureTorrent(2, 4, 8)
	pc.SetBitfield(make([]bool, 2))
	pc.Start()
	if err := WriteMessage(server, &Message{ID: MsgBitfield, Payload: []byte{0b10000000}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-pc.StateChanged():
	case <-time.After(time.Second):
		t.Fatal("late bitfield did not signal availability")
	}
	if bits := pc.Bitfield(); len(bits) != 2 || !bits[0] || bits[1] {
		t.Fatalf("bitfield = %v, want [true false]", bits)
	}
	pc.Close()
}

// An idle worker never reads Incoming. Before the fix, the reader forwarded
// every Have to Incoming, blocked once its 100-slot buffer filled, and then
// stopped reading upload requests too.
func TestReaderKeepsServingRequestsAfterManyHaves(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	const pieces = 300
	pc := NewInboundPeerConnection(client, [20]byte{1})
	pc.ConfigureTorrent(pieces, 4, pieces*4)
	pc.SetBitfield(make([]bool, pieces))
	pc.SetPieceDataHandler(func(PieceWork) ([]byte, bool) { return []byte("data"), true })
	pc.Start()
	defer pc.Close()

	go func() {
		for i := range uint32(200) {
			if err := WriteMessage(server, BuildHave(i)); err != nil {
				return
			}
		}
		_ = WriteMessage(server, BuildRequest(0, 0, 4))
	}()

	_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
	msg, err := ReadMessage(server)
	if err != nil {
		t.Fatalf("no reply to request after 200 Have messages: %v", err)
	}
	if msg.ID != MsgPiece {
		t.Fatalf("reply ID = %d, want piece", msg.ID)
	}
	if got := pc.Bitfield(); !got[0] || !got[199] || got[200] {
		t.Fatal("Have messages were not applied to the bitfield")
	}
}
