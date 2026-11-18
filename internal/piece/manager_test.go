package piece

import (
	"context"
	"crypto/sha1"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/itxkarthik/swarmrail/internal/peer"
	"github.com/itxkarthik/swarmrail/internal/torrent"
	"github.com/itxkarthik/swarmrail/internal/webseed"
)

type fakeWebSeed struct {
	pieces map[uint32][]byte
}

func (f *fakeWebSeed) Name() string { return "fake" }
func (f *fakeWebSeed) FetchPiece(ctx context.Context, index uint32, length int64) ([]byte, error) {
	data := f.pieces[index]
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}
func (f *fakeWebSeed) Disable()        {}
func (f *fakeWebSeed) Available() bool { return true }
func (f *fakeWebSeed) Snapshot() webseed.Snapshot {
	return webseed.Snapshot{Name: f.Name(), State: webseed.StateReady}
}

// VerifyPiece Tests

func TestVerifyPiece_Success(t *testing.T) {
	expectedData := []byte("hello world, this is test piece data for verification")
	hash := sha1.Sum(expectedData)

	m := &Manager{
		pieceHashes: [][20]byte{hash},
	}

	if !m.verifyPiece(0, expectedData) {
		t.Fatal("expected piece verification to pass")
	}
}

func TestVerifyPiece_Failure(t *testing.T) {
	expectedData := []byte("correct piece data")
	wrongData := []byte("corrupted piece data")
	hash := sha1.Sum(expectedData)

	m := &Manager{
		pieceHashes: [][20]byte{hash},
	}

	if m.verifyPiece(0, wrongData) {
		t.Fatal("expected piece verification to fail with wrong data")
	}
}

func TestVerifyPiece_MultiplePieces(t *testing.T) {
	data0 := []byte("piece zero data")
	data1 := []byte("piece one data")
	hash0 := sha1.Sum(data0)
	hash1 := sha1.Sum(data1)

	m := &Manager{
		pieceHashes: [][20]byte{hash0, hash1},
	}

	if !m.verifyPiece(0, data0) {
		t.Fatal("piece 0 should verify correctly")
	}
	if !m.verifyPiece(1, data1) {
		t.Fatal("piece 1 should verify correctly")
	}
	if m.verifyPiece(0, data1) {
		t.Fatal("piece 0 should NOT match data1's hash")
	}
}

func TestUploadBlockRejectsInvalidRanges(t *testing.T) {
	m := &Manager{
		totalPieces: 2,
		pieceLength: 4,
		totalLength: 6,
		completed:   []bool{true, true},
	}
	for _, work := range []peer.PieceWork{
		{Index: -1, Begin: 0, Length: 1},
		{Index: 2, Begin: 0, Length: 1},
		{Index: 1, Begin: 1, Length: 2},
		{Index: 0, Begin: 0, Length: peer.MaxBlockLength + 1},
	} {
		if data, ok := m.readUploadBlock(work); ok || data != nil {
			t.Fatalf("readUploadBlock(%+v) = (%v, %v), want rejected", work, data, ok)
		}
	}
}

func TestDownloadWebSeedOnly(t *testing.T) {
	piece0 := []byte("abcd")
	piece1 := []byte("ef")
	hash0 := sha1.Sum(piece0)
	hash1 := sha1.Sum(piece1)

	tf := &torrent.TorrentFile{
		Name:        "out.bin",
		Length:      6,
		PieceLength: 4,
		PieceHashes: [][20]byte{hash0, hash1},
	}
	m := NewManager(tf, nil)
	m.webseeds = []webseed.Source{&fakeWebSeed{pieces: map[uint32][]byte{
		0: piece0,
		1: piece1,
	}}}
	m.SetLogWriter(io.Discard)

	dir := t.TempDir()
	if err := m.Download(context.Background(), dir, true); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "out.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "abcdef" {
		t.Fatalf("file = %q, want abcdef", data)
	}
}

func TestDownloadAcceptsPeerAddedAtRuntime(t *testing.T) {
	data := []byte("abcd")
	hash := sha1.Sum(data)
	tf := &torrent.TorrentFile{
		Name:        "dynamic.bin",
		Length:      int64(len(data)),
		PieceLength: int64(len(data)),
		PieceHashes: [][20]byte{hash},
	}

	m := NewManager(tf, nil)
	m.SetLogWriter(io.Discard)
	dir := t.TempDir()
	done := make(chan error, 1)
	go func() { done <- m.Download(context.Background(), dir, true) }()

	deadline := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		active := m.downloading
		m.mu.Unlock()
		if active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("download did not become active")
		}
		time.Sleep(time.Millisecond)
	}

	dynamic, closeDynamic := newServingPeer(t, []bool{true}, data)
	defer closeDynamic()
	time.Sleep(20 * time.Millisecond)
	if added := m.AddPeers([]*peer.PeerConnection{dynamic}); added != 1 {
		t.Fatalf("AddPeers() = %d, want 1", added)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("download did not complete from runtime peer")
	}

	written, err := os.ReadFile(filepath.Join(dir, "dynamic.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(written) != string(data) {
		t.Fatalf("downloaded data = %q, want %q", written, data)
	}
}

func TestAddPeersRejectsDuplicatePeerID(t *testing.T) {
	firstClient, firstServer := net.Pipe()
	defer firstServer.Close()
	first := peer.NewPeerConnection(firstClient, [20]byte{1})
	secondClient, secondServer := net.Pipe()
	defer secondServer.Close()
	second := peer.NewPeerConnection(secondClient, [20]byte{1})
	m := NewManager(&torrent.TorrentFile{Name: "dupe", Length: 1, PieceLength: 1, PieceHashes: make([][20]byte, 1)}, nil)
	if added := m.AddPeers([]*peer.PeerConnection{first, second}); added != 1 {
		t.Fatalf("AddPeers() = %d, want one unique peer", added)
	}
	defer first.Close()
}

func TestAddPeersRejectsDuplicateEndpoint(t *testing.T) {
	firstClient, firstServer := net.Pipe()
	defer firstServer.Close()
	first := peer.NewPeerConnection(firstClient, [20]byte{1})
	secondClient, secondServer := net.Pipe()
	defer secondServer.Close()
	second := peer.NewPeerConnection(secondClient, [20]byte{2})
	m := NewManager(&torrent.TorrentFile{Name: "dupe", Length: 1, PieceLength: 1, PieceHashes: make([][20]byte, 1)}, nil)
	if added := m.AddPeers([]*peer.PeerConnection{first, second}); added != 1 {
		t.Fatalf("AddPeers() = %d, want one unique endpoint", added)
	}
	defer first.Close()
}

func TestDownloadCancellationPersistsResumeState(t *testing.T) {
	tf := &torrent.TorrentFile{
		Name: "cancel.bin", Length: 4, PieceLength: 4,
		PieceHashes: [][20]byte{sha1.Sum([]byte("data"))},
		InfoHash:    [20]byte{1, 2, 3},
	}
	dormant, closeDormant := newServingPeer(t, []bool{false}, nil)
	defer closeDormant()
	m := NewManager(tf, []*peer.PeerConnection{dormant})
	m.SetLogWriter(io.Discard)
	dir := t.TempDir()
	statePath := StateFilePath(dir, tf.Name, tf.InfoHash)
	m.EnablePeriodicSave(statePath, tf.InfoHash)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Download(ctx, dir, true) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Download() error = %v, want context.Canceled", err)
	}
	state, err := LoadResume(statePath, tf.InfoHash)
	if err != nil {
		t.Fatalf("resume state not saved on cancellation: %v", err)
	}
	if len(state.Completed) != 1 || state.Completed[0] {
		t.Fatalf("saved completion = %v, want [false]", state.Completed)
	}
}

func TestSourceSnapshotsIncludesWebseedsAndPeers(t *testing.T) {
	connection, closeConnection := newServingPeer(t, []bool{false}, nil)
	defer closeConnection()
	m := &Manager{
		peers:    []*peer.PeerConnection{connection},
		webseeds: []webseed.Source{&fakeWebSeed{}},
	}
	snapshots := m.SourceSnapshots()
	if len(snapshots) != 2 {
		t.Fatalf("SourceSnapshots() count = %d, want 2", len(snapshots))
	}
	if snapshots[0].Kind != SourcePeer || snapshots[0].State != "active" || snapshots[0].Direction != "outbound" {
		t.Fatalf("peer snapshot = %+v", snapshots[0])
	}
	if snapshots[1].Kind != SourceWebSeed || snapshots[1].Endpoint != "fake" || snapshots[1].State != "ready" {
		t.Fatalf("webseed snapshot = %+v", snapshots[1])
	}
}

func newServingPeer(t *testing.T, bitfield []bool, pieceData []byte) (*peer.PeerConnection, func()) {
	t.Helper()
	client, server := net.Pipe()
	connection := peer.NewPeerConnection(client, [20]byte{})
	connection.SetBitfield(bitfield)
	connection.Start(nil)

	go func() {
		_ = peer.WriteMessage(server, &peer.Message{ID: peer.MsgUnchoke})
		for {
			msg, err := peer.ReadMessage(server)
			if err != nil {
				return
			}
			if msg == nil || msg.ID != peer.MsgRequest || len(pieceData) == 0 {
				continue
			}
			_ = peer.WriteMessage(server, peer.BuildPiece(0, 0, pieceData))
		}
	}()

	return connection, func() {
		connection.Close()
		server.Close()
	}
}

// nextPiece Tests

func TestNextPiece_SelectsRarest(t *testing.T) {
	// 5 pieces, 3 peers with different availability
	peers := []*peer.PeerConnection{
		makePeerWithBitfield([]bool{true, true, true, false, false}),  // has 0,1,2
		makePeerWithBitfield([]bool{true, true, false, true, false}),  // has 0,1,3
		makePeerWithBitfield([]bool{true, false, true, false, false}), // has 0,2
	}

	m := &Manager{
		totalPieces: 5,
		peers:       peers,
		completed:   make([]bool, 5),
		inProgress:  make(map[uint32]bool),
	}

	// Peer 0 has pieces 0,1,2. Availability: piece0=3, piece1=2, piece2=2.
	// Rarest among what peer0 has: piece1 or piece2 (avail=2).
	idx := m.nextPiece(peers[0])
	if idx != 1 && idx != 2 {
		t.Fatalf("expected nextPiece for peer0 to be 1 or 2 (avail=2), got %d", idx)
	}
}

func TestNextPiece_SkipsCompleted(t *testing.T) {
	peers := []*peer.PeerConnection{
		makePeerWithBitfield([]bool{true, true, true}),
		makePeerWithBitfield([]bool{true, true, false}),
	}

	m := &Manager{
		totalPieces: 3,
		peers:       peers,
		completed:   []bool{false, true, false},
		inProgress:  make(map[uint32]bool),
	}

	// Piece 1 is completed, should be skipped.
	// Peer0 has pieces 0,1,2. Skipping 1: piece0 avail=2, piece2 avail=1.
	// Rarest: piece2.
	idx := m.nextPiece(peers[0])
	if idx != 2 {
		t.Fatalf("expected nextPiece (skipping completed) to be 2, got %d", idx)
	}
}

func TestCompletedBytesUsesExactFinalPieceSize(t *testing.T) {
	m := &Manager{
		totalPieces: 3,
		pieceLength: 4,
		totalLength: 10,
		completed:   []bool{true, false, true},
	}

	if got, want := m.CompletedBytes(), uint64(6); got != want {
		t.Fatalf("CompletedBytes() = %d, want %d", got, want)
	}
}

func TestCompletedBytesIncludesResumedPieces(t *testing.T) {
	m := &Manager{
		totalPieces: 3,
		pieceLength: 4,
		totalLength: 10,
		completed:   make([]bool, 3),
	}
	m.SetCompleted([]bool{true, true, false})

	if got, want := m.CompletedBytes(), uint64(8); got != want {
		t.Fatalf("CompletedBytes() = %d, want %d", got, want)
	}
}

func TestNextPiece_SkipsInProgressForRarest(t *testing.T) {
	peers := []*peer.PeerConnection{
		makePeerWithBitfield([]bool{true, true, true}),
		makePeerWithBitfield([]bool{true, false, true}),
	}

	m := &Manager{
		totalPieces: 3,
		peers:       peers,
		completed:   make([]bool, 3),
		inProgress:  map[uint32]bool{1: true},
	}

	// RarestPiece skips inProgress (piece 1), so only 0 and 2 are eligible.
	// Availability: piece0=2, piece2=2. Both equal, pick randomly.
	idx := m.nextPiece(peers[0])
	if idx != 0 && idx != 2 {
		t.Fatalf("expected nextPiece (skipping in-progress) to be 0 or 2, got %d", idx)
	}
}

func TestNextPiece_FallsBackToFirstPiece(t *testing.T) {
	// All pieces the peer has are either completed or inProgress.
	// RarestPiece returns -1, so nextPiece falls back to FirstPiece,
	// which ignores inProgress — acting as endgame.
	peers := []*peer.PeerConnection{
		makePeerWithBitfield([]bool{true, true, true}),
		makePeerWithBitfield([]bool{true, true, false}),
	}

	m := &Manager{
		totalPieces: 3,
		peers:       peers,
		completed:   []bool{false, false, true},
		inProgress:  map[uint32]bool{0: true, 1: true},
	}

	// Peer0 has all 3 pieces. Pieces 0 and 1 are inProgress, piece 2 is completed.
	// RarestPiece skips completed and inProgress → -1 (no eligible pieces).
	// Falls back to FirstPiece: skips only completed (piece 2) → returns 0.
	idx := m.nextPiece(peers[0])
	if idx != 0 {
		t.Fatalf("expected nextPiece to fall back to FirstPiece returning 0, got %d", idx)
	}
}

func TestNextPiece_NoAvailablePieces(t *testing.T) {
	peers := []*peer.PeerConnection{
		makePeerWithBitfield([]bool{false, false}),
	}

	m := &Manager{
		totalPieces: 2,
		peers:       peers,
		completed:   make([]bool, 2),
		inProgress:  make(map[uint32]bool),
	}

	idx := m.nextPiece(peers[0])
	if idx != -1 {
		t.Fatalf("expected -1 when no pieces available, got %d", idx)
	}
}

func TestNextPiece_AllCompleted(t *testing.T) {
	peers := []*peer.PeerConnection{
		makePeerWithBitfield([]bool{true, true}),
	}

	m := &Manager{
		totalPieces: 2,
		peers:       peers,
		completed:   []bool{true, true},
		inProgress:  make(map[uint32]bool),
	}

	idx := m.nextPiece(peers[0])
	if idx != -1 {
		t.Fatalf("expected -1 when all pieces completed, got %d", idx)
	}
}

func TestNextPiece_EndgameSelectsInProgress(t *testing.T) {
	// In endgame mode (all fresh pieces claimed), FirstPiece fallback
	// allows selecting pieces that are inProgress.
	peers := []*peer.PeerConnection{
		makePeerWithBitfield([]bool{true, true, true}),
		makePeerWithBitfield([]bool{true, true, true}),
	}

	m := &Manager{
		totalPieces: 3,
		peers:       peers,
		completed:   []bool{false, false, false},
		inProgress:  map[uint32]bool{0: true, 1: true},
	}

	// Piece 2 is the only fresh one — RarestPiece should pick it.
	// After piece 2 is claimed, next round: 0 and 1 are inProgress,
	// RarestPiece returns -1, falls back to FirstPiece → 0.
	idx1 := m.nextPiece(peers[0])
	if idx1 != 2 {
		t.Fatalf("expected first selection to be piece 2 (only fresh one), got %d", idx1)
	}

	// Mark piece 2 as inProgress too, simulating it was claimed.
	m.inProgress[2] = true

	// Now everything is inProgress — FirstPiece fallback should pick 0.
	idx2 := m.nextPiece(peers[0])
	if idx2 != 0 {
		t.Fatalf("expected endgame fallback to return 0, got %d", idx2)
	}
}

// InProgress/Completed Helpers

func TestMarkers(t *testing.T) {
	m := &Manager{
		totalPieces: 3,
		completed:   make([]bool, 3),
		inProgress:  make(map[uint32]bool),
	}

	if m.isCompleted(0) {
		t.Fatal("piece 0 should not be completed initially")
	}

	m.markCompleted(0)
	if !m.isCompleted(0) {
		t.Fatal("piece 0 should be completed after marking")
	}
	if m.isCompleted(2) {
		t.Fatal("piece 2 should still not be completed")
	}

	if m.isInProgress(1) {
		t.Fatal("piece 1 should not be in progress initially")
	}
	m.markInProgress(1)
	if !m.isInProgress(1) {
		t.Fatal("piece 1 should be in progress after marking")
	}
	m.markNotInProgress(1)
	if m.isInProgress(1) {
		t.Fatal("piece 1 should not be in progress after clearing")
	}
}
