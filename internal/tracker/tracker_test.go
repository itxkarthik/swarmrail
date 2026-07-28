package tracker

import (
	"encoding/hex"
	"os"
	"testing"

	"github.com/itxkarthik/swarmrail/internal/torrent"
)

func TestAnnounceHTTPWithTorrentFile(t *testing.T) {
	const torrentPath = "testdata/big-buck-bunny.torrent"
	const peerIDHex = "2d474f303030312d313233343536373839303132" // "-GO0001-123456789012"

	data, err := os.ReadFile(torrentPath)
	if err != nil {
		t.Skipf("missing %s: %v", torrentPath, err)
	}

	var tf torrent.TorrentFile
	if err := tf.Parse(data); err != nil {
		t.Fatalf("failed to parse torrent: %v", err)
	}

	peerIDBytes, err := hex.DecodeString(peerIDHex)
	if err != nil || len(peerIDBytes) != 20 {
		t.Fatalf("peerIDHex must be 40 hex chars (20 bytes), got %q", peerIDHex)
	}
	var peerID [20]byte
	copy(peerID[:], peerIDBytes)

	mock := NewMockTrackerServer(t)
	peers, err := AnnounceHTTP(mock.URL, tf.InfoHash, peerID, 6881, tf.Length)
	if err != nil {
		t.Fatalf("announce failed: %v", err)
	}
	if len(peers) != len(mock.Peers) {
		t.Fatalf("peers = %d, want %d", len(peers), len(mock.Peers))
	}
	if mock.LastInfoHash != tf.InfoHash || mock.LastPeerID != peerID || mock.LastPort != 6881 {
		t.Fatalf("tracker request mismatch: hash=%x peerID=%x port=%d", mock.LastInfoHash, mock.LastPeerID, mock.LastPort)
	}
}
