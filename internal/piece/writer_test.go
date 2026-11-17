package piece

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/itxkarthik/swarmrail/internal/peer"
	"github.com/itxkarthik/swarmrail/internal/torrent"
)

// patterned returns n bytes where every 16 KiB block has a distinct value, so
// reading the wrong block is always detectable.
func patterned(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i/blockSize + 1)
	}
	return data
}

func newTestWriter(t *testing.T, tf *torrent.TorrentFile, payload []byte) *PieceWriter {
	t.Helper()
	pw, err := NewPieceWriter(t.TempDir(), tf, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pw.Close() })
	for i := int64(0); i*tf.PieceLength < int64(len(payload)); i++ {
		end := min((i+1)*tf.PieceLength, int64(len(payload)))
		if err := pw.WritePiece(uint32(i), payload[i*tf.PieceLength:end]); err != nil {
			t.Fatal(err)
		}
	}
	return pw
}

func TestReadBlockHonoursBeginOffset(t *testing.T) {
	const pieceLen = 2 * blockSize
	payload := patterned(2 * pieceLen)
	tf := &torrent.TorrentFile{Name: "single.bin", PieceLength: pieceLen, Length: int64(len(payload))}
	pw := newTestWriter(t, tf, payload)

	for _, tc := range []struct{ index, begin uint32 }{{0, 0}, {0, blockSize}, {1, 0}, {1, blockSize}, {1, 100}} {
		got := make([]byte, blockSize-100)
		if _, err := pw.ReadBlock(tc.index, tc.begin, got); err != nil {
			t.Fatalf("ReadBlock(%d, %d): %v", tc.index, tc.begin, err)
		}
		off := int(tc.index)*pieceLen + int(tc.begin)
		if want := payload[off : off+len(got)]; !bytes.Equal(got, want) {
			t.Fatalf("ReadBlock(%d, %d) returned bytes from the wrong offset", tc.index, tc.begin)
		}
	}
}

func TestReadBlockAcrossFileBoundary(t *testing.T) {
	const pieceLen = 2 * blockSize
	// The first file ends 1000 bytes into the second block of piece 0.
	files := []torrent.FileEntry{
		{Path: []string{"a.bin"}, Length: blockSize + 1000},
		{Path: []string{"dir", "b.bin"}, Length: 3*blockSize - 1000},
	}
	payload := patterned(4 * blockSize)
	tf := &torrent.TorrentFile{Name: "multi", PieceLength: pieceLen, Length: int64(len(payload)), Files: files}
	pw := newTestWriter(t, tf, payload)

	got := make([]byte, blockSize)
	if _, err := pw.ReadBlock(0, blockSize, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload[blockSize:2*blockSize]) {
		t.Fatal("block straddling two files returned the wrong bytes")
	}
}

func TestReadBlockPastEndFails(t *testing.T) {
	payload := patterned(blockSize)
	tf := &torrent.TorrentFile{Name: "short.bin", PieceLength: blockSize, Length: int64(len(payload))}
	pw := newTestWriter(t, tf, payload)

	if _, err := pw.ReadBlock(0, 100, make([]byte, blockSize)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestReadUploadBlockServesRequestedOffset(t *testing.T) {
	const pieceLen = 2 * blockSize
	payload := patterned(pieceLen)
	tf := &torrent.TorrentFile{Name: "upload.bin", PieceLength: pieceLen, Length: pieceLen}
	pw := newTestWriter(t, tf, payload)

	m := &Manager{totalPieces: 1, pieceLength: pieceLen, totalLength: pieceLen, completed: []bool{true}, pw: pw}
	data, ok := m.readUploadBlock(peer.PieceWork{Index: 0, Begin: blockSize, Length: blockSize})
	if !ok {
		t.Fatal("readUploadBlock rejected a valid request")
	}
	if !bytes.Equal(data, payload[blockSize:]) {
		t.Fatal("upload for begin=16384 returned the piece's first block")
	}
}

func TestFileSpans(t *testing.T) {
	lengths := []int64{10, 0, 5, 20}
	tests := []struct {
		name   string
		off, n int64
		want   []span
	}{
		{"inside first file", 2, 3, []span{{file: 0, fileOff: 2, bufOff: 0, length: 3}}},
		{"skips empty file", 8, 4, []span{{0, 8, 0, 2}, {2, 0, 2, 2}}},
		{"crosses three files", 9, 10, []span{{0, 9, 0, 1}, {2, 0, 1, 5}, {3, 0, 6, 4}}},
		{"past the end", 33, 5, []span{{3, 18, 0, 2}}},
		{"empty range", 4, 0, nil},
		{"negative offset", -1, 4, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fileSpans(lengths, tt.off, tt.n); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("fileSpans(%d, %d) = %+v, want %+v", tt.off, tt.n, got, tt.want)
			}
		})
	}
}
