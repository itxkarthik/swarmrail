package torrent

import (
	"os"
	"testing"
)

// FuzzParse checks that metainfo parsing never panics, and that every torrent
// it accepts has consistent, filesystem-safe fields.
func FuzzParse(f *testing.F) {
	if seed, err := os.ReadFile("testdata/big-buck-bunny.torrent"); err == nil {
		f.Add(seed)
	}
	f.Add([]byte("d8:announce3:url4:infod6:lengthi10e4:name1:a12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee"))
	f.Add([]byte("d4:infod5:filesld6:lengthi1e4:pathl1:aeee4:name1:d12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee"))
	f.Fuzz(func(t *testing.T, data []byte) {
		var tf TorrentFile
		if err := tf.Parse(data); err != nil {
			return
		}
		if tf.PieceLength <= 0 || tf.Length < 0 {
			t.Fatalf("accepted piece length %d, length %d", tf.PieceLength, tf.Length)
		}
		if tf.Name == "" || tf.Name == "." || tf.Name == ".." {
			t.Fatalf("accepted unsafe name %q", tf.Name)
		}
	})
}
