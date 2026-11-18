package peer

import (
	"bytes"
	"testing"
)

// FuzzReadMessage checks that framing and validation never panic on
// untrusted peer input and never allocate beyond MaxPeerMessageLength.
func FuzzReadMessage(f *testing.F) {
	for _, seed := range [][]byte{
		{0, 0, 0, 0},
		{0, 0, 0, 1, 1},
		{0, 0, 0, 5, 4, 0, 0, 0, 7},
		{0, 0, 0, 13, 6, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 64, 0},
		{0, 0, 0, 2, 5, 0xFF},
		{0xFF, 0xFF, 0xFF, 0xFF, 7},
	} {
		f.Add(seed, 8)
	}
	f.Fuzz(func(t *testing.T, data []byte, totalPieces int) {
		r := bytes.NewReader(data)
		for {
			msg, err := ReadMessage(r)
			if err != nil {
				return
			}
			if msg == nil {
				continue
			}
			if len(msg.Payload)+1 > MaxPeerMessageLength {
				t.Fatalf("accepted a %d-byte frame", len(msg.Payload)+1)
			}
			if ValidateMessage(msg, totalPieces) == nil && msg.ID == MsgBitfield && totalPieces >= 0 {
				_ = ParseBitfield(msg.Payload, min(totalPieces, 1<<16))
			}
		}
	})
}
