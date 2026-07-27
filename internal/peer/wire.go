package peer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type MessageID uint8

const (
	MsgChoke         MessageID = 0
	MsgUnchoke       MessageID = 1
	MsgInterested    MessageID = 2
	MsgNotInterested MessageID = 3
	MsgHave          MessageID = 4
	MsgBitfield      MessageID = 5
	MsgRequest       MessageID = 6
	MsgPiece         MessageID = 7
	MsgCancel        MessageID = 8
)

const (
	MaxBlockLength       = 16 * 1024
	MaxPeerMessageLength = 4 * 1024 * 1024
)

var (
	ErrInvalidMessage  = errors.New("invalid message")
	ErrMessageTooLarge = errors.New("peer message too large")
)

type Message struct {
	ID      MessageID
	Payload []byte
}

func ReadMessage(r io.Reader) (*Message, error) {
	// Read the message length
	lenBuf := make([]byte, 4)
	_, err := io.ReadFull(r, lenBuf)
	if err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf)

	// Keep alive (length == 0)
	if length == 0 {
		return nil, nil
	}
	if length > MaxPeerMessageLength {
		return nil, fmt.Errorf("%w: %d bytes", ErrMessageTooLarge, length)
	}

	// Read Payload: ID (1 byte) + Payload (length - 1 bytes)
	payloadBuf := make([]byte, length)
	_, err = io.ReadFull(r, payloadBuf)
	if err != nil {
		return nil, err
	}

	return &Message{
		ID:      MessageID(payloadBuf[0]),
		Payload: payloadBuf[1:],
	}, nil
}

// ValidateMessage enforces the BEP 3 payload shapes used by the client.
// Unknown IDs are permitted so extension messages can be ignored safely.
func ValidateMessage(msg *Message, totalPieces int) error {
	if msg == nil {
		return nil
	}
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", ErrInvalidMessage, fmt.Sprintf(format, args...))
	}
	switch msg.ID {
	case MsgChoke, MsgUnchoke, MsgInterested, MsgNotInterested:
		if len(msg.Payload) != 0 {
			return invalid("message %d has %d-byte payload", msg.ID, len(msg.Payload))
		}
	case MsgHave:
		if len(msg.Payload) != 4 {
			return invalid("have payload is %d bytes", len(msg.Payload))
		}
		if totalPieces > 0 && binary.BigEndian.Uint32(msg.Payload) >= uint32(totalPieces) {
			return invalid("have piece index out of range")
		}
	case MsgBitfield:
		if totalPieces > 0 && len(msg.Payload) > (totalPieces+7)/8 {
			return invalid("bitfield is %d bytes for %d pieces", len(msg.Payload), totalPieces)
		}
	case MsgRequest, MsgCancel:
		if len(msg.Payload) != 12 {
			return invalid("message %d payload is %d bytes", msg.ID, len(msg.Payload))
		}
		length := binary.BigEndian.Uint32(msg.Payload[8:12])
		if length == 0 || length > MaxBlockLength {
			return invalid("block length %d is outside 1..%d", length, MaxBlockLength)
		}
	case MsgPiece:
		if len(msg.Payload) < 8 || len(msg.Payload) > 8+MaxBlockLength {
			return invalid("piece payload is %d bytes", len(msg.Payload))
		}
	}
	return nil
}

// ValidateRequest checks an upload request against the torrent's piece bounds.
func ValidateRequest(work PieceWork, totalPieces int, pieceLength, totalLength int64) bool {
	if work.Index < 0 || work.Index >= totalPieces || work.Begin < 0 || work.Length <= 0 || work.Length > MaxBlockLength {
		return false
	}
	if pieceLength <= 0 || totalLength < 0 {
		return false
	}
	pieceSize := pieceLength
	if work.Index == totalPieces-1 {
		pieceSize = totalLength - int64(work.Index)*pieceLength
	}
	begin, length := int64(work.Begin), int64(work.Length)
	return pieceSize >= 0 && begin <= pieceSize && length <= pieceSize-begin
}

func WriteMessage(w io.Writer, msg *Message) error {
	// length = 1 (ID) + len(Payload)
	buf := make([]byte, 4+1+len(msg.Payload))                        // 4 bytes for length, 1 byte for ID, len(Payload) bytes for payload
	binary.BigEndian.PutUint32(buf[0:4], uint32(1+len(msg.Payload))) // store the length of the payload (1 byte for ID + len(Payload))
	buf[4] = byte(msg.ID)
	copy(buf[5:], msg.Payload)
	_, err := w.Write(buf)
	return err
}

func ParseBitfield(payload []byte, totalPieces int) []bool {
	bitfield := make([]bool, totalPieces)
	for i := range totalPieces {
		byteIndex := i / 8
		bitIndex := 7 - (i % 8) // MSB first
		if byteIndex < len(payload) {
			bitfield[i] = (payload[byteIndex]>>bitIndex)&1 == 1
		}
	}
	return bitfield
}

// BuildBitfieldBytes encodes a bool slice into the wire-format byte slice.
func BuildBitfieldBytes(bits []bool) []byte {
	if len(bits) == 0 {
		return nil
	}
	size := (len(bits) + 7) / 8
	buf := make([]byte, size)
	for i, set := range bits {
		if !set {
			continue
		}
		byteIndex := i / 8
		bitIndex := 7 - (i % 8)
		buf[byteIndex] |= 1 << bitIndex
	}
	return buf
}

// BuildPiece constructs a MsgPiece response for the given index, begin offset, and block data.
func BuildPiece(index, begin uint32, data []byte) *Message {
	payload := make([]byte, 8+len(data))
	binary.BigEndian.PutUint32(payload[0:4], index)
	binary.BigEndian.PutUint32(payload[4:8], begin)
	copy(payload[8:], data)
	return &Message{ID: MsgPiece, Payload: payload}
}

// BuildHave constructs a MsgHave for the given piece index.
func BuildHave(index uint32) *Message {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, index)
	return &Message{ID: MsgHave, Payload: payload}
}
