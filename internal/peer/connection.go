package peer

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const defaultTimeout = 30 * time.Second

// PeerConnection manages a single peer's TCP connection with a reader/writer
// goroutine pair. Supports both downloading (requesting blocks) and uploading
// (responding to peer requests).
type PeerConnection struct {
	conn          net.Conn
	peerID        [20]byte
	choked        atomic.Bool
	bitfield      []bool
	incoming      chan *Message  // reader -> manager (wire messages except requests)
	pieceQueue    chan PieceWork // manager -> writer (download requests to send)
	pieceRequests chan PieceWork // reader -> writer (upload requests from remote peer)
	outgoing      chan *Message  // manager/reader -> writer (control messages: unchoke, have, bitfield)
	ctx           context.Context
	cancel        context.CancelFunc
	closeOnce     sync.Once
	direction     Direction
	uploaded      atomic.Uint64
	totalPieces   int
	pieceLength   int64
	totalLength   int64
	metadataMu    sync.RWMutex
	bitfieldMu    sync.RWMutex
	availability  chan struct{}

	// Choked wait: writer parks here when choked, reader signals on unchoke
	unchokeMu   sync.Mutex
	unchokeCond *sync.Cond

	// Upload callback: set by Manager to serve piece data from disk
	getPieceData func(PieceWork) ([]byte, bool)
	handlerMu    sync.RWMutex
}

type Direction string

const (
	DirectionOutbound Direction = "outbound"
	DirectionInbound  Direction = "inbound"
)

type PieceWork struct {
	Index  int
	Begin  int
	Length int
}

func NewPeerConnection(conn net.Conn, peerID [20]byte) *PeerConnection {
	return newPeerConnection(conn, peerID, DirectionOutbound)
}

func NewInboundPeerConnection(conn net.Conn, peerID [20]byte) *PeerConnection {
	return newPeerConnection(conn, peerID, DirectionInbound)
}

func newPeerConnection(conn net.Conn, peerID [20]byte, direction Direction) *PeerConnection {
	ctx, cancel := context.WithCancel(context.Background())
	pc := &PeerConnection{
		conn:          conn,
		peerID:        peerID,
		incoming:      make(chan *Message, 100),
		pieceQueue:    make(chan PieceWork, 10),
		pieceRequests: make(chan PieceWork, 10),
		outgoing:      make(chan *Message, 10),
		ctx:           ctx,
		cancel:        cancel,
		direction:     direction,
		availability:  make(chan struct{}, 1),
	}
	pc.choked.Store(true)
	pc.unchokeCond = sync.NewCond(&pc.unchokeMu)
	return pc
}

func (pc *PeerConnection) Start() {
	go pc.reader()
	go pc.writer()
}

func (pc *PeerConnection) reader() {
	defer pc.Close()
	for {
		// Set deadline
		if err := pc.conn.SetReadDeadline(time.Now().Add(defaultTimeout)); err != nil {
			return
		}

		// Read data
		msg, err := ReadMessage(pc.conn)
		if err != nil {
			return // connection lost
		}
		if msg == nil {
			continue // keep-alive
		}
		totalPieces, pieceLength, totalLength := pc.torrentMetadata()
		if err := ValidateMessage(msg, totalPieces); err != nil {
			return
		}

		// Handle messages that need routing before the generic incoming channel.
		switch msg.ID {
		case MsgChoke:
			pc.choked.Store(true)
		case MsgUnchoke:
			pc.choked.Store(false)
			// Wake the writer so it can resume sending requests.
			pc.unchokeCond.Broadcast()
		case MsgRequest:
			// Remote peer wants a piece from us — route to upload handler.
			if len(msg.Payload) >= 12 {
				index := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
				begin := int(binary.BigEndian.Uint32(msg.Payload[4:8]))
				length := int(binary.BigEndian.Uint32(msg.Payload[8:12]))
				work := PieceWork{Index: index, Begin: begin, Length: length}
				if !ValidateRequest(work, totalPieces, pieceLength, totalLength) {
					continue
				}
				select {
				case pc.pieceRequests <- work:
				case <-pc.ctx.Done():
					return
				}
			}
			continue // don't send to incoming
		case MsgInterested:
			// Peer wants our pieces — auto-unchoke them immediately.
			pc.sendUnchoke()
			continue
		case MsgNotInterested:
			continue
		case MsgBitfield:
			pc.SetBitfield(ParseBitfield(msg.Payload, totalPieces))
			pc.signalAvailability()
		case MsgHave:
			pc.MarkPieceAvailable(binary.BigEndian.Uint32(msg.Payload))
		}

		// Route all other messages (piece, have, bitfield, etc.) to incoming.
		select {
		case pc.incoming <- msg:
		case <-pc.ctx.Done():
			return
		}
	}
}

// sendUnchoke sends an unchoke message to the remote peer via the writer.
func (pc *PeerConnection) sendUnchoke() {
	select {
	case pc.outgoing <- &Message{ID: MsgUnchoke}:
	case <-pc.ctx.Done():
	}
}

// AssignWork queues a download request for the writer to send to the peer.
// Returns false if the peer is choked or the connection is closing.
func (pc *PeerConnection) AssignWork(work PieceWork) bool {
	if pc.choked.Load() {
		return false
	}
	select {
	case pc.pieceQueue <- work:
		return true
	case <-pc.ctx.Done():
		return false
	}
}

// SetPieceDataHandler registers a callback that the writer uses to look up
// piece data when serving upload requests from the remote peer.
func (pc *PeerConnection) SetPieceDataHandler(handler func(PieceWork) ([]byte, bool)) {
	pc.handlerMu.Lock()
	defer pc.handlerMu.Unlock()
	pc.getPieceData = handler
}

func (pc *PeerConnection) pieceData(work PieceWork) ([]byte, bool) {
	pc.handlerMu.RLock()
	handler := pc.getPieceData
	pc.handlerMu.RUnlock()
	if handler == nil {
		return nil, false
	}
	return handler(work)
}

// SendHave queues a Have message to announce a newly completed piece.
func (pc *PeerConnection) SendHave(index uint32) {
	select {
	case pc.outgoing <- BuildHave(index):
	case <-pc.ctx.Done():
	}
}

// SendBitfield queues a Bitfield message to announce our available pieces.
func (pc *PeerConnection) SendBitfield(bits []byte) {
	select {
	case pc.outgoing <- &Message{ID: MsgBitfield, Payload: bits}:
	case <-pc.ctx.Done():
	}
}

// SendInterested queues an Interested message through the connection's single
// writer goroutine. It returns false when the connection is already closed.
func (pc *PeerConnection) SendInterested() bool {
	select {
	case pc.outgoing <- &Message{ID: MsgInterested}:
		return true
	case <-pc.ctx.Done():
		return false
	}
}

func (pc *PeerConnection) writer() {
	defer pc.Close()
	for {
		select {
		case work, ok := <-pc.pieceQueue:
			if !ok {
				return
			}

			pc.waitForUnchoke()

			// If the context was cancelled while we were waiting, bail out.
			if pc.ctx.Err() != nil {
				return
			}

			msg := BuildRequest(work.Index, work.Begin, work.Length)
			if err := pc.writeMessage(msg); err != nil {
				return
			}

		case req, ok := <-pc.pieceRequests:
			if !ok {
				return
			}
			// Upload: remote peer requested a piece from us.
			data, ok := pc.pieceData(req)
			if !ok || len(data) == 0 {
				continue
			}
			msg := BuildPiece(uint32(req.Index), uint32(req.Begin), data)
			if err := pc.writeMessage(msg); err != nil {
				return
			}

		case msg, ok := <-pc.outgoing:
			if !ok {
				return
			}
			if err := pc.writeMessage(msg); err != nil {
				return
			}

		case <-pc.ctx.Done():
			return
		}
	}
}

// writeMessage is a helper that sets the write deadline and writes to TCP.
func (pc *PeerConnection) writeMessage(msg *Message) error {
	if err := pc.conn.SetWriteDeadline(time.Now().Add(defaultTimeout)); err != nil {
		return err
	}
	if err := WriteMessage(pc.conn, msg); err != nil {
		return err
	}
	if msg.ID == MsgPiece && len(msg.Payload) >= 8 {
		pc.uploaded.Add(uint64(len(msg.Payload) - 8))
	}
	return nil
}

// waitForUnchoke blocks until the peer unchokes us or the context is cancelled.
func (pc *PeerConnection) waitForUnchoke() {
	pc.unchokeMu.Lock()
	defer pc.unchokeMu.Unlock()
	for pc.choked.Load() {
		// Check if context was cancelled while we held the lock.
		if pc.ctx.Err() != nil {
			return
		}
		pc.unchokeCond.Wait()
	}
}

func BuildRequest(index, begin, length int) *Message {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))
	return &Message{ID: MsgRequest, Payload: payload}
}

func (pc *PeerConnection) Close() {
	pc.closeOnce.Do(func() {
		pc.cancel()
		pc.choked.Store(false)
		pc.unchokeCond.Broadcast()
		pc.conn.Close()
	})
}

// SetBitfield updates the peer's bitfield (which pieces the peer has).
func (pc *PeerConnection) SetBitfield(bitfield []bool) {
	pc.bitfieldMu.Lock()
	defer pc.bitfieldMu.Unlock()
	pc.bitfield = bitfield
}

// Bitfield returns the peer's current bitfield.
func (pc *PeerConnection) Bitfield() []bool {
	pc.bitfieldMu.RLock()
	defer pc.bitfieldMu.RUnlock()
	out := make([]bool, len(pc.bitfield))
	copy(out, pc.bitfield)
	return out
}

func (pc *PeerConnection) MarkPieceAvailable(index uint32) {
	pc.bitfieldMu.Lock()
	defer pc.bitfieldMu.Unlock()
	if int(index) < len(pc.bitfield) {
		pc.bitfield[index] = true
	}
	pc.signalAvailability()
}

func (pc *PeerConnection) signalAvailability() {
	select {
	case pc.availability <- struct{}{}:
	default:
	}
}

func (pc *PeerConnection) AvailabilityChanged() <-chan struct{} { return pc.availability }

// ConfigureTorrent enables semantic validation of peer bitfields and requests.
func (pc *PeerConnection) ConfigureTorrent(totalPieces int, pieceLength, totalLength int64) {
	pc.metadataMu.Lock()
	defer pc.metadataMu.Unlock()
	pc.totalPieces = totalPieces
	pc.pieceLength = pieceLength
	pc.totalLength = totalLength
}

func (pc *PeerConnection) torrentMetadata() (int, int64, int64) {
	pc.metadataMu.RLock()
	defer pc.metadataMu.RUnlock()
	return pc.totalPieces, pc.pieceLength, pc.totalLength
}

func (pc *PeerConnection) Direction() Direction  { return pc.direction }
func (pc *PeerConnection) PeerID() [20]byte      { return pc.peerID }
func (pc *PeerConnection) UploadedBytes() uint64 { return pc.uploaded.Load() }
func (pc *PeerConnection) Active() bool          { return pc.ctx.Err() == nil }

// Incoming returns the read-only channel of messages from this peer.
func (pc *PeerConnection) Incoming() <-chan *Message {
	return pc.incoming
}

// Done returns a channel that's closed when the peer connection is closed.
// Workers can select on this alongside Incoming() to detect disconnection.
func (pc *PeerConnection) Done() <-chan struct{} {
	return pc.ctx.Done()
}

// RemoteAddr returns the peer's IP:port as a string.
func (pc *PeerConnection) RemoteAddr() string {
	return pc.conn.RemoteAddr().String()
}
