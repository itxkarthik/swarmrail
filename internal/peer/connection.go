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

// MaxPendingRequests caps the block requests a download worker keeps
// outstanding on one connection. It also sizes the incoming channel, so the
// reader can always hand over a requested block without blocking.
const MaxPendingRequests = 32

// PeerConnection manages a single peer's TCP connection with a reader/writer
// goroutine pair. Supports both downloading (requesting blocks) and uploading
// (responding to peer requests).
type PeerConnection struct {
	conn          net.Conn
	peerID        [20]byte
	peerChoking   atomic.Bool    // BEP 3 peer_choking: the remote is choking us
	amInterested  atomic.Bool    // BEP 3 am_interested: we told the remote we want data
	bitfield      []bool         // pieces the remote has
	incoming      chan *Message  // reader -> download worker (requested blocks only)
	pieceRequests chan PieceWork // reader -> writer (upload requests from remote peer)
	outgoing      chan *Message  // everyone -> writer (requests, control messages)
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
	stateChanged  chan struct{} // cap 1; signalled on choke, unchoke, have and bitfield

	// pending holds block requests sent to the remote and not yet answered.
	// The reader forwards a Piece message only when it matches one of them.
	pendingMu sync.Mutex
	pending   map[PieceWork]struct{}

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
		incoming:      make(chan *Message, MaxPendingRequests),
		pieceRequests: make(chan PieceWork, 10),
		outgoing:      make(chan *Message, 2*MaxPendingRequests),
		ctx:           ctx,
		cancel:        cancel,
		direction:     direction,
		stateChanged:  make(chan struct{}, 1),
		pending:       make(map[PieceWork]struct{}),
	}
	pc.peerChoking.Store(true) // BEP 3: connections start choked
	return pc
}

// Start launches the reader and writer goroutines. If first is not nil it is
// written before any other message; callers pass the local bitfield so that,
// as BEP 3 requires, it is the first message after the handshake. Pass nil
// when we have no pieces: an all-zero bitfield is optional.
func (pc *PeerConnection) Start(first *Message) {
	go pc.reader()
	go pc.writer(first)
}

func (pc *PeerConnection) reader() {
	defer pc.Close()
	seenMessage := false
	for {
		if err := pc.conn.SetReadDeadline(time.Now().Add(defaultTimeout)); err != nil {
			return
		}

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
		// BEP 3: bitfield is only ever sent as the first message.
		if msg.ID == MsgBitfield && seenMessage {
			return
		}
		seenMessage = true

		// Fold protocol state into the connection here and forward only the
		// messages a download worker consumes. Anything else on the incoming
		// channel would sit there while the worker is idle, fill the buffer
		// and block this goroutine, which would also stop uploads.
		switch msg.ID {
		case MsgChoke:
			// BEP 3: a choking peer discards our outstanding requests.
			pc.peerChoking.Store(true)
			pc.clearPending()
			pc.signalStateChange()
		case MsgUnchoke:
			pc.peerChoking.Store(false)
			pc.signalStateChange()
		case MsgRequest:
			work := PieceWork{
				Index:  int(binary.BigEndian.Uint32(msg.Payload[0:4])),
				Begin:  int(binary.BigEndian.Uint32(msg.Payload[4:8])),
				Length: int(binary.BigEndian.Uint32(msg.Payload[8:12])),
			}
			if !ValidateRequest(work, totalPieces, pieceLength, totalLength) {
				continue
			}
			select {
			case pc.pieceRequests <- work:
			case <-pc.ctx.Done():
				return
			}
		case MsgInterested:
			// Peer wants our pieces — auto-unchoke them immediately.
			pc.sendUnchoke()
		case MsgBitfield:
			pc.SetBitfield(ParseBitfield(msg.Payload, totalPieces))
			pc.signalStateChange()
		case MsgHave:
			pc.MarkPieceAvailable(binary.BigEndian.Uint32(msg.Payload))
		case MsgPiece:
			pc.deliverBlock(msg)
		}
		// Cancel, NotInterested and unsupported extension messages need no
		// action yet.
	}
}

// deliverBlock hands a Piece message to the download worker if it answers a
// request we sent. Unrequested blocks (late replies after a choke or cancel,
// or junk) are dropped. Because only pending requests are forwarded and
// pending is capped at MaxPendingRequests, the send never has to block.
func (pc *PeerConnection) deliverBlock(msg *Message) {
	work := PieceWork{
		Index:  int(binary.BigEndian.Uint32(msg.Payload[0:4])),
		Begin:  int(binary.BigEndian.Uint32(msg.Payload[4:8])),
		Length: len(msg.Payload) - 8,
	}
	pc.pendingMu.Lock()
	_, requested := pc.pending[work]
	delete(pc.pending, work)
	pc.pendingMu.Unlock()
	if !requested {
		return
	}
	select {
	case pc.incoming <- msg:
	default:
		// Unreachable while the pending cap holds; dropping is safe because
		// the worker re-requests blocks that never arrive.
	}
}

// sendUnchoke sends an unchoke message to the remote peer via the writer.
func (pc *PeerConnection) sendUnchoke() {
	select {
	case pc.outgoing <- &Message{ID: MsgUnchoke}:
	case <-pc.ctx.Done():
	}
}

// PeerChoking reports whether the remote is currently choking us.
func (pc *PeerConnection) PeerChoking() bool { return pc.peerChoking.Load() }

// Request sends a block request to the remote. It returns false without
// sending anything when the remote is choking us, MaxPendingRequests are
// already outstanding, or the connection is closed. Requests are tracked so
// the matching Piece reply can be delivered on Incoming.
func (pc *PeerConnection) Request(work PieceWork) bool {
	if pc.peerChoking.Load() {
		return false
	}
	pc.pendingMu.Lock()
	if len(pc.pending) >= MaxPendingRequests {
		pc.pendingMu.Unlock()
		return false
	}
	pc.pending[work] = struct{}{}
	pc.pendingMu.Unlock()

	select {
	case pc.outgoing <- BuildRequest(work.Index, work.Begin, work.Length):
		return true
	case <-pc.ctx.Done():
		return false
	}
}

// CancelPending sends Cancel for every outstanding request and forgets them,
// so late replies are dropped. Workers call it when they abandon a piece.
func (pc *PeerConnection) CancelPending() {
	for _, work := range pc.clearPending() {
		msg := BuildRequest(work.Index, work.Begin, work.Length)
		msg.ID = MsgCancel
		select {
		case pc.outgoing <- msg:
		case <-pc.ctx.Done():
			return
		}
	}
}

func (pc *PeerConnection) clearPending() []PieceWork {
	pc.pendingMu.Lock()
	defer pc.pendingMu.Unlock()
	out := make([]PieceWork, 0, len(pc.pending))
	for work := range pc.pending {
		out = append(out, work)
	}
	clear(pc.pending)
	return out
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

// SetInterested tells the remote whether it has pieces we want. Interested
// or NotInterested is sent only when the state changes.
func (pc *PeerConnection) SetInterested(interested bool) {
	if !pc.amInterested.CompareAndSwap(!interested, interested) {
		return
	}
	id := MsgNotInterested
	if interested {
		id = MsgInterested
	}
	select {
	case pc.outgoing <- &Message{ID: id}:
	case <-pc.ctx.Done():
	}
}

// AmInterested reports whether we last told the remote we are interested.
func (pc *PeerConnection) AmInterested() bool { return pc.amInterested.Load() }

// writer is the only goroutine that writes to the socket, so frames are never
// interleaved. It never blocks on peer state: a choke only affects which
// requests the worker queues, never whether Have, Unchoke or piece data can
// be sent.
func (pc *PeerConnection) writer(first *Message) {
	defer pc.Close()
	if first != nil {
		if err := pc.writeMessage(first); err != nil {
			return
		}
	}
	for {
		select {
		case req := <-pc.pieceRequests:
			// Upload: remote peer requested a block from us.
			data, ok := pc.pieceData(req)
			if !ok || len(data) == 0 {
				continue
			}
			msg := BuildPiece(uint32(req.Index), uint32(req.Begin), data)
			if err := pc.writeMessage(msg); err != nil {
				return
			}

		case msg := <-pc.outgoing:
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
	pc.signalStateChange()
}

func (pc *PeerConnection) signalStateChange() {
	select {
	case pc.stateChanged <- struct{}{}:
	default:
	}
}

// StateChanged is signalled when the remote chokes or unchokes us or
// announces new pieces. It has a buffer of one, so a burst of changes
// produces a single wake-up; callers re-read the state after waking.
func (pc *PeerConnection) StateChanged() <-chan struct{} { return pc.stateChanged }

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

// Incoming returns the Piece messages that answer this connection's pending
// requests. Each delivered block was requested and has the requested length.
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
