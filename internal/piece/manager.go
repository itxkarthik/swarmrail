package piece

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/itxkarthik/swarmrail/internal/peer"
	"github.com/itxkarthik/swarmrail/internal/torrent"
	"github.com/itxkarthik/swarmrail/internal/webseed"
)

const blockSize = 16 * 1024 // 16KB

// PieceWork represents a single block to request from a peer.
type PieceWork struct {
	Index  uint32 // piece index
	Begin  uint32 // offset within the piece
	Length uint32 // block size
}

// PieceResult is the outcome of downloading a complete piece.
type PieceResult struct {
	Index uint32
	Data  []byte
	Err   error
}

type SourceKind string

const (
	SourcePeer    SourceKind = "peer"
	SourceWebSeed SourceKind = "webseed"
)

type SourceSnapshot struct {
	Kind      SourceKind
	Endpoint  string
	State     string
	Direction string
}

// Manager orchestrates piece downloading across multiple peers.
type Manager struct {
	totalPieces  int
	pieceLength  uint64
	totalLength  uint64
	pieceHashes  [][20]byte
	peers        []*peer.PeerConnection
	webseeds     []webseed.Source
	completed    []bool
	pieceResults chan PieceResult    // workers -> manager collector
	outputName   string              // torrent name for output files
	files        []torrent.FileEntry // multi-file entries (nil for single-file)

	requestTimeout time.Duration // snub timeout for outstanding block requests

	mu         sync.Mutex
	inProgress map[uint32]bool
	// pieceEvents is closed and replaced whenever a piece completes or is
	// released, waking every worker that might now pick or abandon a piece.
	// Guarded by mu; read it through pieceEventsChan.
	pieceEvents chan struct{}
	transferred atomic.Uint64 // accepted peer/webseed payload bytes, including retries

	// Resume support
	savePath string   // path to .gtstate file (empty = no periodic saves)
	infoHash [20]byte // for matching saved state

	// Upload support
	pw          *PieceWriter // set during Download for serving uploaded pieces
	downloadCtx context.Context
	downloading bool

	// Status output
	logWriter io.Writer // status messages (stderr by default, silenced in TUI mode)
	status    string    // high-level runtime status for the TUI
}

// NewManager creates a Manager ready to start downloading.
func NewManager(torrent *torrent.TorrentFile, peers []*peer.PeerConnection) *Manager {
	return &Manager{
		totalPieces:    len(torrent.PieceHashes),
		pieceLength:    uint64(torrent.PieceLength),
		totalLength:    uint64(torrent.Length),
		pieceHashes:    torrent.PieceHashes,
		peers:          peers,
		webseeds:       webseed.NewSources(torrent),
		outputName:     torrent.Name,
		files:          torrent.Files,
		completed:      make([]bool, len(torrent.PieceHashes)),
		pieceResults:   make(chan PieceResult, 32),
		inProgress:     make(map[uint32]bool),
		requestTimeout: defaultRequestTimeout,
		logWriter:      os.Stderr,
	}
}

// Download runs the full download pipeline:
//  1. create the output file(s) and serve uploaded pieces from them
//  2. launch one worker goroutine per connected peer
//  3. each worker independently selects pieces via RarestPiece/FirstPiece
//  4. workers request blocks, assemble pieces, verify SHA1 hashes
//  5. collector writes verified pieces to the output file(s)
//
// When resume is true, the file writer preserves existing data and the
// output is pre-allocated so WriteAt works at any offset. Already-completed
// pieces (from m.completed) are skipped.
//
// If EnablePeriodicSave was called before Download, the manager saves
// progress periodically during the download.
//
// Blocks until all pieces are downloaded or an error occurs.
func (m *Manager) Download(parent context.Context, outputPath string, resume bool) error {
	if parent == nil {
		parent = context.Background()
	}
	defer func() {
		if m.savePath != "" && !m.allCompleted() {
			if err := SaveResume(m.savePath, m.infoHash, m.Completed()); err != nil && m.logWriter != nil {
				fmt.Fprintf(m.logWriter, "\nWarning: failed to save resume state: %v\n", err)
			}
		}
	}()
	if len(m.peers) == 0 {
		if len(m.webseeds) > 0 {
			fmt.Fprintf(m.logWriter, "No peers available; using %d webseed(s)\n", len(m.webseeds))
		} else {
			m.SetStatus("Waiting for inbound peers...")
		}
	}

	// If all pieces already completed, nothing to do
	if m.allCompleted() {
		fmt.Fprintf(m.logWriter, "All %d pieces already completed\n", m.totalPieces)
		return nil
	}

	// Create the piece writer for the output file(s)
	tf := &torrent.TorrentFile{
		PieceLength: int64(m.pieceLength),
		Length:      int64(m.totalLength),
		Name:        m.outputName,
		Files:       m.files,
	}
	pw, err := NewPieceWriter(outputPath, tf, resume)
	if err != nil {
		return fmt.Errorf("creating piece writer: %w", err)
	}
	ctx, cancel := context.WithCancel(parent)
	m.mu.Lock()
	m.pw = pw
	m.downloadCtx = ctx
	m.downloading = true
	initialPeers := append([]*peer.PeerConnection(nil), m.peers...)
	m.mu.Unlock()
	defer func() {
		cancel()
		m.mu.Lock()
		m.downloading = false
		m.downloadCtx = nil
		m.pw = nil
		m.mu.Unlock()
		pw.Close()
	}()

	// Keep the result channel open so peers discovered by periodic tracker
	// announces can join the active download and submit verified pieces.
	for _, p := range initialPeers {
		m.initPeerUpload(p)
		go m.worker(ctx, p)
	}
	for _, source := range m.webseeds {
		go m.webseedWorker(ctx, source)
	}

	// Collector: read piece results, write to file, and track progress
	completedPieces := CountCompleted(m.Completed())
	lastSaveCount := completedPieces

	for completedPieces < m.totalPieces {
		var result PieceResult
		select {
		case result = <-m.pieceResults:
		case <-ctx.Done():
			return ctx.Err()
		}
		if result.Err != nil {
			return result.Err
		}
		if result.Index >= uint32(len(m.completed)) || m.isCompleted(result.Index) {
			continue
		}

		if err := pw.WritePiece(result.Index, result.Data); err != nil {
			return fmt.Errorf("writing piece %d: %w", result.Index, err)
		}

		m.markCompleted(result.Index)
		completedPieces++

		// Announce the new piece to all connected peers so they can request it.
		m.announceHave(result.Index)

		fmt.Fprintf(m.logWriter, "\rProgress %d/%d pieces (%.1f%%)  ",
			completedPieces, m.totalPieces,
			float64(completedPieces)/float64(m.totalPieces)*100)

		// Periodic state save
		if m.savePath != "" && completedPieces-lastSaveCount >= saveInterval {
			if err := SaveResume(m.savePath, m.infoHash, m.Completed()); err != nil {
				fmt.Fprintf(m.logWriter, "\nWarning: failed to save resume state: %v\n", err)
			}
			lastSaveCount = completedPieces
		}
	}

	// Final newline so the shell prompt doesn't overwrite the progress line
	if m.logWriter != nil {
		fmt.Fprintln(m.logWriter)
	}

	return nil
}

// initPeerUpload prepares a connection that has just joined m.peers. It must
// run after the peer is in m.peers: pieces completed from then on reach it
// through announceHave, and pieces completed earlier, after its bitfield was
// captured at connect time, are announced here. Overlap only produces a
// harmless duplicate Have.
func (m *Manager) initPeerUpload(p *peer.PeerConnection) {
	p.ConfigureTorrent(m.totalPieces, int64(m.pieceLength), int64(m.totalLength))
	p.SetPieceDataHandler(m.readUploadBlock)
	for i, done := range m.Completed() {
		if done && !p.AdvertisedHas(i) {
			p.SendHave(uint32(i))
		}
	}
}

// BitfieldMessage returns our bitfield as the first message to send on a new
// connection, or nil when we have no verified pieces (the bitfield is then
// optional under BEP 3).
func (m *Manager) BitfieldMessage() *peer.Message {
	completed := m.Completed()
	if CountCompleted(completed) == 0 {
		return nil
	}
	return &peer.Message{ID: peer.MsgBitfield, Payload: peer.BuildBitfieldBytes(completed)}
}

// needsFrom reports whether p has at least one piece we have not verified,
// which is when BEP 3 says we should be interested in it.
func (m *Manager) needsFrom(p *peer.PeerConnection) bool {
	bf := p.Bitfield()
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, has := range bf {
		if has && i < len(m.completed) && !m.completed[i] {
			return true
		}
	}
	return false
}

func (m *Manager) readUploadBlock(work peer.PieceWork) ([]byte, bool) {
	if !peer.ValidateRequest(work, m.totalPieces, int64(m.pieceLength), int64(m.totalLength)) {
		return nil, false
	}
	m.mu.Lock()
	completed := m.completed[work.Index]
	pw := m.pw
	m.mu.Unlock()
	if !completed || pw == nil {
		return nil, false
	}
	data := make([]byte, work.Length)
	n, err := pw.ReadBlock(uint32(work.Index), uint32(work.Begin), data)
	return data, err == nil && n == len(data)
}

// announceHave sends a Have message to all connected peers when we complete a piece.
func (m *Manager) announceHave(index uint32) {
	for _, p := range m.Peers() {
		p.SendHave(index)
	}
}

// nextPiece selects the best eligible piece for the given peer using a
// two-tier strategy:
//  1. RarestPiece
//  2. FirstPiece
//
// Returns -1 if no eligible piece exists for this peer.
func (m *Manager) nextPiece(p *peer.PeerConnection) int {
	idx := m.RarestPiece(p)
	if idx != -1 {
		return idx
	}
	return m.FirstPiece(p)
}

// worker downloads pieces from one peer until the download ends or the peer
// disconnects. Choke, snub and duplicate completion are normal events: the
// worker releases its piece and picks again instead of exiting, so a peer that
// unchokes late or re-chokes during rotation keeps contributing.
func (m *Manager) worker(ctx context.Context, p *peer.PeerConnection) {
	defer p.CancelPending()
	for {
		if ctx.Err() != nil {
			return
		}
		events := m.pieceEventsChan() // before reading state, so no event is lost
		want := m.needsFrom(p)
		p.SetInterested(want)
		if !want || p.PeerChoking() {
			if !waitForChange(ctx, p, events) {
				return
			}
			continue
		}
		idx := m.nextPiece(p)
		if idx == -1 {
			if !waitForChange(ctx, p, events) {
				return
			}
			continue
		}

		m.markInProgress(uint32(idx))
		pieceData := make([]byte, m.sizeOfPiece(idx))
		err := m.downloadPiece(ctx, p, idx, pieceData)
		switch {
		case err == nil:
		case errors.Is(err, errChoked), errors.Is(err, errSnubbed), errors.Is(err, errPieceDone):
			p.CancelPending()
			m.markNotInProgress(uint32(idx))
			continue
		default: // peer closed or download cancelled
			m.markNotInProgress(uint32(idx))
			return
		}

		if !m.verifyPiece(uint32(idx), pieceData) {
			m.markNotInProgress(uint32(idx))
			continue
		}

		select {
		case m.pieceResults <- PieceResult{Index: uint32(idx), Data: pieceData}:
		case <-ctx.Done():
			m.markNotInProgress(uint32(idx))
			return
		}
	}
}

// waitForChange blocks until the peer's choke state or availability changes,
// or a piece completes or is released elsewhere (which can change what we
// need from this peer).
// changes. It returns false when the peer or the download is gone.
func waitForChange(ctx context.Context, p *peer.PeerConnection, events <-chan struct{}) bool {
	select {
	case <-p.StateChanged():
		return true
	case <-events:
		return true
	case <-p.Done():
		return false
	case <-ctx.Done():
		return false
	}
}

var (
	errChoked     = errors.New("peer choked us")
	errSnubbed    = errors.New("peer sent no requested data in time")
	errPieceDone  = errors.New("piece completed by another source")
	errPeerClosed = errors.New("peer connection closed")
)

// defaultRequestTimeout is how long a worker waits for any requested block
// before treating the peer as snubbing it and releasing the piece.
const defaultRequestTimeout = 60 * time.Second

// downloadPiece fills pieceData with piece idx from p, keeping up to
// peer.MaxPendingRequests block requests in flight.
func (m *Manager) downloadPiece(ctx context.Context, p *peer.PeerConnection, idx int, pieceData []byte) error {
	// Discard blocks left over from an abandoned piece before counting
	// replies for this one.
	for drained := false; !drained; {
		select {
		case <-p.Incoming():
		default:
			drained = true
		}
	}

	// Any choke after this point discards requests sent for this piece, even
	// if an unchoke follows before this worker wakes up.
	chokes := p.ChokeCount()

	numBlocks := (len(pieceData) + blockSize - 1) / blockSize
	requested := make([]bool, numBlocks)
	received := make([]bool, numBlocks)
	next, outstanding, remaining := 0, 0, numBlocks

	timer := time.NewTimer(m.requestTimeout)
	defer timer.Stop()

	// Another peer or a webseed may complete this piece while this peer is
	// silent; the event wakes us so the duplicate requests are cancelled
	// instead of waiting out the snub timeout.
	events := m.pieceEventsChan()
	if m.isCompleted(uint32(idx)) {
		return errPieceDone
	}

	for remaining > 0 {
		for ; next < numBlocks && outstanding < peer.MaxPendingRequests; next++ {
			if received[next] {
				continue
			}
			begin := next * blockSize
			length := min(blockSize, len(pieceData)-begin)
			if p.ChokeCount() != chokes {
				return errChoked
			}
			if !p.Request(peer.PieceWork{Index: idx, Begin: begin, Length: length}) {
				if p.PeerChoking() {
					return errChoked
				}
				if !p.Active() {
					return errPeerClosed
				}
				break // pending cap reached; wait for replies
			}
			requested[next] = true
			outstanding++
		}

		select {
		case msg := <-p.Incoming():
			if len(msg.Payload) < 8 || binary.BigEndian.Uint32(msg.Payload[0:4]) != uint32(idx) {
				continue // reply for a piece this worker gave up on
			}
			begin := int(binary.BigEndian.Uint32(msg.Payload[4:8]))
			block := msg.Payload[8:]
			b := begin / blockSize
			if begin%blockSize != 0 || b >= numBlocks || received[b] ||
				len(block) != min(blockSize, len(pieceData)-begin) {
				continue
			}
			copy(pieceData[begin:], block)
			received[b] = true
			remaining--
			if requested[b] {
				outstanding--
			}
			m.transferred.Add(uint64(len(block)))
			timer.Reset(m.requestTimeout) // Go 1.23+ timers need no drain before Reset
			if remaining > 0 && m.isCompleted(uint32(idx)) {
				return errPieceDone
			}
		case <-p.StateChanged():
			if p.PeerChoking() || p.ChokeCount() != chokes {
				return errChoked
			}
			if m.isCompleted(uint32(idx)) {
				return errPieceDone
			}
		case <-events:
			events = m.pieceEventsChan()
			if m.isCompleted(uint32(idx)) {
				return errPieceDone
			}
		case <-timer.C:
			return errSnubbed
		case <-p.Done():
			return errPeerClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (m *Manager) webseedWorker(ctx context.Context, source webseed.Source) {
	for {
		idx := m.nextWebSeedPiece()
		if idx == -1 {
			return
		}

		pieceSize := m.sizeOfPiece(idx)
		data, err := source.FetchPiece(ctx, uint32(idx), int64(pieceSize))
		if err != nil {
			m.markNotInProgress(uint32(idx))
			return
		}
		m.transferred.Add(uint64(len(data)))
		if !m.verifyPiece(uint32(idx), data) {
			source.Disable()
			m.markNotInProgress(uint32(idx))
			return
		}

		select {
		case m.pieceResults <- PieceResult{Index: uint32(idx), Data: data}:
		case <-ctx.Done():
			m.markNotInProgress(uint32(idx))
			return
		}
	}
}

func (m *Manager) sizeOfPiece(idx int) uint64 {
	pieceSize := m.pieceLength
	if idx == m.totalPieces-1 {
		pieceSize = m.totalLength - uint64(idx)*m.pieceLength
	}
	return pieceSize
}

// nextWebSeedPiece chooses the first piece in the largest contiguous missing
// run. This keeps HTTP webseed reads more sequential than peer rarest-first.
func (m *Manager) nextWebSeedPiece() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	bestStart, bestLen := -1, 0
	for i := 0; i < m.totalPieces; {
		for i < m.totalPieces && (m.completed[i] || m.inProgress[uint32(i)]) {
			i++
		}
		start := i
		for i < m.totalPieces && !m.completed[i] && !m.inProgress[uint32(i)] {
			i++
		}
		if runLen := i - start; runLen > bestLen {
			bestStart, bestLen = start, runLen
		}
	}
	if bestStart == -1 {
		return -1
	}
	m.inProgress[uint32(bestStart)] = true
	return bestStart
}

// verifyPiece checks the assembled piece data against the expected SHA1 hash.
func (m *Manager) verifyPiece(index uint32, data []byte) bool {
	hash := sha1.Sum(data)
	return bytes.Equal(hash[:], m.pieceHashes[index][:])
}

// Helpers

func (m *Manager) isInProgress(index uint32) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.inProgress[index]
	return ok
}

func (m *Manager) markInProgress(index uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inProgress[index] = true
}

func (m *Manager) markNotInProgress(index uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inProgress, index)
	m.notifyPieceEventLocked()
}

func (m *Manager) markCompleted(index uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completed[index] = true
	m.notifyPieceEventLocked()
}

// pieceEventsChan returns a channel that is closed at the next piece
// completion or release. Take it before checking manager state so an event
// between the check and the wait is not missed.
func (m *Manager) pieceEventsChan() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pieceEvents == nil {
		m.pieceEvents = make(chan struct{})
	}
	return m.pieceEvents
}

// notifyPieceEventLocked wakes every waiter on pieceEventsChan. m.mu must be
// held.
func (m *Manager) notifyPieceEventLocked() {
	if m.pieceEvents != nil {
		close(m.pieceEvents)
	}
	m.pieceEvents = make(chan struct{})
}

func (m *Manager) isCompleted(index uint32) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int(index) < len(m.completed) && m.completed[index]
}

// allCompleted returns true when every piece is marked done.
func (m *Manager) allCompleted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.completed {
		if !c {
			return false
		}
	}
	return true
}

// SetCompleted overrides the completed bitfield with a saved state.
func (m *Manager) SetCompleted(completed []bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(completed) == len(m.completed) {
		copy(m.completed, completed)
		m.notifyPieceEventLocked()
	}
}

// SetLogWriter sets the writer for status/progress messages.
// Pass io.Discard to silence all progress output (used in TUI mode).
func (m *Manager) SetLogWriter(w io.Writer) {
	m.logWriter = w
}

// SetStatus updates the high-level runtime status shown in the TUI.
func (m *Manager) SetStatus(status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.status = status
}

// Status returns the latest runtime status string.
func (m *Manager) Status() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.status
}

// SetPeers replaces the active peer list.
func (m *Manager) SetPeers(peers []*peer.PeerConnection) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peers = peers
}

// AddPeers appends newly discovered connections and starts workers immediately
// when a download is active. It returns the number of accepted connections.
func (m *Manager) AddPeers(peers []*peer.PeerConnection) int {
	m.mu.Lock()
	seenIDs := make(map[[20]byte]struct{}, len(m.peers)+len(peers))
	seenEndpoints := make(map[string]struct{}, len(m.peers)+len(peers))
	for _, existing := range m.peers {
		if existing != nil {
			if existing.PeerID() != ([20]byte{}) {
				seenIDs[existing.PeerID()] = struct{}{}
			}
			seenEndpoints[existing.RemoteAddr()] = struct{}{}
		}
	}
	valid := make([]*peer.PeerConnection, 0, len(peers))
	for _, candidate := range peers {
		if candidate == nil || !candidate.Active() {
			continue
		}
		id := candidate.PeerID()
		if id != ([20]byte{}) {
			if _, duplicate := seenIDs[id]; duplicate {
				candidate.Close()
				continue
			}
			seenIDs[id] = struct{}{}
		}
		endpoint := candidate.RemoteAddr()
		if _, duplicate := seenEndpoints[endpoint]; duplicate {
			candidate.Close()
			continue
		}
		seenEndpoints[endpoint] = struct{}{}
		valid = append(valid, candidate)
	}
	if len(valid) == 0 {
		m.mu.Unlock()
		return 0
	}
	m.peers = append(m.peers, valid...)
	ctx := m.downloadCtx
	active := m.downloading && ctx != nil
	m.mu.Unlock()

	if active {
		for _, p := range valid {
			m.initPeerUpload(p)
			go m.worker(ctx, p)
		}
	}
	return len(valid)
}

// Completed returns a copy of the completed bitfield for external save.
func (m *Manager) Completed() []bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]bool, len(m.completed))
	copy(out, m.completed)
	return out
}

// EnablePeriodicSave sets the manager up to save progress to disk
// every saveInterval completed pieces during Download.
func (m *Manager) EnablePeriodicSave(savePath string, infoHash [20]byte) {
	m.savePath = savePath
	m.infoHash = infoHash
}

// TorrentName returns the torrent's display name.
func (m *Manager) TorrentName() string { return m.outputName }

// TotalPieces returns the total number of pieces in the torrent.
func (m *Manager) TotalPieces() int { return m.totalPieces }

// TotalLength returns the total size of the torrent in bytes.
func (m *Manager) TotalLength() uint64 { return m.totalLength }

// WebSeedCount returns the number of configured HTTP sources.
func (m *Manager) WebSeedCount() int { return len(m.webseeds) }

// SourceSnapshots returns a stable, thread-safe view of every active peer and
// configured webseed for terminal and diagnostic interfaces.
func (m *Manager) SourceSnapshots() []SourceSnapshot {
	m.mu.Lock()
	peers := append([]*peer.PeerConnection(nil), m.peers...)
	webseeds := append([]webseed.Source(nil), m.webseeds...)
	m.mu.Unlock()

	out := make([]SourceSnapshot, 0, len(peers)+len(webseeds))
	for _, connection := range peers {
		if connection == nil {
			continue
		}
		state := "active"
		if !connection.Active() {
			state = "closed"
		}
		out = append(out, SourceSnapshot{Kind: SourcePeer, Endpoint: connection.RemoteAddr(), State: state, Direction: string(connection.Direction())})
	}
	for _, source := range webseeds {
		snapshot := source.Snapshot()
		out = append(out, SourceSnapshot{Kind: SourceWebSeed, Endpoint: snapshot.Name, State: string(snapshot.State)})
	}
	return out
}

// CompletedBytes returns the exact number of verified bytes represented by the
// completed bitfield. The final piece may be shorter than pieceLength.
func (m *Manager) CompletedBytes() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	var total uint64
	for i, complete := range m.completed {
		if !complete {
			continue
		}
		size := m.pieceLength
		if i == m.totalPieces-1 {
			size = m.totalLength - uint64(i)*m.pieceLength
		}
		total += size
	}
	return total
}

// TransferredBytes returns accepted download payload bytes. Unlike
// CompletedBytes, it includes data that is later rejected or downloaded again.
func (m *Manager) TransferredBytes() uint64 { return m.transferred.Load() }

// UploadedBytes returns payload bytes successfully written to remote peers.
func (m *Manager) UploadedBytes() uint64 {
	var total uint64
	for _, connection := range m.Peers() {
		if connection != nil {
			total += connection.UploadedBytes()
		}
	}
	return total
}

// Peers returns the list of connected peer connections.
func (m *Manager) Peers() []*peer.PeerConnection {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*peer.PeerConnection, len(m.peers))
	copy(out, m.peers)
	return out
}
