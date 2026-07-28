package piece

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

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

	mu          sync.Mutex
	inProgress  map[uint32]bool
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
		totalPieces:  len(torrent.PieceHashes),
		pieceLength:  uint64(torrent.PieceLength),
		totalLength:  uint64(torrent.Length),
		pieceHashes:  torrent.PieceHashes,
		peers:        peers,
		webseeds:     webseed.NewSources(torrent),
		outputName:   torrent.Name,
		files:        torrent.Files,
		completed:    make([]bool, len(torrent.PieceHashes)),
		pieceResults: make(chan PieceResult, 32),
		inProgress:   make(map[uint32]bool),
		logWriter:    os.Stderr,
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

func (m *Manager) initPeerUpload(p *peer.PeerConnection) {
	p.ConfigureTorrent(m.totalPieces, int64(m.pieceLength), int64(m.totalLength))
	p.SetPieceDataHandler(m.readUploadBlock)

	bitfieldBytes := peer.BuildBitfieldBytes(m.Completed())
	// Send our bitfield so the peer knows what we have.
	if len(bitfieldBytes) > 0 {
		p.SendBitfield(bitfieldBytes)
	}
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
	n, err := pw.ReadPiece(uint32(work.Index), data)
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

// worker selects pieces for its peer via nextPiece, then downloads all
// blocks of each piece. It requests all blocks of a piece at once
// (pipelining), collects the responses, verifies the SHA1 hash, and
// sends the verified piece to pieceResults. On corruption it loops
// back to re-select the piece; on disconnection it exits so the peer's
// in-progress pieces become available to other workers.
func (m *Manager) worker(ctx context.Context, p *peer.PeerConnection) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		idx := m.nextPiece(p)
		if idx == -1 {
			select {
			case <-ctx.Done():
				return
			case <-p.Done():
				return
			case <-p.AvailabilityChanged():
				continue
			}
		}

		pieceSize := m.sizeOfPiece(idx)
		numBlocks := int((pieceSize + blockSize - 1) / blockSize)
		pieceData := make([]byte, pieceSize)

		m.markInProgress(uint32(idx))

		// Pipeline: send all block requests for this piece to the peer.
		ok := m.sendBlockRequests(p, idx, numBlocks, pieceSize)
		if !ok {
			m.markNotInProgress(uint32(idx))
			return // peer choked or disconnected
		}

		// Collect all block responses.
		ok = m.collectBlocks(p, idx, pieceData, numBlocks)
		if !ok {
			m.markNotInProgress(uint32(idx))
			return // peer disconnected mid-piece
		}

		// Verify SHA1 hash.
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

// sendBlockRequests sends all block requests for the given piece to the peer.
// Returns false if the peer can't accept any request (choked/disconnected).
func (m *Manager) sendBlockRequests(p *peer.PeerConnection, idx, numBlocks int, pieceSize uint64) bool {
	for b := range numBlocks {
		begin := int64(b) * blockSize
		length := int64(blockSize)
		if begin+length > int64(pieceSize) {
			length = int64(pieceSize) - begin
		}

		ok := p.AssignWork(peer.PieceWork{
			Index:  int(idx),
			Begin:  int(begin),
			Length: int(length),
		})
		if !ok {
			return false
		}
	}
	return true
}

// collectBlocks reads responses from the peer until all blocks of the piece
// are received. Returns false if the peer disconnects before completion.
// In endgame mode, another peer may complete the same piece while we're
// collecting — if m.completed[idx] becomes true, we exit early.
func (m *Manager) collectBlocks(p *peer.PeerConnection, idx int, pieceData []byte, numBlocks int) bool {
	received := make(map[uint32]bool, numBlocks)
	for remaining := numBlocks; remaining > 0; {
		if m.isCompleted(uint32(idx)) {
			return false
		}

		msg := m.waitForPieceMessage(p)
		if msg == nil {
			return false
		}

		if len(msg.Payload) < 8 {
			return false
		}
		msgIndex := binary.BigEndian.Uint32(msg.Payload[0:4])
		if msgIndex != uint32(idx) {
			return false
		}
		begin := binary.BigEndian.Uint32(msg.Payload[4:8])
		blockData := msg.Payload[8:]
		if int(begin) >= len(pieceData) || int(begin)+len(blockData) > len(pieceData) {
			return false
		}
		if begin%blockSize != 0 {
			return false
		}
		if received[begin] {
			continue
		}
		expected := blockSize
		if int(begin)+expected > len(pieceData) {
			expected = len(pieceData) - int(begin)
		}
		if len(blockData) != expected {
			return false
		}
		copy(pieceData[begin:], blockData)
		received[begin] = true
		m.transferred.Add(uint64(len(blockData)))
		remaining--
	}
	return true
}

// waitForPieceMessage reads from the peer's Incoming channel until a MsgPiece
// is received, tracking Have messages along the way. Returns nil if the peer
// disconnects before delivering the piece.
func (m *Manager) waitForPieceMessage(p *peer.PeerConnection) *peer.Message {
	for {
		select {
		case msg, ok := <-p.Incoming():
			if !ok {
				return nil
			}
			if msg.ID == peer.MsgPiece {
				return msg
			}
			// Track have messages to keep bitfield current.
			if msg.ID == peer.MsgHave && len(msg.Payload) == 4 {
				idx := binary.BigEndian.Uint32(msg.Payload)
				p.MarkPieceAvailable(idx)
			}
		case <-p.Done():
			return nil
		}
	}
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
}

func (m *Manager) markCompleted(index uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completed[index] = true
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
