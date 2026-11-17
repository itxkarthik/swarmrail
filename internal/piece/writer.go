package piece

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/itxkarthik/swarmrail/internal/torrent"
)

// PieceWriter reads and writes piece data in the output file(s). Single-file
// torrents are treated as a file list with one entry, so both layouts share
// the same offset mapping.
type PieceWriter struct {
	pieceLen int64
	lengths  []int64    // length of each file, in torrent order
	files    []*os.File // open handle for each entry in lengths
}

// NewPieceWriter creates a PieceWriter for the given torrent.
// For single-file torrents, opens one output file at outputPath/<Name>.
// For multi-file torrents, creates the directory tree at outputPath/<Name>/
// and opens all files.
//
// When resume is true, files are opened without truncation (existing data
// is preserved for previously completed pieces) and pre-allocated to the
// torrent's full size so WriteAt works at any offset. When false, files
// are created fresh with O_TRUNC.
func NewPieceWriter(outputPath string, tf *torrent.TorrentFile, resume bool) (*PieceWriter, error) {
	// Always use O_RDWR so the writer can also serve uploaded pieces.
	openFlags := os.O_CREATE | os.O_RDWR | os.O_TRUNC
	if resume {
		openFlags = os.O_CREATE | os.O_RDWR
	}

	type target struct {
		path   string
		length int64
	}
	var targets []target
	if len(tf.Files) == 0 {
		targets = []target{{filepath.Join(outputPath, tf.Name), tf.Length}}
	} else {
		baseDir := filepath.Join(outputPath, tf.Name)
		for _, entry := range tf.Files {
			targets = append(targets, target{filepath.Join(append([]string{baseDir}, entry.Path...)...), entry.Length})
		}
	}

	pw := &PieceWriter{
		pieceLen: tf.PieceLength,
		lengths:  make([]int64, 0, len(targets)),
		files:    make([]*os.File, 0, len(targets)),
	}
	for _, t := range targets {
		dir := filepath.Dir(t.path)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			pw.Close()
			return nil, fmt.Errorf("creating directory %s: %w", dir, err)
		}
		f, err := os.OpenFile(t.path, openFlags, 0o644)
		if err != nil {
			pw.Close()
			return nil, fmt.Errorf("opening file %s: %w", t.path, err)
		}
		if resume {
			// Pre-allocate to the full size so WriteAt works at any offset.
			_ = f.Truncate(t.length)
		}
		pw.files = append(pw.files, f)
		pw.lengths = append(pw.lengths, t.length)
	}
	return pw, nil
}

// WritePiece writes a verified piece at its byte offset in the output.
func (pw *PieceWriter) WritePiece(index uint32, data []byte) error {
	if err := pw.writeAt(data, int64(index)*pw.pieceLen); err != nil {
		return fmt.Errorf("writing piece %d: %w", index, err)
	}
	return nil
}

// ReadPiece reads len(data) bytes from the start of piece index.
func (pw *PieceWriter) ReadPiece(index uint32, data []byte) (int, error) {
	return pw.ReadBlock(index, 0, data)
}

// ReadBlock reads len(data) bytes starting begin bytes into piece index. It
// serves BEP 3 upload requests, which address a block by piece index and an
// offset within that piece.
func (pw *PieceWriter) ReadBlock(index, begin uint32, data []byte) (int, error) {
	return pw.readAt(data, int64(index)*pw.pieceLen+int64(begin))
}

// readAt fills p from the torrent-global offset off. It returns
// io.ErrUnexpectedEOF if the range runs past the end of the payload.
func (pw *PieceWriter) readAt(p []byte, off int64) (int, error) {
	total := 0
	for _, s := range fileSpans(pw.lengths, off, int64(len(p))) {
		n, err := pw.files[s.file].ReadAt(p[s.bufOff:s.bufOff+s.length], s.fileOff)
		total += n
		if int64(n) == s.length {
			continue // a full read may still report io.EOF at the end of a file
		}
		if err == nil || errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return total, err
	}
	if total != len(p) {
		return total, io.ErrUnexpectedEOF
	}
	return total, nil
}

// writeAt writes p at the torrent-global offset off.
func (pw *PieceWriter) writeAt(p []byte, off int64) error {
	written := int64(0)
	for _, s := range fileSpans(pw.lengths, off, int64(len(p))) {
		if _, err := pw.files[s.file].WriteAt(p[s.bufOff:s.bufOff+s.length], s.fileOff); err != nil {
			return fmt.Errorf("file %d: %w", s.file, err)
		}
		written += s.length
	}
	if written != int64(len(p)) {
		return fmt.Errorf("range %d+%d is outside the torrent payload", off, len(p))
	}
	return nil
}

// Close closes all open files and returns the first error.
func (pw *PieceWriter) Close() error {
	var firstErr error
	for _, f := range pw.files {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
