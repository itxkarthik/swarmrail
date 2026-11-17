package piece

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/itxkarthik/swarmrail/internal/torrent"
)

const resumeSuffix = ".gtstate"
const stateHashPrefixLength = 12
const saveInterval = 1

// ResumeState holds the persistence data for download resumption.
type ResumeState struct {
	InfoHash  string `json:"info_hash"`
	Completed []bool `json:"completed"`
}

// ResumeReport describes corrections made while validating persisted state.
type ResumeReport struct {
	ResetPieces int
	Migrated    bool
}

// StateFilePath returns the path to the resume state file for a torrent
// identified by its name, placed alongside the output.
func StateFilePath(outputPath, torrentName string, infoHash [20]byte) string {
	fullHash := fmt.Sprintf("%040x", infoHash)
	return filepath.Join(outputPath, fmt.Sprintf("%s.%s%s", torrentName, fullHash[:stateHashPrefixLength], resumeSuffix))
}

// LegacyStateFilePath returns the state path used before info-hash suffixes.
func LegacyStateFilePath(outputPath, torrentName string) string {
	return filepath.Join(outputPath, torrentName+resumeSuffix)
}

// SaveResume writes the current download state to disk atomically.
func SaveResume(path string, infoHash [20]byte, completed []bool) error {
	state := ResumeState{
		InfoHash:  fmt.Sprintf("%040x", infoHash),
		Completed: completed,
	}
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshalling resume state: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating resume directory: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-")
	if err != nil {
		return fmt.Errorf("creating temporary resume state: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return fmt.Errorf("setting resume permissions: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("writing temporary resume state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("flushing temporary resume state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temporary resume state: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replacing resume state: %w", err)
	}
	return nil
}

// LoadResume reads a saved state from disk and validates it matches
// the given infoHash. Returns nil if the file doesn't exist or
// the infoHash doesn't match (caller should start fresh).
func LoadResume(path string, infoHash [20]byte) (*ResumeState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var state ResumeState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("parsing resume state: %w", err)
	}
	expected := fmt.Sprintf("%040x", infoHash)
	if state.InfoHash != expected {
		return nil, fmt.Errorf("info_hash mismatch: state=%s, torrent=%s", state.InfoHash, expected)
	}
	return &state, nil
}

// LoadVerifiedResume loads the current or legacy state and verifies every
// completed piece against the payload before it can be trusted.
func LoadVerifiedResume(outputPath string, tf *torrent.TorrentFile) (*ResumeState, ResumeReport, error) {
	path := StateFilePath(outputPath, tf.Name, tf.InfoHash)
	state, err := LoadResume(path, tf.InfoHash)
	legacy := false
	if os.IsNotExist(err) {
		legacyPath := LegacyStateFilePath(outputPath, tf.Name)
		state, err = LoadResume(legacyPath, tf.InfoHash)
		legacy = err == nil
	}
	if err != nil {
		return nil, ResumeReport{}, err
	}
	if len(state.Completed) != len(tf.PieceHashes) {
		return nil, ResumeReport{}, fmt.Errorf("resume piece count %d does not match torrent piece count %d", len(state.Completed), len(tf.PieceHashes))
	}

	report := ResumeReport{}
	for index, complete := range state.Completed {
		if !complete {
			continue
		}
		data, readErr := readPayloadPiece(outputPath, tf, index)
		if readErr != nil || sha1.Sum(data) != tf.PieceHashes[index] {
			state.Completed[index] = false
			report.ResetPieces++
		}
	}

	if legacy {
		if err := SaveResume(path, tf.InfoHash, state.Completed); err != nil {
			return nil, report, fmt.Errorf("migrating legacy resume state: %w", err)
		}
		if err := os.Remove(LegacyStateFilePath(outputPath, tf.Name)); err != nil && !os.IsNotExist(err) {
			return nil, report, fmt.Errorf("removing legacy resume state: %w", err)
		}
		report.Migrated = true
	}
	return state, report, nil
}

func readPayloadPiece(outputPath string, tf *torrent.TorrentFile, index int) ([]byte, error) {
	start := int64(index) * tf.PieceLength
	length := min(tf.PieceLength, tf.Length-start)
	if length <= 0 {
		return nil, io.ErrUnexpectedEOF
	}
	data := make([]byte, length)
	if len(tf.Files) == 0 {
		file, err := os.Open(filepath.Join(outputPath, tf.Name))
		if err != nil {
			return nil, err
		}
		defer file.Close()
		n, err := file.ReadAt(data, start)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if int64(n) != length {
			return nil, io.ErrUnexpectedEOF
		}
		return data, nil
	}

	var torrentOffset int64
	written := int64(0)
	for _, entry := range tf.Files {
		fileStart := torrentOffset
		fileEnd := fileStart + entry.Length
		torrentOffset = fileEnd
		overlapStart := max(start, fileStart)
		overlapEnd := min(start+length, fileEnd)
		if overlapStart >= overlapEnd {
			continue
		}
		path := filepath.Join(append([]string{outputPath, tf.Name}, entry.Path...)...)
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		partLength := overlapEnd - overlapStart
		n, readErr := file.ReadAt(data[written:written+partLength], overlapStart-fileStart)
		file.Close()
		if readErr != nil && readErr != io.EOF {
			return nil, readErr
		}
		if int64(n) != partLength {
			return nil, io.ErrUnexpectedEOF
		}
		written += partLength
	}
	if written != length {
		return nil, io.ErrUnexpectedEOF
	}
	return data, nil
}

// RemoveResume deletes the state file after a successful download.
func RemoveResume(path string) {
	os.Remove(path)
}

// CountCompleted returns the number of true values in the bitfield.
func CountCompleted(completed []bool) int {
	n := 0
	for _, c := range completed {
		if c {
			n++
		}
	}
	return n
}
