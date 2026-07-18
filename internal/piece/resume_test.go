package piece

import (
	"crypto/sha1"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/itxkarthik/GoTorrent/internal/torrent"
)

func TestStateFilePathIncludesShortInfoHash(t *testing.T) {
	var infoHash [20]byte
	copy(infoHash[:], []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab})

	got := StateFilePath("downloads", "example", infoHash)
	want := filepath.Join("downloads", "example.0123456789ab.gtstate")
	if got != want {
		t.Fatalf("StateFilePath() = %q, want %q", got, want)
	}
}

func TestLoadVerifiedResumeResetsMissingSingleFilePieces(t *testing.T) {
	piece0 := []byte("abcd")
	piece1 := []byte("efgh")
	tf := resumeTestTorrent("payload.bin", piece0, piece1)
	dir := t.TempDir()
	statePath := StateFilePath(dir, tf.Name, tf.InfoHash)
	if err := SaveResume(statePath, tf.InfoHash, []bool{true, true}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, tf.Name), piece0, 0o644); err != nil {
		t.Fatal(err)
	}

	state, report, err := LoadVerifiedResume(dir, tf)
	if err != nil {
		t.Fatal(err)
	}
	if !state.Completed[0] || state.Completed[1] {
		t.Fatalf("completed = %v, want [true false]", state.Completed)
	}
	if report.ResetPieces != 1 {
		t.Fatalf("ResetPieces = %d, want 1", report.ResetPieces)
	}
}

func TestLoadVerifiedResumeReadsPiecesAcrossMultipleFiles(t *testing.T) {
	piece0 := []byte("abcd")
	piece1 := []byte("efgh")
	tf := resumeTestTorrent("bundle", piece0, piece1)
	tf.Files = []torrent.FileEntry{
		{Length: 3, Path: []string{"one.bin"}},
		{Length: 5, Path: []string{"two.bin"}},
	}
	dir := t.TempDir()
	base := filepath.Join(dir, tf.Name)
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "one.bin"), []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "two.bin"), []byte("defgh"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := SaveResume(StateFilePath(dir, tf.Name, tf.InfoHash), tf.InfoHash, []bool{true, true}); err != nil {
		t.Fatal(err)
	}

	state, report, err := LoadVerifiedResume(dir, tf)
	if err != nil {
		t.Fatal(err)
	}
	if got := CountCompleted(state.Completed); got != 2 || report.ResetPieces != 0 {
		t.Fatalf("completed=%v reset=%d, want both verified", state.Completed, report.ResetPieces)
	}
}

func TestLoadVerifiedResumeMigratesLegacyState(t *testing.T) {
	piece0 := []byte("abcd")
	tf := resumeTestTorrent("legacy.bin", piece0)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, tf.Name), piece0, 0o644); err != nil {
		t.Fatal(err)
	}
	legacy := LegacyStateFilePath(dir, tf.Name)
	if err := SaveResume(legacy, tf.InfoHash, []bool{true}); err != nil {
		t.Fatal(err)
	}

	state, report, err := LoadVerifiedResume(dir, tf)
	if err != nil {
		t.Fatal(err)
	}
	if !state.Completed[0] || !report.Migrated {
		t.Fatalf("state=%v report=%+v, want migrated verified state", state.Completed, report)
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatalf("legacy state still exists: %v", err)
	}
	if _, err := os.Stat(StateFilePath(dir, tf.Name, tf.InfoHash)); err != nil {
		t.Fatalf("migrated state missing: %v", err)
	}
}

func TestSaveResumeLeavesNoTemporaryFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.gtstate")
	if err := SaveResume(path, [20]byte{1}, []bool{true}); err != nil {
		t.Fatal(err)
	}
	if err := SaveResume(path, [20]byte{1}, []bool{false}); err != nil {
		t.Fatalf("replacing existing state: %v", err)
	}
	state, err := LoadResume(path, [20]byte{1})
	if err != nil || len(state.Completed) != 1 || state.Completed[0] {
		t.Fatalf("replacement state = %+v, err=%v", state, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".tmp-") {
			t.Fatalf("temporary state file left behind: %s", entry.Name())
		}
	}
}

func resumeTestTorrent(name string, pieces ...[]byte) *torrent.TorrentFile {
	hashes := make([][20]byte, len(pieces))
	var total int64
	for i, data := range pieces {
		hashes[i] = sha1.Sum(data)
		total += int64(len(data))
	}
	var infoHash [20]byte
	copy(infoHash[:], []byte(name))
	return &torrent.TorrentFile{
		Name: name, InfoHash: infoHash, PieceLength: int64(len(pieces[0])),
		Length: total, PieceHashes: hashes,
	}
}
