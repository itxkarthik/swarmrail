package tui

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/itxkarthik/swarmrail/internal/piece"
	"github.com/itxkarthik/swarmrail/internal/session"
	"github.com/itxkarthik/swarmrail/internal/torrent"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

func TestUpdateDownloadRateUsesRollingTransferBytes(t *testing.T) {
	now := time.Unix(100, 0)
	m := Model{
		transferSamples: []transferSample{
			{at: now.Add(-4 * time.Second), bytes: 0},
			{at: now.Add(-3 * time.Second), bytes: 1_000_000},
			{at: now, bytes: 4_000_000},
		},
	}

	m.updateDownloadRate(now, 3*time.Second)

	if diff := math.Abs(m.downloadRate - 1_000_000); diff > 0.01 {
		t.Fatalf("downloadRate = %.2f, want 1000000 bytes/sec", m.downloadRate)
	}
	if got, want := len(m.transferSamples), 2; got != want {
		t.Fatalf("retained samples = %d, want %d", got, want)
	}
}

func TestResponsiveViewFitsTerminalAndShowsWebseed(t *testing.T) {
	tf := &torrent.TorrentFile{
		Name: "responsive-download", Length: 100, PieceLength: 100,
		PieceHashes: make([][20]byte, 1), URLList: []string{"https://seed.example/files/"},
	}
	manager := piece.NewManager(tf, nil)
	manager.SetStatus("Downloading with webseed")
	for _, size := range []struct{ width, height int }{{120, 30}, {80, 24}, {55, 16}} {
		model := NewModel(manager, make(chan error), nil, nil)
		updated, _ := model.Update(tea.WindowSizeMsg{Width: size.width, Height: size.height})
		view := updated.(Model).View().Content
		if !strings.Contains(view, "Sources") || !strings.Contains(view, "webseed") {
			t.Fatalf("%dx%d view does not show webseed Sources:\n%s", size.width, size.height, view)
		}
		for _, line := range strings.Split(view, "\n") {
			if got := lipgloss.Width(line); got > size.width {
				t.Fatalf("%dx%d line width = %d:\n%s", size.width, size.height, got, line)
			}
		}
	}
}

func TestReannounceKeyQueuesManualRequest(t *testing.T) {
	tf := &torrent.TorrentFile{Name: "test", Length: 1, PieceLength: 1, PieceHashes: make([][20]byte, 1)}
	controller := session.NewReannounceController(0)
	model := NewModel(piece.NewManager(tf, nil), make(chan error), controller, nil)
	_, _ = model.Update(tea.KeyPressMsg{Code: 'r'})
	select {
	case <-controller.Requests():
	default:
		t.Fatal("r key did not queue manual reannounce")
	}
}

func TestStatsRowUsesSeparatedColonLabels(t *testing.T) {
	tf := &torrent.TorrentFile{Name: "stats", Length: 100, PieceLength: 100, PieceHashes: make([][20]byte, 1)}
	model := NewModel(piece.NewManager(tf, nil), nil, nil, nil)
	row := model.renderStats(false, 94)
	for _, want := range []string{"Pieces:", "Sources:", "Speed:", "Size:", "ETA:", "│"} {
		if !strings.Contains(row, want) {
			t.Fatalf("stats row missing %q: %s", want, row)
		}
	}
	if got := lipgloss.Width(row); got > 94 {
		t.Fatalf("stats row width = %d, want <= 94", got)
	}
}

func TestUpdateDownloadRateFallsToZeroWithoutRecentTraffic(t *testing.T) {
	now := time.Unix(100, 0)
	m := Model{
		transferSamples: []transferSample{
			{at: now.Add(-4 * time.Second), bytes: 500},
			{at: now, bytes: 500},
		},
	}

	m.updateDownloadRate(now, 3*time.Second)

	if m.downloadRate != 0 {
		t.Fatalf("downloadRate = %.2f, want 0", m.downloadRate)
	}
}
