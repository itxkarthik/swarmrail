package tui

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/itxkarthik/swarmrail/internal/piece"
	"github.com/itxkarthik/swarmrail/internal/session"

	"charm.land/bubbles/v2/help"
	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/progress"
	"charm.land/bubbles/v2/spinner"
	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

type TickMsg struct{}
type DownloadDoneMsg struct{ Err error }

type transferSample struct {
	at    time.Time
	bytes uint64
}

type keyMap struct {
	Reannounce key.Binding
	Quit       key.Binding
}

func defaultKeyMap() keyMap {
	return keyMap{
		Reannounce: key.NewBinding(key.WithKeys("r"), key.WithHelp("r", "reannounce")),
		Quit:       key.NewBinding(key.WithKeys("q", "esc", "ctrl+c"), key.WithHelp("q", "quit")),
	}
}

func (k keyMap) ShortHelp() []key.Binding  { return []key.Binding{k.Reannounce, k.Quit} }
func (k keyMap) FullHelp() [][]key.Binding { return [][]key.Binding{{k.Reannounce, k.Quit}} }

type Model struct {
	m                *piece.Manager
	width            int
	height           int
	ready            bool
	percent          float64
	progress         progress.Model
	spinner          spinner.Model
	sources          table.Model
	help             help.Model
	keys             keyMap
	done             bool
	err              error
	doneCh           <-chan error
	reannounce       *session.ReannounceController
	listener         *session.PeerListener
	cancel           context.CancelFunc
	localStatus      string
	localStatusUntil time.Time
	pieceMapWidth    int

	transferSamples []transferSample
	downloadRate    float64
}

func NewModel(m *piece.Manager, doneCh <-chan error, reannounce *session.ReannounceController, cancel context.CancelFunc, listeners ...*session.PeerListener) Model {
	progressModel := progress.New(progress.WithColors(green), progress.WithoutPercentage())
	spinnerModel := spinner.New(spinner.WithSpinner(spinner.MiniDot), spinner.WithStyle(lipgloss.NewStyle().Foreground(indigo)))
	tableStyles := table.DefaultStyles()
	tableStyles.Header = tableStyles.Header.Foreground(indigo).Bold(true)
	tableStyles.Cell = tableStyles.Cell.Foreground(white)
	tableStyles.Selected = tableStyles.Selected.Foreground(green).Bold(true)
	sources := table.New(table.WithFocused(true), table.WithHeight(6), table.WithStyles(tableStyles))
	helpModel := help.New()
	helpModel.Styles = help.DefaultDarkStyles()
	model := Model{
		m: m, doneCh: doneCh, reannounce: reannounce, cancel: cancel,
		progress: progressModel, spinner: spinnerModel, sources: sources,
		help: helpModel, keys: defaultKeyMap(), pieceMapWidth: 60,
		transferSamples: []transferSample{{at: time.Now(), bytes: m.TransferredBytes()}},
	}
	if len(listeners) > 0 {
		model.listener = listeners[0]
	}
	model.refreshSources(60)
	return model
}

func (m Model) Init() tea.Cmd {
	cmds := []tea.Cmd{m.tickCmd(), m.spinner.Tick}
	if m.doneCh != nil {
		cmds = append(cmds, m.waitForDone())
	}
	return tea.Batch(cmds...)
}

func (m Model) tickCmd() tea.Cmd {
	return tea.Tick(250*time.Millisecond, func(time.Time) tea.Msg { return TickMsg{} })
}

func (m Model) waitForDone() tea.Cmd {
	return func() tea.Msg {
		err, ok := <-m.doneCh
		if !ok {
			return nil
		}
		return DownloadDoneMsg{Err: err}
	}
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height, m.ready = msg.Width, msg.Height, true
		m.resizeComponents()
		return m, nil
	case TickMsg:
		if !m.done {
			if !m.localStatusUntil.IsZero() && time.Now().After(m.localStatusUntil) {
				m.localStatus = ""
				m.localStatusUntil = time.Time{}
			}
			m.updateStats()
			m.refreshSources(m.sources.Width())
			cmds = append(cmds, m.tickCmd())
		}
	case DownloadDoneMsg:
		m.done, m.err = true, msg.Err
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		cmds = append(cmds, cmd)
	case tea.KeyPressMsg:
		switch {
		case key.Matches(msg, m.keys.Quit):
			if m.cancel != nil {
				m.cancel()
			}
			return m, tea.Quit
		case key.Matches(msg, m.keys.Reannounce):
			if m.reannounce == nil {
				m.localStatus = "No trackers available for reannounce"
			} else {
				result := m.reannounce.Request()
				if result.Accepted {
					m.localStatus = "Manual reannounce queued"
				} else if result.RetryAfter > 0 {
					m.localStatus = fmt.Sprintf("Reannounce available in %s", result.RetryAfter.Round(time.Second))
				} else {
					m.localStatus = "Reannounce already in progress"
				}
			}
			m.localStatusUntil = time.Now().Add(3 * time.Second)
		}
	}

	var tableCmd tea.Cmd
	m.sources, tableCmd = m.sources.Update(msg)
	cmds = append(cmds, tableCmd)
	return m, tea.Batch(cmds...)
}

func (m *Model) resizeComponents() {
	inner := max(20, m.width-6)
	if m.width < 70 {
		inner = max(20, m.width-2)
	}
	m.progress.SetWidth(max(10, inner-8))
	m.help.SetWidth(inner)
	m.pieceMapWidth = inner
	sourceWidth := inner
	if m.width >= 100 && m.height >= 22 {
		sourceWidth = max(36, inner*45/100)
		m.pieceMapWidth = max(30, inner-sourceWidth-3)
	}
	height := 6
	if m.height < 18 {
		height = 4
	} else if m.width >= 100 {
		height = min(10, max(6, m.height-12))
	}
	m.sources.SetWidth(sourceWidth)
	m.sources.SetHeight(height)
	m.refreshSources(sourceWidth)
}

func (m *Model) refreshSources(width int) {
	if width <= 0 {
		width = 60
	}
	kindWidth, stateWidth := 9, 10
	endpointWidth := max(10, width-kindWidth-stateWidth-6)
	if width < 50 {
		kindWidth, stateWidth = 8, 9
		endpointWidth = max(8, width-kindWidth-stateWidth-6)
	}
	m.sources.SetColumns([]table.Column{{Title: "Type", Width: kindWidth}, {Title: "Endpoint", Width: endpointWidth}, {Title: "State", Width: stateWidth}})
	rows := make([]table.Row, 0)
	for _, source := range m.m.SourceSnapshots() {
		kind := string(source.Kind)
		if source.Direction == "inbound" {
			kind = "peer/in"
		} else if source.Direction == "outbound" {
			kind = "peer/out"
		}
		rows = append(rows, table.Row{kind, source.Endpoint, source.State})
	}
	if len(rows) == 0 {
		rows = append(rows, table.Row{"—", "Waiting for sources", "idle"})
	}
	m.sources.SetRows(rows)
}

func (m *Model) updateStats() {
	now := time.Now()
	m.transferSamples = append(m.transferSamples, transferSample{at: now, bytes: m.m.TransferredBytes()})
	m.updateDownloadRate(now, 3*time.Second)
	if total := m.m.TotalLength(); total > 0 {
		m.percent = float64(m.m.CompletedBytes()) / float64(total)
	}
	m.percent = max(0, min(1, m.percent))
}

func (m *Model) updateDownloadRate(now time.Time, window time.Duration) {
	cutoff := now.Add(-window)
	first := 0
	for first+1 < len(m.transferSamples) && !m.transferSamples[first+1].at.After(cutoff) {
		first++
	}
	m.transferSamples = m.transferSamples[first:]
	oldest, latest := m.transferSamples[0], m.transferSamples[len(m.transferSamples)-1]
	elapsed := latest.at.Sub(oldest.at).Seconds()
	if elapsed <= 0 || latest.bytes < oldest.bytes {
		m.downloadRate = 0
		return
	}
	m.downloadRate = float64(latest.bytes-oldest.bytes) / elapsed
}

func (m Model) View() tea.View {
	if !m.ready {
		return tea.NewView(m.status())
	}
	if m.err != nil {
		return tea.NewView(m.wrap(ErrorStyle.Render(fmt.Sprintf("Download failed: %v\n\nPress q to exit", m.err))))
	}
	if m.done {
		return tea.NewView(m.wrap(lipgloss.NewStyle().Foreground(green).Bold(true).Render("Download complete") + "\n\nPress q to exit"))
	}
	return tea.NewView(m.renderMain())
}

func (m Model) renderMain() string {
	innerWidth := max(20, m.width-6)
	if m.width < 70 {
		innerWidth = max(20, m.width-2)
	}
	header := lipgloss.NewStyle().Foreground(white).Bold(true).MaxWidth(innerWidth).Render("SwarmRail  " + m.m.TorrentName())
	status := lipgloss.NewStyle().Foreground(grayLight).MaxWidth(max(10, innerWidth-3)).Render(m.status())
	progressLine := m.progress.ViewAs(m.percent) + fmt.Sprintf(" %5.1f%%", m.percent*100)
	stats := m.renderStats(m.width < 70, innerWidth)
	sources := SectionStyle.Render("Sources") + "\n" + m.sources.View()
	parts := []string{header, m.spinner.View() + " " + status, progressLine, stats}
	if listenerStatus := m.listenerStatus(); listenerStatus != "" {
		parts = append(parts, listenerStatus)
	}
	if m.width >= 100 && m.height >= 22 {
		piecePanel := lipgloss.NewStyle().Width(m.pieceMapWidth).Render(m.renderPieceMap())
		sourcePanel := lipgloss.NewStyle().Width(m.sources.Width()).Render(sources)
		parts = append(parts, lipgloss.JoinHorizontal(lipgloss.Top, piecePanel, "   ", sourcePanel))
	} else {
		if m.width >= 70 && m.height >= 18 {
			parts = append(parts, m.renderPieceMap())
		}
		parts = append(parts, sources)
	}
	parts = append(parts, m.help.View(m.keys))
	return m.wrap(strings.Join(parts, "\n"))
}

func (m Model) listenerStatus() string {
	if m.listener == nil {
		return ""
	}
	snapshot := m.listener.Snapshot()
	state := "stopped"
	if snapshot.Running {
		state = "listening"
	}
	return lipgloss.NewStyle().Foreground(grayLight).Render(fmt.Sprintf("Inbound: %s on %s  accepted %d  rejected %d", state, snapshot.Address, snapshot.Accepted, snapshot.Rejected))
}

func (m Model) wrap(content string) string {
	if m.width < 70 || m.height < 18 {
		return lipgloss.NewStyle().MaxWidth(max(1, m.width)).Padding(0, 1).Render(content)
	}
	return lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(indigo).Padding(0, 1).Width(max(1, m.width-4)).Render(content)
}

func (m Model) status() string {
	if m.localStatus != "" {
		return m.localStatus
	}
	if status := m.m.Status(); status != "" {
		return status
	}
	return "Initialising..."
}

func (m Model) renderStats(compact bool, width int) string {
	completed := piece.CountCompleted(m.m.Completed())
	downloaded := int64(m.m.CompletedBytes())
	if compact {
		return fmt.Sprintf("%d/%d pcs  %s  Up %s  ETA %s", completed, m.m.TotalPieces(), m.formatSpeed(m.downloadRate), m.formatBytes(int64(m.m.UploadedBytes())), m.formatETA(m.downloadRate, int64(m.m.TotalLength())-downloaded))
	}
	separator := lipgloss.NewStyle().Foreground(grayDark).Render(" │ ")
	contents := []string{
		StatLabel.Render("Pieces:") + StatValue.Render(fmt.Sprintf("%d/%d", completed, m.m.TotalPieces())),
		StatLabel.Render("Sources:") + StatValue.Render(fmt.Sprintf("%d", len(m.m.SourceSnapshots()))),
		StatLabel.Render("Speed:") + SpeedStyle.Render(m.formatSpeed(m.downloadRate)),
		StatLabel.Render("Uploaded:") + StatValue.Render(m.formatBytes(int64(m.m.UploadedBytes()))),
		StatLabel.Render("Size:") + StatValue.Render(m.formatBytes(int64(m.m.TotalLength()))),
		StatLabel.Render("ETA:") + StatValue.Render(m.formatETA(m.downloadRate, int64(m.m.TotalLength())-downloaded)),
	}
	available := max(len(contents), width-(len(contents)-1)*lipgloss.Width(separator))
	baseWidth := available / len(contents)
	remainder := available % len(contents)
	cells := make([]string, len(contents))
	for index, content := range contents {
		cellWidth := baseWidth
		if index < remainder {
			cellWidth++
		}
		cells[index] = lipgloss.NewStyle().Width(cellWidth).MaxWidth(cellWidth).Align(lipgloss.Center).Render(content)
	}
	return strings.Join(cells, separator)
}

func (m Model) renderPieceMap() string {
	completed := m.m.Completed()
	total := m.m.TotalPieces()
	if total == 0 {
		return SectionStyle.Render("Piece Map") + "\n—"
	}
	width := max(1, min(m.pieceMapWidth-2, total))
	var row strings.Builder
	for i := range width {
		index := int(float64(i) / float64(width) * float64(total))
		if index < len(completed) && completed[index] {
			row.WriteString(PieceDone.String())
		} else {
			row.WriteString(PiecePending.String())
		}
	}
	return SectionStyle.Render("Piece Map") + "\n" + row.String()
}

func (m Model) formatSpeed(bytesPerSec float64) string {
	switch {
	case bytesPerSec >= 1_000_000:
		return fmt.Sprintf("%.1f MB/s", bytesPerSec/1_000_000)
	case bytesPerSec >= 1_000:
		return fmt.Sprintf("%.0f KB/s", bytesPerSec/1_000)
	default:
		return fmt.Sprintf("%.0f B/s", bytesPerSec)
	}
}

func (m Model) formatBytes(b int64) string {
	switch {
	case b >= 1_000_000_000:
		return fmt.Sprintf("%.1f GB", float64(b)/1_000_000_000)
	case b >= 1_000_000:
		return fmt.Sprintf("%.1f MB", float64(b)/1_000_000)
	case b >= 1_000:
		return fmt.Sprintf("%.0f KB", float64(b)/1_000)
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func (m Model) formatETA(speed float64, remaining int64) string {
	if speed <= 0 || remaining <= 0 {
		return "—"
	}
	seconds := float64(remaining) / speed
	if math.IsInf(seconds, 0) || math.IsNaN(seconds) {
		return "—"
	}
	if seconds < 60 {
		return fmt.Sprintf("%.0fs", seconds)
	}
	if seconds < 3600 {
		return fmt.Sprintf("%.0fm %.0fs", seconds/60, math.Mod(seconds, 60))
	}
	return fmt.Sprintf("%.0fh %.0fm", seconds/3600, math.Mod(seconds/60, 60))
}

func Run(m *piece.Manager, doneCh <-chan error, reannounce *session.ReannounceController, cancel context.CancelFunc, listeners ...*session.PeerListener) error {
	program := tea.NewProgram(NewModel(m, doneCh, reannounce, cancel, listeners...))
	_, err := program.Run()
	return err
}
