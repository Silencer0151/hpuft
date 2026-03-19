package main

import (
	"fmt"
	"hpuft/receiver"
	"hpuft/sender"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ── Shared styles ─────────────────────────────────────────────────────────────

var (
	styleDim    = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	styleBold   = lipgloss.NewStyle().Bold(true)
	styleGreen  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("10"))
	styleYellow = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("11"))
	styleRed    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("9"))
	styleCyan   = lipgloss.NewStyle().Foreground(lipgloss.Color("14"))

	styleBox = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("8")).
			Padding(0, 1)

	styleHeader = lipgloss.NewStyle().
			Bold(true).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("8")).
			Padding(0, 2)
)

// ── Shared messages ───────────────────────────────────────────────────────────

type tuiTickMsg struct{}
type tuiDoneMsg struct{ err error }

func tuiTickCmd() tea.Cmd {
	return tea.Tick(100*time.Millisecond, func(_ time.Time) tea.Msg {
		return tuiTickMsg{}
	})
}

func tuiWatchDone(errCh <-chan error) tea.Cmd {
	return func() tea.Msg {
		return tuiDoneMsg{err: <-errCh}
	}
}

// ── Shared helpers ────────────────────────────────────────────────────────────

// tuiBar renders a Unicode block progress bar of the given width.
func tuiBar(pct float64, width int) string {
	if width < 4 {
		width = 4
	}
	filled := int(pct * float64(width))
	if filled > width {
		filled = width
	}
	var b strings.Builder
	for i := 0; i < filled; i++ {
		b.WriteRune('█')
	}
	for i := filled; i < width; i++ {
		b.WriteRune('░')
	}
	return b.String()
}

// metricBox renders a small rounded box with a big value and a dim label.
func metricBox(label, value string, width int) string {
	content := styleBold.Render(value) + "\n" + styleDim.Render(label)
	return styleBox.Width(width).Render(content)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Sender TUI
// ═══════════════════════════════════════════════════════════════════════════════

type sendTUIModel struct {
	s        *sender.Sender
	errCh    <-chan error
	fileName string
	addr     string

	width int
	last  sender.SenderProgress
	err   error
	done  bool
}

func newSendTUI(s *sender.Sender, fileName, addr string, errCh <-chan error) sendTUIModel {
	return sendTUIModel{
		s:        s,
		errCh:    errCh,
		fileName: fileName,
		addr:     addr,
		width:    80,
	}
}

func (m sendTUIModel) Init() tea.Cmd {
	return tea.Batch(tuiTickCmd(), tuiWatchDone(m.errCh))
}

func (m sendTUIModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
	case tuiTickMsg:
		m.last = m.s.Progress()
		return m, tuiTickCmd()
	case tuiDoneMsg:
		m.last = m.s.Progress()
		m.err = msg.err
		m.done = true
		return m, tea.Quit
	case tea.KeyMsg:
		if msg.Type == tea.KeyCtrlC {
			return m, tea.Quit
		}
	}
	return m, nil
}

func (m sendTUIModel) View() string {
	p := m.last

	// ── Header ───────────────────────────────────────────────────────────────
	direction := styleGreen.Render(m.fileName) + styleDim.Render(" → ") + styleCyan.Render(m.addr)
	header := styleHeader.Width(m.width - 4).Render(
		"  HPUFT  " + styleDim.Render("│") + "  " + direction,
	)

	// ── Metric boxes ─────────────────────────────────────────────────────────
	// Divide available width among 3 boxes with 2-space gaps.
	// Each box width includes the border (2) + padding (2) = 4 overhead.
	availW := m.width - 6 // 2 side margins + 2 gaps of 2
	boxW := availW / 3
	if boxW < 14 {
		boxW = 14
	}

	rateMBps := p.RateBPS / 1e6
	rateStr := fmt.Sprintf("%.1f MB/s", rateMBps)
	rateBox := metricBox("Throughput", rateStr, boxW)

	rttStr := "--- ms"
	if p.RTT > 0 {
		rttStr = fmt.Sprintf("%d ms", p.RTT.Milliseconds())
	}
	rttBox := metricBox("RTT", rttStr, boxW)

	lossStr := fmt.Sprintf("%.2f%%  NACKs: %d", p.LossRate, p.NACKsSent)
	healthBox := metricBox("Loss / NACKs", lossStr, boxW)

	metricsRow := lipgloss.JoinHorizontal(lipgloss.Top, rateBox, "  ", rttBox, "  ", healthBox)

	// ── Phase / state indicator ───────────────────────────────────────────────
	var phaseStr string
	switch {
	case p.InRepair:
		phaseStr = styleYellow.Render("⟳  Repairing tail drops...")
	case p.CCPhase == 2:
		phaseStr = styleCyan.Render("Phase 2: Additive Avoidance")
	default:
		phaseStr = styleGreen.Render("Phase 1: Multiplicative Probe")
	}

	// ── Progress bar ─────────────────────────────────────────────────────────
	pct := 0.0
	if p.TotalBytes > 0 {
		pct = float64(p.BytesSent) / float64(p.TotalBytes)
		if pct > 1 {
			pct = 1
		}
	}

	barWidth := m.width - 4
	bar := tuiBar(pct, barWidth)

	var progressLine string
	if p.InRepair {
		progressLine = fmt.Sprintf("%s / %s  (%s)  Repairing...",
			humanBytes(p.TotalBytes), humanBytes(p.TotalBytes),
			styleYellow.Render("100%"),
		)
	} else {
		etaStr := ""
		if p.RateBPS > 0 && pct < 1 {
			remaining := float64(p.TotalBytes-p.BytesSent) / p.RateBPS
			etaStr = "  ETA: " + fmtDuration(remaining)
		}
		progressLine = fmt.Sprintf("%s / %s  (%s)%s",
			humanBytes(p.BytesSent), humanBytes(p.TotalBytes),
			styleBold.Render(fmt.Sprintf("%.0f%%", pct*100)),
			etaStr,
		)
	}

	return "\n" +
		"  " + header + "\n\n" +
		"  " + metricsRow + "\n\n" +
		"  " + phaseStr + "\n\n" +
		"  " + progressLine + "\n" +
		"  " + bar + "\n"
}

// RunSendTUI runs the full-screen sender dashboard and blocks until the
// transfer completes (or is interrupted). It returns the transfer error.
func RunSendTUI(s *sender.Sender, fileName, addr string, errCh <-chan error) error {
	m := newSendTUI(s, fileName, addr, errCh)
	p := tea.NewProgram(m, tea.WithAltScreen())
	final, err := p.Run()
	if err != nil {
		return fmt.Errorf("tui: %w", err)
	}
	if fm, ok := final.(sendTUIModel); ok {
		return fm.err
	}
	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════
// Receiver TUI
// ═══════════════════════════════════════════════════════════════════════════════

type recvTUIModel struct {
	r        *receiver.Receiver
	errCh    <-chan error
	fileName string
	addr     string

	width int
	last  receiver.ReceiverProgress
	err   error
	done  bool
}

func newRecvTUI(r *receiver.Receiver, fileName, addr string, errCh <-chan error) recvTUIModel {
	return recvTUIModel{
		r:        r,
		errCh:    errCh,
		fileName: fileName,
		addr:     addr,
		width:    80,
	}
}

func (m recvTUIModel) Init() tea.Cmd {
	return tea.Batch(tuiTickCmd(), tuiWatchDone(m.errCh))
}

func (m recvTUIModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
	case tuiTickMsg:
		m.last = m.r.Progress()
		return m, tuiTickCmd()
	case tuiDoneMsg:
		m.last = m.r.Progress()
		m.err = msg.err
		m.done = true
		return m, tea.Quit
	case tea.KeyMsg:
		if msg.Type == tea.KeyCtrlC {
			return m, tea.Quit
		}
	}
	return m, nil
}

func (m recvTUIModel) View() string {
	p := m.last

	// ── Header ───────────────────────────────────────────────────────────────
	direction := styleCyan.Render(m.addr) + styleDim.Render(" → ") + styleGreen.Render(m.fileName)
	header := styleHeader.Width(m.width - 4).Render(
		"  HPUFT  " + styleDim.Render("│") + "  " + direction,
	)

	// ── Metric box row ────────────────────────────────────────────────────────
	availW := m.width - 6
	boxW := availW / 3
	if boxW < 14 {
		boxW = 14
	}

	elapsed := time.Duration(0)
	if p.StartNs > 0 {
		elapsed = time.Duration(time.Now().UnixNano()-p.StartNs) * time.Nanosecond
	}

	rateMBps := 0.0
	if elapsed.Seconds() > 0.1 {
		rateMBps = float64(p.BytesReceived) / elapsed.Seconds() / 1e6
	}

	rateBox := metricBox("Throughput", fmt.Sprintf("%.1f MB/s", rateMBps), boxW)

	pct := 0.0
	if p.TotalBytes > 0 {
		pct = float64(p.BytesReceived) / float64(p.TotalBytes)
		if pct > 1 {
			pct = 1
		}
	}
	pctBox := metricBox("Progress", fmt.Sprintf("%.0f%%", pct*100), boxW)
	fecBox := metricBox("FEC Rebuilt", fmt.Sprintf("%d pkts", p.Rebuilt), boxW)
	metricsRow := lipgloss.JoinHorizontal(lipgloss.Top, rateBox, "  ", pctBox, "  ", fecBox)

	// ── Progress bar ─────────────────────────────────────────────────────────
	barWidth := m.width - 4
	bar := tuiBar(pct, barWidth)

	etaStr := ""
	if rateMBps > 0 && pct < 1 {
		remaining := float64(p.TotalBytes-p.BytesReceived) / (rateMBps * 1e6)
		etaStr = "  ETA: " + fmtDuration(remaining)
	}
	progressLine := fmt.Sprintf("%s / %s  (%s)%s",
		humanBytes(p.BytesReceived), humanBytes(p.TotalBytes),
		styleBold.Render(fmt.Sprintf("%.0f%%", pct*100)),
		etaStr,
	)

	return "\n" +
		"  " + header + "\n\n" +
		"  " + metricsRow + "\n\n" +
		"  " + progressLine + "\n" +
		"  " + bar + "\n"
}

// RunRecvTUI runs the full-screen receiver dashboard and blocks until the
// transfer completes (or is interrupted). It returns the transfer error.
func RunRecvTUI(r *receiver.Receiver, fileName, addr string, errCh <-chan error) error {
	m := newRecvTUI(r, fileName, addr, errCh)
	p := tea.NewProgram(m, tea.WithAltScreen())
	final, err := p.Run()
	if err != nil {
		return fmt.Errorf("tui: %w", err)
	}
	if fm, ok := final.(recvTUIModel); ok {
		return fm.err
	}
	return nil
}
