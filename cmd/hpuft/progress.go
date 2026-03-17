package main

import (
	"fmt"
	"hpuft/receiver"
	"hpuft/sender"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const barWidth = 20

// RunSendProgress prints a live \r progress bar for a push/send transfer.
// It polls s.Progress() every 100ms until done is closed, then prints the
// final summary line.
func RunSendProgress(s *sender.Sender, done <-chan struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			p := s.Progress()
			printSendBar(p, true)
			fmt.Fprintln(os.Stdout)
			return
		case <-ticker.C:
			printSendBar(s.Progress(), false)
		}
	}
}

// RunRecvProgress prints a live \r progress bar for a recv/get transfer.
func RunRecvProgress(r *receiver.Receiver, done <-chan struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			p := r.Progress()
			printRecvBar(p, true)
			fmt.Fprintln(os.Stdout)
			return
		case <-ticker.C:
			printRecvBar(r.Progress(), false)
		}
	}
}

func printSendBar(p sender.SenderProgress, final bool) {
	if p.TotalBytes == 0 {
		return
	}
	pct := float64(p.BytesSent) / float64(p.TotalBytes)
	if pct > 1 {
		pct = 1
	}

	elapsed := time.Duration(time.Now().UnixNano()-p.StartNs) * time.Nanosecond
	if p.StartNs == 0 {
		elapsed = 0
	}

	rateMBps := 0.0
	if p.RateBPS > 0 {
		rateMBps = p.RateBPS / 1e6
	} else if elapsed.Seconds() > 0 {
		rateMBps = float64(p.BytesSent) / elapsed.Seconds() / 1e6
	}

	eta := ""
	if rateMBps > 0 && pct < 1 {
		remaining := float64(p.TotalBytes-p.BytesSent) / (rateMBps * 1e6)
		eta = fmt.Sprintf(" | ETA: %s", fmtDuration(remaining))
	}

	bar := buildBar(pct)
	var line string
	if p.InRepair {
		line = fmt.Sprintf("\r%s %s 100%% | Repairing... | NACKs: %d",
			bar, humanBytes(p.TotalBytes), p.NACKsSent)
	} else {
		line = fmt.Sprintf("\r%s %s %3.0f%% | %5.1f MB/s%s | NACKs: %d",
			bar, humanBytes(p.TotalBytes), pct*100, rateMBps, eta, p.NACKsSent)
	}

	if final {
		fmt.Fprint(os.Stdout, line)
	} else {
		fmt.Fprint(os.Stdout, line)
	}
}

func printRecvBar(p receiver.ReceiverProgress, final bool) {
	if p.TotalBytes == 0 {
		return
	}
	pct := float64(p.BytesReceived) / float64(p.TotalBytes)
	if pct > 1 {
		pct = 1
	}

	elapsed := time.Duration(time.Now().UnixNano()-p.StartNs) * time.Nanosecond
	if p.StartNs == 0 {
		elapsed = 0
	}

	rateMBps := 0.0
	if elapsed.Seconds() > 0.1 {
		rateMBps = float64(p.BytesReceived) / elapsed.Seconds() / 1e6
	}

	eta := ""
	if rateMBps > 0 && pct < 1 {
		remaining := float64(p.TotalBytes-p.BytesReceived) / (rateMBps * 1e6)
		eta = fmt.Sprintf(" | ETA: %s", fmtDuration(remaining))
	}

	bar := buildBar(pct)
	line := fmt.Sprintf("\r%s %s %3.0f%% | %5.1f MB/s%s | Rebuilt: %d",
		bar, humanBytes(p.TotalBytes), pct*100, rateMBps, eta, p.Rebuilt)

	fmt.Fprint(os.Stdout, line)
}

func buildBar(pct float64) string {
	filled := int(pct * barWidth)
	if filled > barWidth {
		filled = barWidth
	}
	var b strings.Builder
	b.WriteByte('[')
	for i := 0; i < barWidth; i++ {
		switch {
		case i < filled:
			b.WriteByte('=')
		case i == filled:
			b.WriteByte('>')
		default:
			b.WriteByte(' ')
		}
	}
	b.WriteByte(']')
	return b.String()
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.0f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func fmtDuration(sec float64) string {
	if sec > 3600 {
		return fmt.Sprintf("%d:%02d:%02d", int(sec)/3600, int(sec)%3600/60, int(sec)%60)
	}
	return fmt.Sprintf("%d:%02d", int(sec)/60, int(sec)%60)
}

// truncName shortens a filename to at most maxLen chars for the progress bar.
func truncName(path string, maxLen int) string {
	name := filepath.Base(path)
	if len(name) <= maxLen {
		return name
	}
	return name[:maxLen-3] + "..."
}
