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

const barWidth = 30

// RunSendProgress prints a live \r progress bar for a send transfer.
// It blocks until errCh receives and returns the transfer error.
//
//	[====>                         ]  14%   52.3 MB/s   ETA 8s
//	[==================>           ]  62%   34.7 MB/s   ETA 6s  NACKs: 23
//	[=============================>] 100%   97.4 MB/s   4.2s
//	[==============================] 100%   Repairing   NACKs: 4
func RunSendProgress(s *sender.Sender, errCh <-chan error) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	start := time.Now()
	var prevBytes int64
	var prevTime time.Time
	var ewma float64

	updateEWMA := func(bytes int64, now time.Time) {
		if !prevTime.IsZero() {
			dt := now.Sub(prevTime).Seconds()
			if dt > 0 && bytes > prevBytes {
				instant := float64(bytes-prevBytes) / dt / 1e6
				if ewma == 0 {
					ewma = instant
				} else {
					ewma = 0.25*instant + 0.75*ewma
				}
			}
		}
		prevBytes = bytes
		prevTime = now
	}

	for {
		select {
		case err := <-errCh:
			p := s.Progress()
			if line := fmtSendBar(p, ewma, time.Since(start), true); line != "" {
				fmt.Fprint(os.Stdout, line)
				fmt.Fprintln(os.Stdout)
			}
			return err
		case <-ticker.C:
			now := time.Now()
			p := s.Progress()
			updateEWMA(p.BytesSent, now)
			if line := fmtSendBar(p, ewma, time.Since(start), false); line != "" {
				fmt.Fprint(os.Stdout, line)
			}
		}
	}
}

// RunRecvProgress prints a live \r progress bar for a receive transfer.
// It blocks until errCh receives and returns the transfer error.
func RunRecvProgress(r *receiver.Receiver, errCh <-chan error) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	start := time.Now()
	var prevBytes int64
	var prevTime time.Time
	var ewma float64

	updateEWMA := func(bytes int64, now time.Time) {
		if !prevTime.IsZero() {
			dt := now.Sub(prevTime).Seconds()
			if dt > 0 && bytes > prevBytes {
				instant := float64(bytes-prevBytes) / dt / 1e6
				if ewma == 0 {
					ewma = instant
				} else {
					ewma = 0.25*instant + 0.75*ewma
				}
			}
		}
		prevBytes = bytes
		prevTime = now
	}

	for {
		select {
		case err := <-errCh:
			p := r.Progress()
			if line := fmtRecvBar(p, ewma, time.Since(start), true); line != "" {
				fmt.Fprint(os.Stdout, line)
				fmt.Fprintln(os.Stdout)
			}
			return err
		case <-ticker.C:
			now := time.Now()
			p := r.Progress()
			updateEWMA(p.BytesReceived, now)
			if line := fmtRecvBar(p, ewma, time.Since(start), false); line != "" {
				fmt.Fprint(os.Stdout, line)
			}
		}
	}
}

func fmtSendBar(p sender.SenderProgress, ewma float64, elapsed time.Duration, final bool) string {
	if p.TotalBytes == 0 {
		return ""
	}
	pct := float64(p.BytesSent) / float64(p.TotalBytes)
	if pct > 1 {
		pct = 1
	}
	bar := buildBar(pct)

	var suffix string
	switch {
	case p.InRepair:
		suffix = "   Repairing"
		if p.NACKsSent > 0 {
			suffix += fmt.Sprintf("   NACKs: %d", p.NACKsSent)
		}
	case final:
		suffix = fmt.Sprintf("  %5.1f MB/s   %s", ewma, fmtElapsed(elapsed.Seconds()))
	default:
		suffix = fmt.Sprintf("  %5.1f MB/s", ewma)
		if ewma > 0 && pct < 1 {
			remaining := float64(p.TotalBytes-p.BytesSent) / (ewma * 1e6)
			suffix += fmt.Sprintf("   ETA %s", fmtETA(remaining))
		}
		if p.NACKsSent > 0 {
			suffix += fmt.Sprintf("  NACKs: %d", p.NACKsSent)
		}
	}
	return fmt.Sprintf("\r%s %3.0f%%%s", bar, pct*100, suffix)
}

func fmtRecvBar(p receiver.ReceiverProgress, ewma float64, elapsed time.Duration, final bool) string {
	if p.TotalBytes == 0 {
		return ""
	}
	pct := float64(p.BytesReceived) / float64(p.TotalBytes)
	if pct > 1 {
		pct = 1
	}
	bar := buildBar(pct)

	var suffix string
	if final {
		suffix = fmt.Sprintf("  %5.1f MB/s   %s", ewma, fmtElapsed(elapsed.Seconds()))
	} else {
		suffix = fmt.Sprintf("  %5.1f MB/s", ewma)
		if ewma > 0 && pct < 1 {
			remaining := float64(p.TotalBytes-p.BytesReceived) / (ewma * 1e6)
			suffix += fmt.Sprintf("   ETA %s", fmtETA(remaining))
		}
	}
	return fmt.Sprintf("\r%s %3.0f%%%s", bar, pct*100, suffix)
}

// buildBar returns a [====>     ] style bar of width barWidth.
// The > is the head; at 100% it sits at the last position.
func buildBar(pct float64) string {
	filled := int(pct * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}
	var b strings.Builder
	b.WriteByte('[')
	for i := 0; i < barWidth; i++ {
		switch {
		case i < filled-1:
			b.WriteByte('=')
		case i == filled-1:
			b.WriteByte('>')
		default:
			b.WriteByte(' ')
		}
	}
	b.WriteByte(']')
	return b.String()
}

// fmtETA formats a remaining-seconds estimate as "8s", "1m30s", etc.
func fmtETA(sec float64) string {
	s := int(sec)
	if s < 60 {
		return fmt.Sprintf("%ds", s)
	}
	m := s / 60
	s = s % 60
	if s == 0 {
		return fmt.Sprintf("%dm", m)
	}
	return fmt.Sprintf("%dm%ds", m, s)
}

// fmtElapsed formats an elapsed duration with one decimal for sub-minute values.
func fmtElapsed(sec float64) string {
	if sec < 60 {
		return fmt.Sprintf("%.1fs", sec)
	}
	m := int(sec) / 60
	s := int(sec) % 60
	return fmt.Sprintf("%dm%ds", m, s)
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

// truncName shortens a filename to at most maxLen chars for display.
func truncName(path string, maxLen int) string {
	name := filepath.Base(path)
	if len(name) <= maxLen {
		return name
	}
	return name[:maxLen-3] + "..."
}
