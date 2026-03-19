Proposal: Phase 1.5 – The HP-UDP Terminal Dashboard
1. Alignment: Why TUI First?

After reviewing the feedback, you are 100% correct on the prioritization and the technical risks.

The Batching Trap: Your catch on the WriteBatch pacing interaction was spot on. Buffering 87 KB of packets and blasting them post-sleep perfectly recreates the exact micro-bursting issue we just spent days fixing in the teardown loop. Writing complex sub-pacing logic in Go just to save Linux CPU cycles (when the Windows receiver is the actual bottleneck) is a low-ROI distraction. Syscall batching firmly belongs in Phase 2 (the C port) where sendmmsg and memory arenas can be designed together from the ground up.

Commercial Viability: The protocol math is proven (we just hit 59.9 MB/s and eliminated the memory leak with the lock-free sliding window). The next most valuable step is visualization. A wall of scrolling [cc_debug] text doesn't sell the brilliance of the engine. A clean, reactive dashboard proves it instantly to anyone evaluating the tool.

We are pausing the WriteBatch optimization and moving forward with the TUI.
2. Architectural Blueprint: The Decoupled Engine

The highest risk in building a UI on top of a 60 MB/s network engine is lock-contention. The UI rendering must never block the main send loop or the token bucket.

We will achieve this via a Non-Blocking Telemetry Channel.
The Telemetry Payload

We will define a lightweight struct to carry snapshots of the engine's state:
Go

type Telemetry struct {
    BytesSent    int64
    TotalBytes   int64
    CurrentRate  float64       // From TokenBucket
    LossRate     float64       // From Heartbeat
    RTT          time.Duration // From EchoTimestampNs
    Phase        int           // 1 (Probe) or 2 (Avoidance)
    NACKs        int64         // Cumulative
    State        string        // "Probing", "Transferring", "Repairing..."
}

The Non-Blocking Emit

Inside sender.go, instead of writing to os.Stdout, we will push this struct to a buffered channel (e.g., size 1 or 2) every ~100ms. We will use a select block with a default case: if the channel is full because the TUI is busy drawing the last frame, the sender simply drops the telemetry frame and keeps pumping packets. Zero latency penalty.
3. The Tech Stack & UI Layout

We will use the Charmbracelet ecosystem (bubbletea, lipgloss, bubbles) to build an Elm-style reactive terminal dashboard.

Estimated effort: 5-10 hours, broken down into:

    1-2 hrs: Engine decoupling and telemetry channel wiring.

    3-6 hrs: Bubble Tea model, update loop, and Lipgloss layout iteration.

Proposed Visual Layout (Lipgloss Grids)

    Header Bar: SessionID | Target: 192.168.50.82 | File: big.bin

    Metrics Row (3 Columns):

        Throughput: Live MB/s (Green when high)

        Latency: Live RTT ms

        Health: Loss % and Cumulative NACK count

    State Indicator: Bold text showing Phase 1: Multiplicative Probe or Phase 2: Additive Avoidance.

    Progress Footer: Standard bubbles/progress bar.

        UX Polish: When the main send loop finishes, the bar turns yellow and the text switches to "Repairing Tail Drops..." to indicate FEC/NACK recovery rather than a frozen transfer.