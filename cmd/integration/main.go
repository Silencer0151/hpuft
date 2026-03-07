// Command integration runs end-to-end transfer tests through a lossy proxy.
//
// Usage:
//
//	go run cmd/integration/main.go [-files file1,file2,...] [-loss 0,1,5,10,15]
//
// Files default to testdata/small.txt and testdata/random.bin.
// If a file doesn't exist, it's skipped with a warning.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type testResult struct {
	File           string
	Loss           float64
	Duration       time.Duration
	ThroughputMBps float64
	Pass           bool
	Error          string
}

func main() {
	var (
		filesStr string
		lossStr  string
		outDir   string
		timeout  int
	)

	flag.StringVar(&filesStr, "files", "", "comma-separated list of files to transfer (default: auto-detect testdata/)")
	flag.StringVar(&lossStr, "loss", "0,1,5,10,15", "comma-separated loss percentages to test")
	flag.StringVar(&outDir, "out", "", "output directory for received files (default: temp dir)")
	flag.IntVar(&timeout, "timeout", 120, "per-transfer timeout in seconds")
	flag.Parse()

	log.SetFlags(log.Ltime)

	// Parse loss rates
	var lossRates []float64
	for _, s := range strings.Split(lossStr, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			log.Fatalf("invalid loss rate %q: %v", s, err)
		}
		lossRates = append(lossRates, v)
	}

	// Discover test files
	var files []string
	if filesStr != "" {
		files = strings.Split(filesStr, ",")
	} else {
		files = discoverTestFiles("testdata")
	}

	if len(files) == 0 {
		log.Fatal("no test files found")
	}

	// Validate files exist
	var validFiles []string
	for _, f := range files {
		f = strings.TrimSpace(f)
		if _, err := os.Stat(f); err != nil {
			log.Printf("WARNING: skipping %s (%v)", f, err)
			continue
		}
		validFiles = append(validFiles, f)
	}
	files = validFiles

	if len(files) == 0 {
		log.Fatal("no valid test files found")
	}

	log.Printf("=== HP-UDP Integration Test Suite ===")
	log.Printf("files: %v", files)
	log.Printf("loss rates: %v%%", lossRates)
	log.Printf("")

	// Build binaries
	log.Printf("building binaries...")
	buildAll()

	// Output directory
	if outDir == "" {
		var err error
		outDir, err = os.MkdirTemp("", "hpuft-integration-*")
		if err != nil {
			log.Fatalf("create temp dir: %v", err)
		}
		defer os.RemoveAll(outDir)
	}

	// Run tests
	var results []testResult
	passed, failed := 0, 0

	for _, loss := range lossRates {
		for _, file := range files {
			r := runTransfer(file, loss, outDir, timeout)
			results = append(results, r)
			if r.Pass {
				passed++
			} else {
				failed++
			}
		}
	}

	// Print summary
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    INTEGRATION TEST RESULTS                     ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  %-30s %6s %8s %10s %4s ║\n", "FILE", "LOSS%", "TIME", "RATE", "OK")
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")

	for _, r := range results {
		status := "PASS"
		if !r.Pass {
			status = "FAIL"
		}
		name := filepath.Base(r.File)
		if len(name) > 30 {
			name = name[:27] + "..."
		}
		fmt.Printf("║  %-30s %5.1f%% %7.1fs %7.2f MB/s %4s ║\n",
			name, r.Loss, r.Duration.Seconds(), r.ThroughputMBps, status)
	}

	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  TOTAL: %d passed, %d failed, %d tests                          ║\n",
		passed, failed, len(results))
	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")

	if failed > 0 {
		fmt.Println()
		fmt.Println("FAILURES:")
		for _, r := range results {
			if !r.Pass {
				fmt.Printf("  %s @ %.1f%% loss: %s\n", filepath.Base(r.File), r.Loss, r.Error)
			}
		}
		os.Exit(1)
	}
}

func runTransfer(file string, lossPct float64, outDir string, timeoutSec int) testResult {
	fileInfo, err := os.Stat(file)
	if err != nil {
		return testResult{File: file, Loss: lossPct, Error: err.Error()}
	}
	fileSize := fileInfo.Size()

	// Pick random ports to avoid conflicts
	receiverPort := randomPort()
	proxyPort := randomPort()

	recvDir := filepath.Join(outDir, fmt.Sprintf("loss_%.0f", lossPct))
	os.MkdirAll(recvDir, 0755)
	receivedPath := filepath.Join(recvDir, filepath.Base(file))
	os.Remove(receivedPath)

	log.Printf("[test] %s @ %.1f%% loss (receiver=:%d proxy=:%d)",
		filepath.Base(file), lossPct, receiverPort, proxyPort)

	// Start receiver
	receiver := exec.Command(binPath("hpuft-receiver"),
		"-listen", fmt.Sprintf(":%d", receiverPort),
		"-out", recvDir,
	)
	var recvOut bytes.Buffer
	receiver.Stdout = &recvOut
	receiver.Stderr = &recvOut
	if err := receiver.Start(); err != nil {
		return testResult{File: file, Loss: lossPct, Error: fmt.Sprintf("start receiver: %v", err)}
	}
	defer func() {
		receiver.Process.Kill()
		receiver.Wait()
	}()
	time.Sleep(200 * time.Millisecond)

	// Start proxy (skip if 0% loss — connect directly)
	var senderTarget string
	var proxy *exec.Cmd

	if lossPct > 0 {
		proxy = exec.Command(binPath("hpuft-proxy"),
			"-listen", fmt.Sprintf(":%d", proxyPort),
			"-target", fmt.Sprintf("127.0.0.1:%d", receiverPort),
			"-loss", fmt.Sprintf("%.1f", lossPct),
			"-seed", fmt.Sprintf("%d", rand.Int63()),
		)
		var proxyOut bytes.Buffer
		proxy.Stdout = &proxyOut
		proxy.Stderr = &proxyOut
		if err := proxy.Start(); err != nil {
			return testResult{File: file, Loss: lossPct, Error: fmt.Sprintf("start proxy: %v", err)}
		}
		defer func() {
			proxy.Process.Kill()
			proxy.Wait()
		}()
		time.Sleep(200 * time.Millisecond)
		senderTarget = fmt.Sprintf("127.0.0.1:%d", proxyPort)
	} else {
		senderTarget = fmt.Sprintf("127.0.0.1:%d", receiverPort)
	}

	// Start sender
	// Use -nodelay for integration tests: the goal is integrity verification,
	// not congestion control behavior. CC is tested separately.
	absFile, _ := filepath.Abs(file)
	sender := exec.Command(binPath("hpuft-sender"),
		"-file", absFile,
		"-addr", senderTarget,
		"-nodelay",
	)
	var sendOut bytes.Buffer
	sender.Stdout = &sendOut
	sender.Stderr = &sendOut

	start := time.Now()
	if err := sender.Start(); err != nil {
		return testResult{File: file, Loss: lossPct, Error: fmt.Sprintf("start sender: %v", err)}
	}

	// Wait with timeout
	done := make(chan error, 1)
	go func() { done <- sender.Wait() }()

	select {
	case err := <-done:
		if err != nil {
			return testResult{
				File:     file,
				Loss:     lossPct,
				Duration: time.Since(start),
				Error:    fmt.Sprintf("sender error: %v\nSENDER:\n%s\nRECEIVER:\n%s", err, sendOut.String(), recvOut.String()),
			}
		}
	case <-time.After(time.Duration(timeoutSec) * time.Second):
		sender.Process.Kill()
		return testResult{
			File:     file,
			Loss:     lossPct,
			Duration: time.Duration(timeoutSec) * time.Second,
			Error:    fmt.Sprintf("timeout after %ds\nsender: %s\nreceiver: %s", timeoutSec, sendOut.String(), recvOut.String()),
		}
	}

	duration := time.Since(start)

	// Wait for receiver to finish
	recvDone := make(chan error, 1)
	go func() { recvDone <- receiver.Wait() }()

	select {
	case err := <-recvDone:
		if err != nil {
			return testResult{
				File:     file,
				Loss:     lossPct,
				Duration: duration,
				Error:    fmt.Sprintf("receiver error: %v\n%s", err, recvOut.String()),
			}
		}
	case <-time.After(15 * time.Second):
		return testResult{
			File:     file,
			Loss:     lossPct,
			Duration: duration,
			Error:    fmt.Sprintf("receiver didn't finish within 15s of sender completing\n%s", recvOut.String()),
		}
	}

	// Verify file integrity
	original, err := os.ReadFile(file)
	if err != nil {
		return testResult{File: file, Loss: lossPct, Duration: duration, Error: fmt.Sprintf("read original: %v", err)}
	}

	received, err := os.ReadFile(receivedPath)
	if err != nil {
		return testResult{File: file, Loss: lossPct, Duration: duration, Error: fmt.Sprintf("read received: %v", err)}
	}

	if !bytes.Equal(original, received) {
		return testResult{
			File:     file,
			Loss:     lossPct,
			Duration: duration,
			Error:    fmt.Sprintf("INTEGRITY FAIL: original=%d bytes, received=%d bytes", len(original), len(received)),
		}
	}

	throughput := float64(fileSize) / duration.Seconds() / 1e6

	log.Printf("[test] PASS %s @ %.1f%% loss in %.1fs (%.2f MB/s)",
		filepath.Base(file), lossPct, duration.Seconds(), throughput)

	return testResult{
		File:           file,
		Loss:           lossPct,
		Duration:       duration,
		ThroughputMBps: throughput,
		Pass:           true,
	}
}

func buildAll() {
	binaries := []struct {
		output string
		pkg    string
	}{
		{"hpuft-sender", "./cmd/sender"},
		{"hpuft-receiver", "./cmd/receiver"},
		{"hpuft-proxy", "./cmd/proxy"},
	}

	for _, b := range binaries {
		output := b.output
		if runtime.GOOS == "windows" {
			output += ".exe"
		}
		absOut, _ := filepath.Abs(output)
		cmd := exec.Command("go", "build", "-o", absOut, b.pkg)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Fatalf("build %s: %v", b.pkg, err)
		}
	}

	// Store absolute path prefix for binary lookup
	binDir, _ = filepath.Abs(".")
	log.Printf("binaries built in %s", binDir)
}

var binDir string

func binPath(name string) string {
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join(binDir, name)
}

func discoverTestFiles(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	var files []string
	for _, e := range entries {
		if e.IsDir() || strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		files = append(files, filepath.Join(dir, e.Name()))
	}
	return files
}

func randomPort() int {
	// Find a free port
	l, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return 10000 + rand.Intn(50000)
	}
	port := l.LocalAddr().(*net.UDPAddr).Port
	l.Close()
	return port
}
