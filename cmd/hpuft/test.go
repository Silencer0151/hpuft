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
	"strconv"
	"strings"
	"time"
)

func runTest(args []string) {
	fs := flag.NewFlagSet("test", flag.ExitOnError)
	fs.Usage = func() {
		log.Print("usage: hpuft test [-files f1,f2,...] [-loss 0,1,5,10,15] [-out dir] [-timeout 120]")
		fs.PrintDefaults()
	}

	var (
		filesStr string
		lossStr  string
		outDir   string
		timeout  int
	)

	fs.StringVar(&filesStr, "files", "", "comma-separated list of files to transfer (default: auto-detect testdata/)")
	fs.StringVar(&lossStr, "loss", "0,1,5,10,15", "comma-separated loss percentages to test")
	fs.StringVar(&outDir, "out", "", "output directory for received files (default: temp dir)")
	fs.IntVar(&timeout, "timeout", 120, "per-transfer timeout in seconds")
	fs.Parse(args)

	log.SetFlags(log.Ltime)

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

	var files []string
	if filesStr != "" {
		files = strings.Split(filesStr, ",")
	} else {
		files = discoverTestFiles("testdata")
	}
	if len(files) == 0 {
		log.Fatal("no test files found")
	}

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

	// Resolve the running binary so subprocesses call back into this binary.
	self, err := os.Executable()
	if err != nil {
		log.Fatalf("resolve executable: %v", err)
	}

	if outDir == "" {
		outDir, err = os.MkdirTemp("", "hpuft-test-*")
		if err != nil {
			log.Fatalf("create temp dir: %v", err)
		}
		defer os.RemoveAll(outDir)
	}

	log.Printf("=== HP-UDP Integration Test Suite ===")
	log.Printf("binary:  %s", self)
	log.Printf("files:   %v", files)
	log.Printf("loss:    %v%%", lossRates)
	log.Printf("")

	type result struct {
		file           string
		loss           float64
		duration       time.Duration
		throughputMBps float64
		pass           bool
		errMsg         string
	}

	var results []result
	passed, failed := 0, 0

	for _, loss := range lossRates {
		for _, file := range files {
			r := runOneTransfer(self, file, loss, outDir, timeout)
			results = append(results, result(r))
			if r.pass {
				passed++
			} else {
				failed++
			}
		}
	}

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    INTEGRATION TEST RESULTS                     ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  %-30s %6s %8s %10s %4s ║\n", "FILE", "LOSS%", "TIME", "RATE", "OK")
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")

	for _, r := range results {
		status := "PASS"
		if !r.pass {
			status = "FAIL"
		}
		name := filepath.Base(r.file)
		if len(name) > 30 {
			name = name[:27] + "..."
		}
		fmt.Printf("║  %-30s %5.1f%% %7.1fs %7.2f MB/s %4s ║\n",
			name, r.loss, r.duration.Seconds(), r.throughputMBps, status)
	}

	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  TOTAL: %d passed, %d failed, %d tests                          ║\n",
		passed, failed, len(results))
	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")

	if failed > 0 {
		fmt.Println()
		fmt.Println("FAILURES:")
		for _, r := range results {
			if !r.pass {
				fmt.Printf("  %s @ %.1f%% loss: %s\n", filepath.Base(r.file), r.loss, r.errMsg)
			}
		}
		os.Exit(1)
	}
}

type transferResult struct {
	file           string
	loss           float64
	duration       time.Duration
	throughputMBps float64
	pass           bool
	errMsg         string
}

func runOneTransfer(self, file string, lossPct float64, outDir string, timeoutSec int) transferResult {
	fileInfo, err := os.Stat(file)
	if err != nil {
		return transferResult{file: file, loss: lossPct, errMsg: err.Error()}
	}
	fileSize := fileInfo.Size()

	receiverPort := freePort()
	proxyPort := freePort()

	recvDir := filepath.Join(outDir, fmt.Sprintf("loss_%.0f", lossPct))
	os.MkdirAll(recvDir, 0755)
	receivedPath := filepath.Join(recvDir, filepath.Base(file))
	os.Remove(receivedPath)

	log.Printf("[test] %s @ %.1f%% loss (receiver=:%d proxy=:%d)",
		filepath.Base(file), lossPct, receiverPort, proxyPort)

	// Start receiver
	recv := exec.Command(self, "recv",
		"-listen", fmt.Sprintf(":%d", receiverPort),
		"-out", recvDir,
	)
	var recvOut bytes.Buffer
	recv.Stdout = &recvOut
	recv.Stderr = &recvOut
	if err := recv.Start(); err != nil {
		return transferResult{file: file, loss: lossPct, errMsg: fmt.Sprintf("start receiver: %v", err)}
	}
	defer func() { recv.Process.Kill(); recv.Wait() }()
	time.Sleep(200 * time.Millisecond)

	// Start proxy (skip at 0% loss — connect sender directly to receiver)
	var senderTarget string
	var proxy *exec.Cmd

	if lossPct > 0 {
		proxy = exec.Command(self, "proxy",
			"-listen", fmt.Sprintf(":%d", proxyPort),
			"-target", fmt.Sprintf("127.0.0.1:%d", receiverPort),
			"-loss", fmt.Sprintf("%.1f", lossPct),
			"-seed", fmt.Sprintf("%d", rand.Int63()),
		)
		var proxyOut bytes.Buffer
		proxy.Stdout = &proxyOut
		proxy.Stderr = &proxyOut
		if err := proxy.Start(); err != nil {
			return transferResult{file: file, loss: lossPct, errMsg: fmt.Sprintf("start proxy: %v", err)}
		}
		defer func() { proxy.Process.Kill(); proxy.Wait() }()
		time.Sleep(200 * time.Millisecond)
		senderTarget = fmt.Sprintf("127.0.0.1:%d", proxyPort)
	} else {
		senderTarget = fmt.Sprintf("127.0.0.1:%d", receiverPort)
	}

	absFile, _ := filepath.Abs(file)
	send := exec.Command(self, "send",
		"-file", absFile,
		"-addr", senderTarget,
		"-nodelay",
	)
	var sendOut bytes.Buffer
	send.Stdout = &sendOut
	send.Stderr = &sendOut

	start := time.Now()
	if err := send.Start(); err != nil {
		return transferResult{file: file, loss: lossPct, errMsg: fmt.Sprintf("start sender: %v", err)}
	}

	done := make(chan error, 1)
	go func() { done <- send.Wait() }()

	select {
	case err := <-done:
		if err != nil {
			return transferResult{
				file:     file,
				loss:     lossPct,
				duration: time.Since(start),
				errMsg:   fmt.Sprintf("sender error: %v\nSENDER:\n%s\nRECEIVER:\n%s", err, sendOut.String(), recvOut.String()),
			}
		}
	case <-time.After(time.Duration(timeoutSec) * time.Second):
		send.Process.Kill()
		return transferResult{
			file:     file,
			loss:     lossPct,
			duration: time.Duration(timeoutSec) * time.Second,
			errMsg:   fmt.Sprintf("timeout after %ds\nsender: %s\nreceiver: %s", timeoutSec, sendOut.String(), recvOut.String()),
		}
	}

	duration := time.Since(start)

	recvDone := make(chan error, 1)
	go func() { recvDone <- recv.Wait() }()

	select {
	case err := <-recvDone:
		if err != nil {
			return transferResult{
				file:     file,
				loss:     lossPct,
				duration: duration,
				errMsg:   fmt.Sprintf("receiver error: %v\n%s", err, recvOut.String()),
			}
		}
	case <-time.After(15 * time.Second):
		return transferResult{
			file:     file,
			loss:     lossPct,
			duration: duration,
			errMsg:   fmt.Sprintf("receiver didn't finish within 15s of sender completing\n%s", recvOut.String()),
		}
	}

	original, err := os.ReadFile(file)
	if err != nil {
		return transferResult{file: file, loss: lossPct, duration: duration, errMsg: fmt.Sprintf("read original: %v", err)}
	}
	received, err := os.ReadFile(receivedPath)
	if err != nil {
		return transferResult{file: file, loss: lossPct, duration: duration, errMsg: fmt.Sprintf("read received: %v", err)}
	}
	if !bytes.Equal(original, received) {
		return transferResult{
			file:     file,
			loss:     lossPct,
			duration: duration,
			errMsg:   fmt.Sprintf("INTEGRITY FAIL: original=%d bytes, received=%d bytes", len(original), len(received)),
		}
	}

	throughput := float64(fileSize) / duration.Seconds() / 1e6
	log.Printf("[test] PASS %s @ %.1f%% loss in %.1fs (%.2f MB/s)",
		filepath.Base(file), lossPct, duration.Seconds(), throughput)

	return transferResult{
		file:           file,
		loss:           lossPct,
		duration:       duration,
		throughputMBps: throughput,
		pass:           true,
	}
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

func freePort() int {
	l, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return 10000 + rand.Intn(50000)
	}
	port := l.LocalAddr().(*net.UDPAddr).Port
	l.Close()
	return port
}
