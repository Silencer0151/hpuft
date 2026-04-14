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

	var tmpDir string
	if outDir == "" {
		tmpDir, err = os.MkdirTemp("", "hpuft-test-*")
		if err != nil {
			log.Fatalf("create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)
		outDir = tmpDir
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

// runOneTransfer starts a serve daemon (with optional loss proxy) and drives a
// complete put→get round-trip, verifying the retrieved file matches the source.
func runOneTransfer(self, file string, lossPct float64, outDir string, timeoutSec int) transferResult {
	fileInfo, err := os.Stat(file)
	if err != nil {
		return transferResult{file: file, loss: lossPct, errMsg: err.Error()}
	}
	fileSize := fileInfo.Size()

	servePort := freePort()
	proxyPort := freePort()

	// Serve daemon writes received files into a dedicated subdirectory.
	serveDir := filepath.Join(outDir, fmt.Sprintf("serve_%.0f_%d", lossPct, servePort))
	os.MkdirAll(serveDir, 0755)

	// Get output lands in a separate dir to avoid name collision.
	getDir := filepath.Join(outDir, fmt.Sprintf("get_%.0f_%d", lossPct, servePort))
	os.MkdirAll(getDir, 0755)

	log.Printf("[test] %s @ %.1f%% loss (serve=:%d proxy=:%d)",
		filepath.Base(file), lossPct, servePort, proxyPort)

	// Start serve daemon.
	serve := exec.Command(self, "serve",
		"-listen", fmt.Sprintf(":%d", servePort),
		"-dir", serveDir,
	)
	var serveOut bytes.Buffer
	serve.Stdout = &serveOut
	serve.Stderr = &serveOut
	if err := serve.Start(); err != nil {
		return transferResult{file: file, loss: lossPct, errMsg: fmt.Sprintf("start serve: %v", err)}
	}
	defer func() { serve.Process.Kill(); serve.Wait() }()
	time.Sleep(200 * time.Millisecond)

	// Determine the address that put/get will connect to.
	var targetAddr string
	var proxy *exec.Cmd

	if lossPct > 0 {
		proxy = exec.Command(self, "proxy",
			"-listen", fmt.Sprintf(":%d", proxyPort),
			"-target", fmt.Sprintf("127.0.0.1:%d", servePort),
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
		targetAddr = fmt.Sprintf("127.0.0.1:%d", proxyPort)
	} else {
		targetAddr = fmt.Sprintf("127.0.0.1:%d", servePort)
	}

	absFile, _ := filepath.Abs(file)

	// --- PUT phase ---
	put := exec.Command(self, "put",
		"-file", absFile,
		"-addr", targetAddr,
		"-nodelay",
	)
	var putOut bytes.Buffer
	put.Stdout = &putOut
	put.Stderr = &putOut

	start := time.Now()
	if err := put.Start(); err != nil {
		return transferResult{file: file, loss: lossPct, errMsg: fmt.Sprintf("start put: %v", err)}
	}

	putDone := make(chan error, 1)
	go func() { putDone <- put.Wait() }()

	select {
	case err := <-putDone:
		if err != nil {
			return transferResult{
				file:     file,
				loss:     lossPct,
				duration: time.Since(start),
				errMsg:   fmt.Sprintf("put error: %v\nPUT:\n%s\nSERVE:\n%s", err, putOut.String(), serveOut.String()),
			}
		}
	case <-time.After(time.Duration(timeoutSec) * time.Second):
		put.Process.Kill()
		return transferResult{
			file:     file,
			loss:     lossPct,
			duration: time.Duration(timeoutSec) * time.Second,
			errMsg:   fmt.Sprintf("put timeout after %ds\nput: %s\nserve: %s", timeoutSec, putOut.String(), serveOut.String()),
		}
	}

	putDuration := time.Since(start)

	// --- GET phase ---
	get := exec.Command(self, "get",
		"-file", filepath.Base(absFile),
		"-addr", targetAddr,
		"-out", getDir,
		"-nodelay",
	)
	var getOut bytes.Buffer
	get.Stdout = &getOut
	get.Stderr = &getOut

	if err := get.Start(); err != nil {
		return transferResult{file: file, loss: lossPct, duration: putDuration, errMsg: fmt.Sprintf("start get: %v", err)}
	}

	getDone := make(chan error, 1)
	go func() { getDone <- get.Wait() }()

	select {
	case err := <-getDone:
		if err != nil {
			return transferResult{
				file:     file,
				loss:     lossPct,
				duration: putDuration,
				errMsg:   fmt.Sprintf("get error: %v\nGET:\n%s\nSERVE:\n%s", err, getOut.String(), serveOut.String()),
			}
		}
	case <-time.After(time.Duration(timeoutSec) * time.Second):
		get.Process.Kill()
		return transferResult{
			file:     file,
			loss:     lossPct,
			duration: putDuration,
			errMsg:   fmt.Sprintf("get timeout after %ds\nget: %s\nserve: %s", timeoutSec, getOut.String(), serveOut.String()),
		}
	}

	duration := putDuration + time.Since(start.Add(putDuration))

	// --- Integrity check ---
	original, err := os.ReadFile(file)
	if err != nil {
		return transferResult{file: file, loss: lossPct, duration: duration, errMsg: fmt.Sprintf("read original: %v", err)}
	}
	receivedPath := filepath.Join(getDir, filepath.Base(file))
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
