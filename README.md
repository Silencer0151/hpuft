# hpuft

## Installation Instructions

### Download and install Go
  https://go.dev/

### Clone the repo or download code 

### build with
```go
go build -o hpuft-sender.exe ./cmd/sender
go build -o hpuft-receiver.exe ./cmd/receiver
```

## Usage
### Sender
```bash
  Usage hpuft-sender:
  -addr string
        receiver address (e.g., 127.0.0.1:9000) (default "127.0.0.1:9000")
  -delay int
        inter-packet delay in microseconds (disables CC) (default -1)
  -file string
        path to file to send (required)
  -nocc
        disable congestion control (use fixed rate)
  -nodelay
        send as fast as possible (disables CC)
  -rate float
        initial send rate in MB/s (CC adjusts from here)
```
### Receiver
```bash
  Usage hpuft-receiver:
  -listen string
        UDP address to listen on (e.g., :9000) (default ":9000")
  -out string
        directory to write received files (default ".")
```