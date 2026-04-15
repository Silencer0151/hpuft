#!/usr/bin/env python3
"""
hpuft netem WAN test suite (Linux only)

Simulates three WAN conditions via `tc netem` on the loopback interface and
runs put -> ls -> get cycles for a 50 MB and a 500 MB file under each
condition. Transfers are timed and throughput is computed.

Network profiles (one-way delay applied to `lo`; loopback traffic passes
through the qdisc in both directions so the effective RTT is ~2x the
one-way delay):

    trans-atlantic   ~80 ms RTT   0.1 % loss
    trans-pacific   ~150 ms RTT   0.3 % loss
    satellite       ~600 ms RTT   1.0 % loss

Usage:
    sudo python3 tests/test_hpuft_netem.py [options]

Options:
    --addr ADDR       serve daemon address (default: 127.0.0.1:9001)
    --exe PATH        path to hpuft binary (auto-detected)
    --no-build        skip building the binary
    --iface NAME      interface to apply netem to (default: lo)
    --profile NAME    run only the named profile (repeatable)
    --size MB         run only one file size in MB (repeatable)
    --report PATH     append a JSON results record to this file
    --keep            keep generated test files after the run

Requires: Linux, iproute2 (tc), root (netem rules need CAP_NET_ADMIN).

Exit code: 0 if all transfers pass, 1 otherwise.
"""

import argparse
import hashlib
import json
import os
import platform
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

# ── ANSI colours ──────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

# ── Profiles ──────────────────────────────────────────────────────────────────
# (name, one-way-delay-ms, jitter-ms, loss-pct)
PROFILES = [
    ("trans-atlantic",  40,  2, 0.1),
    ("trans-pacific",   75,  5, 0.3),
    ("satellite",      300, 10, 1.0),
]

# (size_mb, label) — run in series per profile
FILE_SIZES = [
    ( 50, "50MB"),
    (500, "500MB"),
]

# Per-size timeouts. Generous enough for satellite conditions.
TIMEOUTS = {
    50:  600,    # 10 min
    500: 1800,   # 30 min
}

RESULTS: list = []


# ══════════════════════════════════════════════════════════════════════════════
# Utilities
# ══════════════════════════════════════════════════════════════════════════════

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def gen_file(path: Path, size_bytes: int, chunk: int = 8 * 1024 * 1024) -> None:
    remaining = size_bytes
    with open(path, "wb") as f:
        while remaining > 0:
            n = min(chunk, remaining)
            f.write(os.urandom(n))
            remaining -= n


def run_cmd(cmd: list, timeout: int = 30, cwd: Path = None) -> tuple:
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        cwd=str(cwd) if cwd else None,
    )
    return (
        result.returncode,
        result.stdout.decode(errors="replace"),
        result.stderr.decode(errors="replace"),
    )


def _last_line(text: str, max_chars: int = 120) -> str:
    lines = [l for l in text.strip().splitlines() if l.strip()]
    return lines[-1][:max_chars] if lines else ""


def human_rate(mb: float, seconds: float) -> str:
    if seconds is None or seconds <= 0:
        return "—"
    rate = mb / seconds
    return f"{rate:7.2f} MB/s"


def human_time(seconds) -> str:
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds:6.2f}s"
    return f"{int(seconds // 60):2d}m{seconds % 60:05.2f}s"


def ls_filenames(exe: str, serve_addr: str) -> tuple:
    """
    Run `ls` against the daemon and return (filenames, raw_output).
    Filenames are the first whitespace-separated token per line, skipping any
    lines that begin with '(' (e.g. '(no files available)').
    Returns (None, err) on failure.
    """
    rc, out, err = run_cmd([exe, "ls", "-addr", serve_addr], timeout=15)
    if rc != 0:
        return None, (err or out).strip()
    names = set()
    for line in out.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("("):
            continue
        toks = stripped.split()
        if toks:
            names.add(toks[0])
    return names, out


def wait_for_finalization(exe: str, serve_addr: str, fname: str,
                          timeout: float = 120.0,
                          interval: float = 1.0) -> tuple:
    """
    Poll `ls` until `fname` appears as a promoted file (not the `.tmp`
    staging name). Returns (found, elapsed_seconds).
    """
    start    = time.monotonic()
    deadline = start + timeout
    while time.monotonic() < deadline:
        names, _ = ls_filenames(exe, serve_addr)
        if names is not None and fname in names:
            return True, time.monotonic() - start
        time.sleep(interval)
    return False, time.monotonic() - start


# ══════════════════════════════════════════════════════════════════════════════
# netem control
# ══════════════════════════════════════════════════════════════════════════════

def netem_clear(iface: str) -> None:
    # Ignore failure — may be no qdisc applied.
    subprocess.run(
        ["tc", "qdisc", "del", "dev", iface, "root"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def netem_apply(iface: str, delay_ms: int, jitter_ms: int, loss_pct: float) -> None:
    netem_clear(iface)
    cmd = [
        "tc", "qdisc", "add", "dev", iface, "root", "netem",
        "delay", f"{delay_ms}ms", f"{jitter_ms}ms",
        "loss", f"{loss_pct}%",
    ]
    rc, _, err = run_cmd(cmd, timeout=10)
    if rc != 0:
        raise RuntimeError(f"tc netem apply failed: {err.strip()}")


def netem_show(iface: str) -> str:
    rc, out, _ = run_cmd(["tc", "qdisc", "show", "dev", iface], timeout=5)
    return out.strip() if rc == 0 else ""


# ══════════════════════════════════════════════════════════════════════════════
# serve daemon helper
# ══════════════════════════════════════════════════════════════════════════════

class ServeProc:
    def __init__(self, cmd: list):
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._lines: list = []
        for stream in (self.proc.stderr, self.proc.stdout):
            t = threading.Thread(target=self._reader, args=(stream,), daemon=True)
            t.start()

    def _reader(self, stream) -> None:
        for raw in stream:
            self._lines.append(raw.decode(errors="replace").rstrip())

    def wait_for(self, pattern: str, timeout: float = 6.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if any(pattern in l for l in self._lines):
                return True
            if self.proc.poll() is not None:
                return False
            time.sleep(0.05)
        return False

    def kill(self) -> None:
        if self.proc.poll() is None:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=3)
            except Exception:
                self.proc.kill()


# ══════════════════════════════════════════════════════════════════════════════
# One transfer cycle
# ══════════════════════════════════════════════════════════════════════════════

def run_cycle(exe: str, test_dir: Path, serve_addr: str,
              size_mb: int, size_label: str, profile_name: str) -> dict:
    """
    put -> ls -> get -> rm for one file. Returns a result dict.
    """
    size_bytes = size_mb * 1024 * 1024
    timeout    = TIMEOUTS.get(size_mb, 1800)

    src_name = f"{profile_name}_{size_label}_{int(time.time())}.bin"
    src      = test_dir / src_name
    get_dir  = test_dir / f"get_{profile_name}_{size_label}"
    get_dir.mkdir(exist_ok=True)

    result = {
        "profile":      profile_name,
        "size_mb":      size_mb,
        "size_label":   size_label,
        "put_ok":       False,
        "ls_ok":        False,
        "get_ok":       False,
        "rm_ok":        False,
        "put_sec":      None,
        "get_sec":      None,
        "finalize_sec": None,
        "put_mbps":     None,
        "get_mbps":     None,
        "hash_match":   False,
        "error":        "",
    }

    print(f"  generating {size_label} file ({size_mb} MB) …", end=" ", flush=True)
    gen_file(src, size_bytes)
    src_hash = sha256(src)
    print("done")

    # put ---------------------------------------------------------------------
    t0 = time.monotonic()
    rc, out, err = run_cmd(
        [exe, "put", "-addr", serve_addr, "-file", str(src)],
        timeout=timeout,
    )
    elapsed = time.monotonic() - t0
    if rc == 0:
        result["put_ok"]   = True
        result["put_sec"]  = elapsed
        result["put_mbps"] = size_mb / elapsed if elapsed > 0 else None
        print(f"  {GREEN}[PASS]{RESET} put    "
              f"— {human_time(elapsed)}  {human_rate(size_mb, elapsed)}")
    else:
        result["error"] = f"put: {_last_line(err or out)}"
        print(f"  {RED}[FAIL]{RESET} put    — {result['error']}")
        return result

    # ls ----------------------------------------------------------------------
    # The client-side `put` returns when the sender finishes transmitting.
    # The server still has to drain its receive channel, verify the hash,
    # rename the .tmp to its final name, add it to the manifest, and release
    # its busy lock. Under loss + high RTT that wall-clock lag is visible,
    # so poll ls until the *promoted* filename appears.
    print(f"  {YELLOW}[...]{RESET}  waiting for server-side finalization "
          f"(hash verify + promote) …", flush=True)
    found, wait_sec = wait_for_finalization(
        exe, serve_addr, src.name, timeout=180.0, interval=1.0,
    )
    result["finalize_sec"] = wait_sec
    if found:
        result["ls_ok"] = True
        print(f"  {GREEN}[PASS]{RESET} ls     — promoted in "
              f"{wait_sec:5.2f}s after put")
    else:
        result["error"] = f"ls: file never appeared within {wait_sec:.0f}s"
        print(f"  {RED}[FAIL]{RESET} ls     — {result['error']}")
        return result

    # get ---------------------------------------------------------------------
    t0 = time.monotonic()
    rc, out, err = run_cmd(
        [exe, "get", "-addr", serve_addr, "-file", src.name,
         "-out", str(get_dir)],
        timeout=timeout,
    )
    elapsed = time.monotonic() - t0
    if rc == 0:
        result["get_sec"]  = elapsed
        result["get_mbps"] = size_mb / elapsed if elapsed > 0 else None
        dest = get_dir / src.name
        if dest.exists() and sha256(dest) == src_hash:
            result["get_ok"] = True
            result["hash_match"] = True
            print(f"  {GREEN}[PASS]{RESET} get    "
                  f"— {human_time(elapsed)}  {human_rate(size_mb, elapsed)}  hash match")
        else:
            result["error"] = "get: hash mismatch or missing file"
            print(f"  {RED}[FAIL]{RESET} get    — {result['error']}")
    else:
        result["error"] = f"get: {_last_line(err or out)}"
        print(f"  {RED}[FAIL]{RESET} get    — {result['error']}")

    # rm ----------------------------------------------------------------------
    rc, out, err = run_cmd(
        [exe, "rm", "-addr", serve_addr, "-file", src.name],
        timeout=15,
    )
    if rc == 0:
        result["rm_ok"] = True
        print(f"  {GREEN}[PASS]{RESET} rm     — ok")
    else:
        print(f"  {RED}[FAIL]{RESET} rm     — {_last_line(err or out)}")

    # Free disk: drop the source and download immediately.
    try:
        src.unlink()
        shutil.rmtree(get_dir, ignore_errors=True)
    except Exception:
        pass

    return result


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="hpuft netem WAN test suite (Linux only)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--addr",     default="127.0.0.1:9001")
    parser.add_argument("--exe",      default="")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--iface",    default="lo")
    parser.add_argument("--profile",  action="append", default=[],
                        help="run only named profile (repeatable)")
    parser.add_argument("--size",     action="append", type=int, default=[],
                        help="run only this file size in MB (repeatable)")
    parser.add_argument("--report",   default="",
                        help="append JSON record of results to this path")
    parser.add_argument("--keep",     action="store_true")
    args = parser.parse_args()

    # ── Guardrails ────────────────────────────────────────────────────────────
    if platform.system() != "Linux":
        print(f"{RED}This test requires Linux (tc netem).{RESET}")
        sys.exit(1)
    if os.geteuid() != 0:
        print(f"{RED}This test requires root to apply tc netem qdiscs.{RESET}")
        print(f"Re-run with: sudo python3 {sys.argv[0]}")
        sys.exit(1)
    if shutil.which("tc") is None:
        print(f"{RED}`tc` not found. Install iproute2.{RESET}")
        sys.exit(1)

    selected_profiles = PROFILES
    if args.profile:
        wanted = set(args.profile)
        selected_profiles = [p for p in PROFILES if p[0] in wanted]
        if not selected_profiles:
            print(f"{RED}No profiles match {wanted}. Known: "
                  f"{[p[0] for p in PROFILES]}{RESET}")
            sys.exit(1)

    selected_sizes = FILE_SIZES
    if args.size:
        wanted = set(args.size)
        selected_sizes = [s for s in FILE_SIZES if s[0] in wanted]
        if not selected_sizes:
            print(f"{RED}No sizes match {wanted}. Known: "
                  f"{[s[0] for s in FILE_SIZES]}{RESET}")
            sys.exit(1)

    repo_root = Path(__file__).resolve().parent.parent
    exe       = str(Path(args.exe).resolve()) if args.exe else str(repo_root / "hpuft")

    # ── Build ─────────────────────────────────────────────────────────────────
    if not args.no_build:
        print(f"{YELLOW}[build]{RESET} go build -o {exe} ./cmd/hpuft ...")
        rc, _, err = run_cmd(["go", "build", "-o", exe, "./cmd/hpuft"],
                             timeout=180, cwd=repo_root)
        if rc != 0:
            print(f"{RED}[build] FAILED{RESET}\n{err}")
            sys.exit(1)
        print(f"{GREEN}[build] OK{RESET}")
    elif not Path(exe).exists():
        print(f"{RED}[error]{RESET} binary not found at {exe}. Run without --no-build.")
        sys.exit(1)

    # ── Prepare dirs ──────────────────────────────────────────────────────────
    test_dir  = repo_root / "received" / "netem_test_files"
    serve_dir = test_dir / "serve_root"
    if test_dir.exists():
        shutil.rmtree(test_dir, ignore_errors=True)
    test_dir.mkdir(parents=True, exist_ok=True)
    serve_dir.mkdir(exist_ok=True)

    port = args.addr.split(":")[-1]

    # ── Install signal handler to guarantee netem cleanup ─────────────────────
    def cleanup_netem(*_):
        netem_clear(args.iface)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda s, f: (cleanup_netem(), sys.exit(130)))

    print()
    print("=" * 70)
    print(f"  {BOLD}hpuft netem WAN test suite{RESET}")
    print(f"  binary     : {Path(exe).name}")
    print(f"  serve addr : {args.addr}")
    print(f"  interface  : {args.iface}")
    print(f"  profiles   : {[p[0] for p in selected_profiles]}")
    print(f"  file sizes : {[s[1] for s in selected_sizes]}")
    print("=" * 70)

    all_results: list = []

    try:
        for profile in selected_profiles:
            pname, delay_ms, jitter_ms, loss_pct = profile
            rtt_ms = delay_ms * 2

            print(f"\n{CYAN}── profile: {pname}"
                  f"  ({rtt_ms}ms RTT, ±{jitter_ms}ms jitter, {loss_pct}% loss)"
                  f" {'─' * max(0, 24 - len(pname))}{RESET}")

            # Wipe any stragglers from the previous profile so the daemon's
            # rebuilt manifest starts empty.
            shutil.rmtree(serve_dir, ignore_errors=True)
            serve_dir.mkdir(exist_ok=True)

            # Apply netem before starting the daemon so the daemon's socket
            # binds to the already-impaired loopback.
            try:
                netem_apply(args.iface, delay_ms, jitter_ms, loss_pct)
            except RuntimeError as exc:
                print(f"  {RED}[FAIL]{RESET} netem apply — {exc}")
                continue
            print(f"  {YELLOW}[netem]{RESET} {netem_show(args.iface)}")

            srv = ServeProc([exe, "serve",
                             "-listen", f":{port}",
                             "-dir", str(serve_dir)])

            try:
                if not srv.wait_for("[serve] Online", timeout=10.0):
                    print(f"  {RED}[FAIL]{RESET} serve daemon did not start")
                    continue

                for size_mb, size_label in selected_sizes:
                    print(f"\n  {BOLD}{size_label} transfer{RESET}")
                    result = run_cycle(exe, test_dir, args.addr,
                                       size_mb, size_label, pname)
                    result["rtt_ms"]     = rtt_ms
                    result["jitter_ms"]  = jitter_ms
                    result["loss_pct"]   = loss_pct
                    all_results.append(result)
            finally:
                srv.kill()
                netem_clear(args.iface)

    finally:
        netem_clear(args.iface)
        if not args.keep:
            shutil.rmtree(test_dir, ignore_errors=True)

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("=" * 70)
    print(f"  {BOLD}Summary{RESET}")
    print("=" * 70)
    print(f"  {'profile':<16} {'size':>7} {'put time':>12} {'put rate':>12} "
          f"{'finalize':>10} {'get time':>12} {'get rate':>12}  result")
    print(f"  {'-'*16} {'-'*7} {'-'*12} {'-'*12} {'-'*10} {'-'*12} {'-'*12}  {'-'*6}")

    failed = 0
    for r in all_results:
        ok = r["put_ok"] and r["ls_ok"] and r["get_ok"] and r["hash_match"]
        if not ok:
            failed += 1
        tag   = f"{GREEN}PASS{RESET}" if ok else f"{RED}FAIL{RESET}"
        put_t = human_time(r["put_sec"])
        put_r = human_rate(r["size_mb"], r["put_sec"])
        fin_t = human_time(r["finalize_sec"])
        get_t = human_time(r["get_sec"])
        get_r = human_rate(r["size_mb"], r["get_sec"])
        print(f"  {r['profile']:<16} {r['size_label']:>7} "
              f"{put_t:>12} {put_r:>12} {fin_t:>10} "
              f"{get_t:>12} {get_r:>12}  {tag}")

    total = len(all_results)
    colour = GREEN if failed == 0 else RED
    print()
    print(f"  {colour}{total - failed}/{total} transfers passed{RESET}")
    print("=" * 70)
    print()

    # ── Optional JSON report ──────────────────────────────────────────────────
    if args.report:
        record = {
            "timestamp":  time.strftime("%Y-%m-%dT%H:%M:%S"),
            "binary":     Path(exe).name,
            "iface":      args.iface,
            "results":    all_results,
        }
        try:
            with open(args.report, "a") as f:
                f.write(json.dumps(record) + "\n")
            print(f"  {YELLOW}[report]{RESET} appended to {args.report}")
        except Exception as exc:
            print(f"  {RED}[report] failed to write:{RESET} {exc}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
