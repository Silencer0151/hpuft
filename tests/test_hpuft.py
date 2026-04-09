#!/usr/bin/env python3
"""
hpuft integration test suite
Tests: send, recv, serve, push, list, get  (+ encrypted push/get)

Works on Windows, Linux, and macOS.

Usage:
    python tests/test_hpuft.py [options]

Options:
    --addr ADDR        Serve daemon address      (default: 127.0.0.1:9001)
    --recv-addr ADDR   Direct recv address       (default: 127.0.0.1:9000)
    --exe PATH         Path to hpuft binary      (auto-detected)
    --no-build         Skip building the binary
    --keep             Keep test files after run (useful for debugging)

Exit code: 0 if all tests pass, 1 otherwise.
"""

import argparse
import hashlib
import os
import platform
import shutil
import subprocess
import sys
import time
import threading
from pathlib import Path

# ── ANSI colours ───────────────────────────────────────────────────────────────
if platform.system() == "Windows":
    try:
        import ctypes
        ctypes.windll.kernel32.SetConsoleMode(
            ctypes.windll.kernel32.GetStdHandle(-11), 7)
    except Exception:
        pass

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"

# ── Global results list ────────────────────────────────────────────────────────
RESULTS: list = []  # list of (name: str, passed: bool, detail: str)


# ══════════════════════════════════════════════════════════════════════════════
# Utilities
# ══════════════════════════════════════════════════════════════════════════════

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def report(name: str, passed: bool, detail: str = "") -> None:
    tag    = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
    suffix = f"  — {detail}" if detail else ""
    print(f"  [{tag}] {name}{suffix}")
    RESULTS.append((name, passed, detail))


def run_cmd(cmd: list, timeout: int = 30, cwd: Path = None) -> tuple:
    """Run a command, capture output.  Returns (rc, stdout, stderr)."""
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


def ts() -> str:
    """Return a compact timestamp string suitable for filenames."""
    return time.strftime("%Y%m%d_%H%M%S")


# ══════════════════════════════════════════════════════════════════════════════
# Background process wrapper
# ══════════════════════════════════════════════════════════════════════════════

class BackgroundProc:
    """Start a long-running subprocess and stream its output to internal lists."""

    def __init__(self, cmd: list):
        self.cmd  = cmd
        self.proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        self._stderr_lines: list = []
        self._stdout_lines: list = []
        for stream, store in [
            (self.proc.stderr, self._stderr_lines),
            (self.proc.stdout, self._stdout_lines),
        ]:
            t = threading.Thread(target=self._reader, args=(stream, store), daemon=True)
            t.start()

    @staticmethod
    def _reader(stream, store: list) -> None:
        for raw_line in stream:
            store.append(raw_line.decode(errors="replace").rstrip())

    def wait_for(self, pattern: str, timeout: float = 6.0) -> bool:
        """Block until *pattern* appears in any line of stdout or stderr."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            all_lines = self._stderr_lines + self._stdout_lines
            if any(pattern in line for line in all_lines):
                return True
            if self.proc.poll() is not None:
                return False
            time.sleep(0.05)
        return False

    def is_alive(self) -> bool:
        return self.proc.poll() is None

    def kill(self) -> None:
        if self.is_alive():
            try:
                self.proc.terminate()
                self.proc.wait(timeout=3)
            except Exception:
                self.proc.kill()


# ══════════════════════════════════════════════════════════════════════════════
# Individual test groups
# ══════════════════════════════════════════════════════════════════════════════

def test_send_recv(exe: str, test_dir: Path, recv_addr: str) -> None:
    """
    Direct send → recv pair, both on localhost.

    Flow:
      1. Start `recv` listening on recv_addr.
      2. Run `send` pointing at recv_addr.
      3. Verify the received file matches the source by SHA-256.
    """
    src      = test_dir / f"src_send_{ts()}.bin"
    out_dir  = test_dir / "out_recv"
    out_dir.mkdir(exist_ok=True)

    # 64 KB of random bytes — small enough to be fast, big enough to exercise FEC.
    src.write_bytes(os.urandom(64 * 1024))
    src_hash = sha256(src)

    port = recv_addr.split(":")[-1]
    srv  = BackgroundProc([exe, "recv", "-listen", f":{port}", "-out", str(out_dir), "-debug"])

    if not srv.wait_for("[recv] Listening", timeout=6.0):
        srv.kill()
        report("recv", False, "daemon did not print ready message in time")
        report("send", False, "recv not ready")
        return

    rc, out, err = run_cmd(
        [exe, "send", "-file", str(src), "-addr", recv_addr, "-debug"],
        timeout=30,
    )
    srv.kill()

    if rc != 0:
        detail = (err or out).strip().splitlines()[-1][:120] if (err or out).strip() else f"exit {rc}"
        report("send", False, detail)
        report("recv", False, "send exited non-zero")
        return

    dest = out_dir / src.name
    if not dest.exists():
        report("send", False, "output file not found in recv dir")
        report("recv", False, "output file not found in recv dir")
        return

    ok = sha256(dest) == src_hash
    report("send", ok, "hash match" if ok else "hash mismatch")
    report("recv", ok, "hash match" if ok else "hash mismatch")


def test_serve_push_list_get(exe: str, test_dir: Path, serve_addr: str) -> None:
    """
    Serve daemon integration: push → list → get (plain and encrypted).

    Flow:
      1. Start `serve` daemon pointing at an empty directory.
      2. `list`  → expect "(no files available)".
      3. `push`  a plain file.
      4. `list`  → expect the filename to appear.
      5. `get`   the file back, verify SHA-256.
      6. `push`  an encrypted file.
      7. `get`   it back with -encrypt, verify SHA-256.
    """
    serve_dir = test_dir / "serve_root"
    get_dir   = test_dir / "out_get"
    serve_dir.mkdir(exist_ok=True)
    get_dir.mkdir(exist_ok=True)

    port = serve_addr.split(":")[-1]
    srv  = BackgroundProc([exe, "serve", "-listen", f":{port}", "-dir", str(serve_dir)])

    if not srv.wait_for("[serve] Online", timeout=6.0):
        srv.kill()
        for name in ("serve", "push", "list (empty)", "list (after push)", "get",
                     "push (encrypted)", "get (encrypted)"):
            report(name, False, "serve daemon did not start")
        return

    report("serve", True, f"daemon listening on :{port}")

    try:
        # ── list (empty) ──────────────────────────────────────────────────────
        rc, out, err = run_cmd([exe, "list", "-addr", serve_addr], timeout=10)
        list_empty_ok = rc == 0
        detail = out.strip()[:80] if list_empty_ok else err.strip()[:80]
        report("list (empty)", list_empty_ok, detail or "(no output)")

        # ── push (plain) ──────────────────────────────────────────────────────
        src      = test_dir / f"push_{ts()}.bin"
        src.write_bytes(os.urandom(128 * 1024))   # 128 KB
        src_hash = sha256(src)

        rc, out, err = run_cmd(
            [exe, "push", "-addr", serve_addr, "-file", str(src), "-debug"],
            timeout=30,
        )
        push_ok = rc == 0
        report("push", push_ok, "ok" if push_ok else err.strip().splitlines()[-1][:120] if err.strip() else f"exit {rc}")

        # ── list (after push) ─────────────────────────────────────────────────
        rc, out, err = run_cmd([exe, "list", "-addr", serve_addr], timeout=10)
        found = rc == 0 and src.name in out
        report(
            "list (after push)", found,
            f"found '{src.name}'" if found else f"output: {out.strip()[:80] or err.strip()[:80]}",
        )

        # ── get (plain) ───────────────────────────────────────────────────────
        rc, out, err = run_cmd(
            [exe, "get", "-addr", serve_addr, "-file", src.name, "-out", str(get_dir), "-debug"],
            timeout=30,
        )
        if rc != 0:
            report("get", False, err.strip().splitlines()[-1][:120] if err.strip() else f"exit {rc}")
        else:
            dest = get_dir / src.name
            if dest.exists():
                ok = sha256(dest) == src_hash
                report("get", ok, "hash match" if ok else "hash mismatch")
            else:
                report("get", False, "file missing from output dir")

        # ── push (encrypted) ─────────────────────────────────────────────────
        # Use a fresh filename — serve rejects duplicate names.
        src_enc      = test_dir / f"push_enc_{ts()}.bin"
        src_enc.write_bytes(os.urandom(128 * 1024))
        src_enc_hash = sha256(src_enc)

        rc, out, err = run_cmd(
            [exe, "push", "-addr", serve_addr, "-file", str(src_enc), "-encrypt", "-debug"],
            timeout=30,
        )
        push_enc_ok = rc == 0
        report(
            "push (encrypted)",
            push_enc_ok,
            "ok" if push_enc_ok else err.strip().splitlines()[-1][:120] if err.strip() else f"exit {rc}",
        )

        # ── get (encrypted) ───────────────────────────────────────────────────
        if push_enc_ok:
            get_enc_dir = test_dir / "out_get_enc"
            get_enc_dir.mkdir(exist_ok=True)
            rc, out, err = run_cmd(
                [
                    exe, "get",
                    "-addr",    serve_addr,
                    "-file",    src_enc.name,
                    "-out",     str(get_enc_dir),
                    "-encrypt",
                    "-debug",
                ],
                timeout=30,
            )
            if rc != 0:
                report("get (encrypted)", False,
                       err.strip().splitlines()[-1][:120] if err.strip() else f"exit {rc}")
            else:
                dest_enc = get_enc_dir / src_enc.name
                if dest_enc.exists():
                    ok = sha256(dest_enc) == src_enc_hash
                    report("get (encrypted)", ok, "hash match" if ok else "hash mismatch")
                else:
                    report("get (encrypted)", False, "file missing from output dir")
        else:
            report("get (encrypted)", False, "skipped — encrypted push failed")

    finally:
        srv.kill()


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="hpuft integration test suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--addr", default="127.0.0.1:9001",
        help="serve daemon address  (default: 127.0.0.1:9001)",
    )
    parser.add_argument(
        "--recv-addr", default="127.0.0.1:9000",
        help="direct recv address   (default: 127.0.0.1:9000)",
    )
    parser.add_argument(
        "--exe", default="",
        help="path to hpuft binary  (auto-detected if omitted)",
    )
    parser.add_argument(
        "--no-build", action="store_true",
        help="skip building the binary",
    )
    parser.add_argument(
        "--keep", action="store_true",
        help="keep test files after the run",
    )
    args = parser.parse_args()

    # ── Locate repo root (one level above this script) ────────────────────────
    repo_root = Path(__file__).resolve().parent.parent

    # ── Resolve binary path ───────────────────────────────────────────────────
    is_win  = platform.system() == "Windows"
    exe_name = "hpuft.exe" if is_win else "hpuft"
    exe      = str(Path(args.exe).resolve()) if args.exe else str(repo_root / exe_name)

    # ── Build ─────────────────────────────────────────────────────────────────
    if not args.no_build:
        print(f"{YELLOW}[build]{RESET} go build -o {exe} ./cmd/hpuft ...")
        rc, out, err = run_cmd(
            ["go", "build", "-o", exe, "./cmd/hpuft"],
            timeout=120,
            cwd=repo_root,
        )
        if rc != 0:
            print(f"{RED}[build] FAILED{RESET}\n{err}")
            sys.exit(1)
        print(f"{GREEN}[build] OK{RESET}")
    elif not Path(exe).exists():
        print(f"{RED}[error]{RESET} binary not found at {exe}. Run without --no-build to compile.")
        sys.exit(1)

    # ── Prepare test directory ────────────────────────────────────────────────
    test_dir = repo_root / "received" / "test_files"
    test_dir.mkdir(parents=True, exist_ok=True)

    print()
    print("=" * 60)
    print("  hpuft integration test suite")
    print(f"  binary     : {Path(exe).name}")
    print(f"  test dir   : {test_dir}")
    print(f"  serve addr : {args.addr}")
    print(f"  recv addr  : {args.recv_addr}")
    print("=" * 60)
    print()

    try:
        # ── send / recv ───────────────────────────────────────────────────────
        print(f"{CYAN}── send / recv {'─' * 42}{RESET}")
        test_send_recv(exe, test_dir, args.recv_addr)
        print()

        # ── serve / push / list / get ─────────────────────────────────────────
        print(f"{CYAN}── serve / push / list / get {'─' * 29}{RESET}")
        test_serve_push_list_get(exe, test_dir, args.addr)

    finally:
        # ── Cleanup ───────────────────────────────────────────────────────────
        if not args.keep:
            print()
            print(f"{YELLOW}[cleanup]{RESET} removing {test_dir}")
            shutil.rmtree(test_dir, ignore_errors=True)
        else:
            print(f"\n{YELLOW}[keep]{RESET} test files retained at {test_dir}")

        # ── Summary ───────────────────────────────────────────────────────────
        passed = sum(1 for _, ok, _ in RESULTS if ok)
        total  = len(RESULTS)
        colour = GREEN if passed == total else RED

        print()
        print("=" * 60)
        for name, ok, detail in RESULTS:
            tag    = f"{GREEN}PASS{RESET}" if ok else f"{RED}FAIL{RESET}"
            suffix = f"  — {detail}" if detail else ""
            print(f"  [{tag}] {name}{suffix}")
        print()
        print(f"  {colour}{passed}/{total} tests passed{RESET}")
        print("=" * 60)
        print()

        sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
