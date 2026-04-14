#!/usr/bin/env python3
"""
hpuft integration test suite
Tests: serve, put, ls, get, rm

Works on Windows, Linux, and macOS.

Usage:
    python tests/test_hpuft.py [options]

Options:
    --addr ADDR        Serve daemon address      (default: 127.0.0.1:9001)
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

def test_serve_commands(exe: str, test_dir: Path, serve_addr: str) -> None:
    """
    Serve daemon integration: put → ls → get → rm

    Flow:
      1. Start `serve` daemon pointing at an empty directory.
      2. `ls`   → expect "(no files available)" or empty listing.
      3. `put`  a file.
      4. `ls`   → expect the filename to appear.
      5. `get`  the file back, verify SHA-256.
      6. `rm`   the file from the server.
      7. `ls`   → expect the file is gone.
    """
    serve_dir = test_dir / "serve_root"
    get_dir   = test_dir / "out_get"
    serve_dir.mkdir(exist_ok=True)
    get_dir.mkdir(exist_ok=True)

    port = serve_addr.split(":")[-1]
    srv  = BackgroundProc([exe, "serve", "-listen", f":{port}", "-dir", str(serve_dir)])

    if not srv.wait_for("[serve] Online", timeout=6.0):
        srv.kill()
        for name in ("serve", "ls (empty)", "put", "ls (after put)", "get", "rm", "ls (after rm)"):
            report(name, False, "serve daemon did not start")
        return

    report("serve", True, f"daemon listening on :{port}")

    try:
        # ── ls (empty) ────────────────────────────────────────────────────────
        rc, out, err = run_cmd([exe, "ls", "-addr", serve_addr], timeout=10)
        ls_empty_ok = rc == 0
        detail = out.strip()[:80] if ls_empty_ok else err.strip()[:80]
        report("ls (empty)", ls_empty_ok, detail or "(no output)")

        # ── put ───────────────────────────────────────────────────────────────
        src      = test_dir / f"put_{ts()}.bin"
        src.write_bytes(os.urandom(128 * 1024))   # 128 KB
        src_hash = sha256(src)

        rc, out, err = run_cmd(
            [exe, "put", "-addr", serve_addr, "-file", str(src), "-debug"],
            timeout=30,
        )
        put_ok = rc == 0
        report("put", put_ok,
               "ok" if put_ok else err.strip().splitlines()[-1][:120] if err.strip() else f"exit {rc}")

        # ── ls (after put) ────────────────────────────────────────────────────
        rc, out, err = run_cmd([exe, "ls", "-addr", serve_addr], timeout=10)
        found = rc == 0 and src.name in out
        report(
            "ls (after put)", found,
            f"found '{src.name}'" if found else f"output: {out.strip()[:80] or err.strip()[:80]}",
        )

        # ── get ───────────────────────────────────────────────────────────────
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

        # ── rm ────────────────────────────────────────────────────────────────
        rc, out, err = run_cmd(
            [exe, "rm", "-addr", serve_addr, "-file", src.name],
            timeout=10,
        )
        rm_ok = rc == 0
        report("rm", rm_ok,
               "ok" if rm_ok else err.strip().splitlines()[-1][:120] if err.strip() else f"exit {rc}")

        # ── ls (after rm) ─────────────────────────────────────────────────────
        rc, out, err = run_cmd([exe, "ls", "-addr", serve_addr], timeout=10)
        gone = rc == 0 and src.name not in out
        report(
            "ls (after rm)", gone,
            f"'{src.name}' not present" if gone else f"file still listed: {out.strip()[:80]}",
        )

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
    print("=" * 60)
    print()

    try:
        # ── serve / put / ls / get / rm ───────────────────────────────────────
        print(f"{CYAN}── serve / put / ls / get / rm {'─' * 26}{RESET}")
        test_serve_commands(exe, test_dir, args.addr)

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
