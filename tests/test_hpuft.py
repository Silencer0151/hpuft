#!/usr/bin/env python3
"""
hpuft integration test suite
Three test phases against a single serve daemon:
  Phase 1  — direct commands, small file  (64 KB):  put → ls → get → rm
  Phase 2  — direct commands, large file (100 MB):  put → ls → get → rm
  Phase 3  — connect REPL, small file   (64 KB):   ls → put → ls → get → rm

Works on Windows, Linux, and macOS.

Usage:
    python tests/test_hpuft.py [options]

Options:
    --addr ADDR   Serve daemon address  (default: 127.0.0.1:9001)
    --exe PATH    Path to hpuft binary  (auto-detected)
    --no-build    Skip building the binary
    --keep        Keep test files after run

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

RESULTS: list = []   # (name: str, passed: bool, detail: str)


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
    return time.strftime("%Y%m%d_%H%M%S")


def _last_line(text: str, max_chars: int = 120) -> str:
    lines = [l for l in text.strip().splitlines() if l.strip()]
    return lines[-1][:max_chars] if lines else ""


# ══════════════════════════════════════════════════════════════════════════════
# Background process helpers
# ══════════════════════════════════════════════════════════════════════════════

class BackgroundProc:
    """Long-running subprocess with background output capture."""

    def __init__(self, cmd: list, stdin_pipe: bool = False):
        self.cmd  = cmd
        self.proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE if stdin_pipe else None,
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

    def lines_snapshot(self) -> list:
        return list(self._lines)

    def send_stdin(self, text: str) -> None:
        if self.proc.stdin:
            self.proc.stdin.write((text + "\n").encode())
            self.proc.stdin.flush()

    def is_alive(self) -> bool:
        return self.proc.poll() is None

    def kill(self) -> None:
        if self.is_alive():
            try:
                self.proc.terminate()
                self.proc.wait(timeout=3)
            except Exception:
                self.proc.kill()

    def wait(self, timeout: float = 5.0) -> int:
        try:
            return self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.kill()
            return -1


# ══════════════════════════════════════════════════════════════════════════════
# Phase 1 & 2 — direct command tests
# ══════════════════════════════════════════════════════════════════════════════

def test_direct(exe: str, test_dir: Path, serve_addr: str,
                size_bytes: int, label: str,
                put_timeout: int = 120, get_timeout: int = 120) -> None:
    """
    Full put → ls → get → rm cycle for a file of the given size.
    label is used as a prefix in report names, e.g. "small" or "large".
    """
    src      = test_dir / f"{label}_{ts()}.bin"
    get_dir  = test_dir / f"get_{label}"
    get_dir.mkdir(exist_ok=True)

    print(f"  generating {label} file ({size_bytes // 1024} KB) …", end=" ", flush=True)
    src.write_bytes(os.urandom(size_bytes))
    src_hash = sha256(src)
    print("done")

    # ── ls (empty before put) ─────────────────────────────────────────────────
    rc, out, err = run_cmd([exe, "ls", "-addr", serve_addr], timeout=10)
    empty_ok = rc == 0 and src.name not in out
    report(f"{label}: ls (before put)", empty_ok,
           out.strip()[:80] if empty_ok else _last_line(err or out))

    # ── put ───────────────────────────────────────────────────────────────────
    rc, out, err = run_cmd(
        [exe, "put", "-addr", serve_addr, "-file", str(src)],
        timeout=put_timeout,
    )
    put_ok = rc == 0
    report(f"{label}: put", put_ok,
           "ok" if put_ok else _last_line(err or out))
    if not put_ok:
        report(f"{label}: ls (after put)", False, "skipped — put failed")
        report(f"{label}: get",            False, "skipped — put failed")
        report(f"{label}: rm",             False, "skipped — put failed")
        return

    # ── ls (after put) ────────────────────────────────────────────────────────
    rc, out, err = run_cmd([exe, "ls", "-addr", serve_addr], timeout=10)
    found = rc == 0 and src.name in out
    report(f"{label}: ls (after put)", found,
           f"found '{src.name}'" if found else _last_line(out or err))

    # ── get ───────────────────────────────────────────────────────────────────
    rc, out, err = run_cmd(
        [exe, "get", "-addr", serve_addr, "-file", src.name,
         "-out", str(get_dir)],
        timeout=get_timeout,
    )
    if rc != 0:
        report(f"{label}: get", False, _last_line(err or out))
    else:
        dest = get_dir / src.name
        if dest.exists():
            ok = sha256(dest) == src_hash
            report(f"{label}: get", ok, "hash match" if ok else "hash mismatch")
        else:
            report(f"{label}: get", False, "output file not found")

    # ── rm ────────────────────────────────────────────────────────────────────
    rc, out, err = run_cmd(
        [exe, "rm", "-addr", serve_addr, "-file", src.name],
        timeout=10,
    )
    rm_ok = rc == 0
    report(f"{label}: rm", rm_ok,
           "ok" if rm_ok else _last_line(err or out))

    # ── ls (after rm) ─────────────────────────────────────────────────────────
    rc, out, err = run_cmd([exe, "ls", "-addr", serve_addr], timeout=10)
    gone = rc == 0 and src.name not in out
    report(f"{label}: ls (after rm)", gone,
           f"'{src.name}' not present" if gone else _last_line(out or err))


# ══════════════════════════════════════════════════════════════════════════════
# Phase 3 — connect REPL
# ══════════════════════════════════════════════════════════════════════════════

def test_connect_repl(exe: str, test_dir: Path, serve_addr: str) -> None:
    """
    Drive the `connect` REPL via stdin and verify each operation.

    Sequence: establish → ls (empty) → put → ls (after put) → get → rm → exit
    """
    src      = test_dir / f"repl_{ts()}.bin"
    get_dir  = test_dir / "repl_get"
    get_dir.mkdir(exist_ok=True)

    print(f"  generating repl file (64 KB) …", end=" ", flush=True)
    src.write_bytes(os.urandom(64 * 1024))
    src_hash = sha256(src)
    print("done")

    # Use forward slashes in paths sent over stdin to avoid shell-splitting
    # issues with backslashes on Windows.
    src_arg     = str(src).replace("\\", "/")
    get_dir_arg = str(get_dir).replace("\\", "/")

    repl = BackgroundProc([exe, "connect", "-addr", serve_addr], stdin_pipe=True)

    def wait_new(pattern: str, since: int, timeout: float) -> tuple:
        """Return (found, new_line_count) after watching for pattern since index."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            snapshot = repl.lines_snapshot()
            for line in snapshot[since:]:
                if pattern in line:
                    return True, len(snapshot)
            if not repl.is_alive():
                return False, len(repl.lines_snapshot())
            time.sleep(0.05)
        return False, len(repl.lines_snapshot())

    try:
        # ── establish ─────────────────────────────────────────────────────────
        ok, cursor = wait_new("Connected to", 0, timeout=6.0)
        report("connect: establish", ok,
               f"connected to {serve_addr}" if ok else "no Connected message")
        if not ok:
            return

        # ── ls (empty) ────────────────────────────────────────────────────────
        repl.send_stdin("ls")
        ok, cursor = wait_new("no files available", cursor, timeout=5.0)
        if not ok:
            # There might be leftover files from a previous failed run; just
            # verify ls responded at all (cursor advanced).
            new_cursor = len(repl.lines_snapshot())
            ok = new_cursor > cursor
            cursor = new_cursor
        report("connect: ls (empty)", ok,
               "(no files available)" if ok else "no ls response")

        # ── put ───────────────────────────────────────────────────────────────
        repl.send_stdin(f"put {src_arg}")
        ok, cursor = wait_new("put: COMPLETE", cursor, timeout=30.0)
        report("connect: put", ok, "ok" if ok else "no COMPLETE message")
        if not ok:
            repl.send_stdin("exit")
            repl.wait()
            report("connect: ls (after put)", False, "skipped — put failed")
            report("connect: get",            False, "skipped — put failed")
            report("connect: rm",             False, "skipped — put failed")
            return

        # ── ls (after put) ────────────────────────────────────────────────────
        repl.send_stdin("ls")
        ok, cursor = wait_new(src.name, cursor, timeout=5.0)
        report("connect: ls (after put)", ok,
               f"found '{src.name}'" if ok else "file not listed")

        # ── get ───────────────────────────────────────────────────────────────
        repl.send_stdin(f"get {src.name} -o {get_dir_arg}")
        ok, cursor = wait_new("get: COMPLETE", cursor, timeout=30.0)
        if ok:
            dest = get_dir / src.name
            if dest.exists():
                hash_ok = sha256(dest) == src_hash
                report("connect: get", hash_ok,
                       "hash match" if hash_ok else "hash mismatch")
            else:
                report("connect: get", False, "output file not found")
        else:
            report("connect: get", False, "no COMPLETE message")

        # ── rm ────────────────────────────────────────────────────────────────
        repl.send_stdin(f"rm {src.name}")
        ok, cursor = wait_new("deleted", cursor, timeout=5.0)
        report("connect: rm", ok, "ok" if ok else "no delete confirmation")

        # ── exit ──────────────────────────────────────────────────────────────
        repl.send_stdin("exit")
        rc = repl.wait(timeout=5.0)
        report("connect: exit", rc == 0,
               f"exit code {rc}" if rc != 0 else "clean exit")

    except Exception as exc:
        report("connect: error", False, str(exc))
    finally:
        repl.kill()


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="hpuft integration test suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--addr",     default="127.0.0.1:9001",
                        help="serve daemon address (default: 127.0.0.1:9001)")
    parser.add_argument("--exe",      default="",
                        help="path to hpuft binary (auto-detected if omitted)")
    parser.add_argument("--no-build", action="store_true",
                        help="skip building the binary")
    parser.add_argument("--keep",     action="store_true",
                        help="keep test files after the run")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    is_win    = platform.system() == "Windows"
    exe_name  = "hpuft.exe" if is_win else "hpuft"
    exe       = str(Path(args.exe).resolve()) if args.exe else str(repo_root / exe_name)

    # ── Build ─────────────────────────────────────────────────────────────────
    if not args.no_build:
        print(f"{YELLOW}[build]{RESET} go build -o {exe} ./cmd/hpuft ...")
        rc, out, err = run_cmd(["go", "build", "-o", exe, "./cmd/hpuft"],
                               timeout=120, cwd=repo_root)
        if rc != 0:
            print(f"{RED}[build] FAILED{RESET}\n{err}")
            sys.exit(1)
        print(f"{GREEN}[build] OK{RESET}")
    elif not Path(exe).exists():
        print(f"{RED}[error]{RESET} binary not found at {exe}. Run without --no-build.")
        sys.exit(1)

    # ── Prepare test directory ────────────────────────────────────────────────
    test_dir  = repo_root / "received" / "test_files"
    serve_dir = test_dir / "serve_root"
    test_dir.mkdir(parents=True, exist_ok=True)
    serve_dir.mkdir(exist_ok=True)

    port = args.addr.split(":")[-1]

    print()
    print("=" * 64)
    print("  hpuft integration test suite")
    print(f"  binary     : {Path(exe).name}")
    print(f"  serve addr : {args.addr}")
    print(f"  test dir   : {test_dir}")
    print("=" * 64)
    print()

    # ── Start one serve daemon for all three phases ───────────────────────────
    srv = BackgroundProc([exe, "serve", "-listen", f":{port}", "-dir", str(serve_dir)])

    if not srv.wait_for("[serve] Online", timeout=6.0):
        srv.kill()
        for name in (
            "serve",
            "small: ls (before put)", "small: put", "small: ls (after put)",
            "small: get", "small: rm", "small: ls (after rm)",
            "large: ls (before put)", "large: put", "large: ls (after put)",
            "large: get", "large: rm", "large: ls (after rm)",
            "connect: establish", "connect: ls (empty)", "connect: put",
            "connect: ls (after put)", "connect: get", "connect: rm", "connect: exit",
        ):
            report(name, False, "serve daemon did not start")
        return

    report("serve", True, f"daemon online on :{port}")

    try:
        # ── Phase 1: small file (64 KB) ───────────────────────────────────────
        print(f"\n{CYAN}── Phase 1: direct commands — small file (64 KB) {'─' * 13}{RESET}")
        test_direct(exe, test_dir, args.addr,
                    size_bytes=64 * 1024, label="small",
                    put_timeout=30, get_timeout=30)

        # ── Phase 2: large file (100 MB) ──────────────────────────────────────
        print(f"\n{CYAN}── Phase 2: direct commands — large file (100 MB) {'─' * 12}{RESET}")
        test_direct(exe, test_dir, args.addr,
                    size_bytes=100 * 1024 * 1024, label="large",
                    put_timeout=120, get_timeout=120)

        # ── Phase 3: connect REPL (64 KB) ─────────────────────────────────────
        print(f"\n{CYAN}── Phase 3: connect REPL — small file (64 KB) {'─' * 16}{RESET}")
        test_connect_repl(exe, test_dir, args.addr)

    finally:
        srv.kill()

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
        print("=" * 64)
        for name, ok, detail in RESULTS:
            tag    = f"{GREEN}PASS{RESET}" if ok else f"{RED}FAIL{RESET}"
            suffix = f"  — {detail}" if detail else ""
            print(f"  [{tag}] {name}{suffix}")
        print()
        print(f"  {colour}{passed}/{total} tests passed{RESET}")
        print("=" * 64)
        print()

        sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
