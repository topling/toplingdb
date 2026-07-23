#!/usr/bin/env python3
"""RSS sampler: wrap a command, sample its RSS every second via /proc/statm.

Usage:
  python3 sample_rss.py --series <series_file> --log <log_file> -- <command...>

The series file receives:
  # start_epoch=<float>  page_size=<int>
  <epoch_float> <resident_pages>
  ...

Timing uses signal.setitimer(ITIMER_REAL) for kernel-level periodic callbacks
with zero cumulative drift.  The child process exit code is transparently
forwarded.
"""
import argparse
import os
import signal
import subprocess
import sys
import time


def find_child_pid(parent_pid: int) -> int | None:
    """Find the first child of *parent_pid* via /proc."""
    try:
        children = open(f"/proc/{parent_pid}/task/{parent_pid}/children").read().split()
        if children:
            return int(children[0])
    except (OSError, ValueError):
        pass
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Sample RSS while running a command")
    ap.add_argument("--series", required=True, help="Output file for RSS time series")
    ap.add_argument("--log", required=True, help="File to redirect command stdout+stderr")
    ap.add_argument("command", nargs=argparse.REMAINDER, help="Command to run (after --)")
    args = ap.parse_args()

    cmd = args.command
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd:
        print("sample_rss: no command given", file=sys.stderr)
        sys.exit(1)

    log_f = open(args.log, "w")
    proc = subprocess.Popen(cmd, stdout=log_f, stderr=subprocess.STDOUT)

    start_epoch = time.time()
    page_size = os.sysconf("SC_PAGE_SIZE")

    series_f = open(args.series, "w")
    series_f.write(f"# start_epoch={start_epoch:.6f}  page_size={page_size}\n")
    series_f.flush()

    target_pid = proc.pid
    child_pid: int | None = None

    def sample(_signum, _frame):
        nonlocal child_pid, target_pid
        # /usr/bin/time forks db_bench as child; track once found
        if child_pid is None:
            child_pid = find_child_pid(proc.pid)
            if child_pid is not None:
                target_pid = child_pid
        try:
            statm = open(f"/proc/{target_pid}/statm").read().split()
            resident = int(statm[1])
            series_f.write(f"{time.time():.6f} {resident}\n")
            series_f.flush()
        except (OSError, ValueError, IndexError):
            pass

    signal.signal(signal.SIGALRM, sample)
    signal.setitimer(signal.ITIMER_REAL, 1.0, 1.0)

    rc = proc.wait()

    signal.setitimer(signal.ITIMER_REAL, 0)
    signal.signal(signal.SIGALRM, signal.SIG_DFL)

    series_f.close()
    log_f.close()
    sys.exit(rc)


if __name__ == "__main__":
    main()
