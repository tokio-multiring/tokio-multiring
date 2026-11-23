#!/usr/bin/env python3
"""Run local servers in interleaved order; record load results and server CPU.

Configuration is a JSON list of {name, server: [argv...], client: [argv...],
ready_address: [host, port], client_env: {...}, prepare: [argv...] (optional)}.
Commands run in the repository root. Client may be ssh, provided the remote
load binary's MEASURE_START/END stderr markers are passed through unchanged.
Use an idle server machine: machine CPU includes unrelated activity.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import signal
import shutil
import sys
import socket
import subprocess
import tempfile
import time
import threading

ROOT = Path(__file__).resolve().parent.parent
TICKS = os.sysconf("SC_CLK_TCK")


def sample(pid):
    # comm may contain spaces and parentheses; stat fields start after its last ).
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    process = (int(fields[11]) + int(fields[12])) / TICKS
    fields = list(map(int, Path("/proc/stat").read_text().splitlines()[0].split()[1:9]))
    machine = (sum(fields) - fields[3] - fields[4]) / TICKS
    return time.monotonic(), process, machine


def wait_ready(server, address, log, ready_text):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if server.poll() is not None:
            raise RuntimeError(f"server exited with {server.returncode}")
        if ready_text and ready_text.encode() not in os.pread(log.fileno(), 65536, 0):
            time.sleep(0.05)
            continue
        try:
            with socket.create_connection(tuple(address), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError("server did not listen within 30 seconds")


def stop(process):
    if process is None:
        return
    # All launches use their own session. Kill descendants too: an exited
    # wrapper can otherwise leave a child holding the stderr pipe open.
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def run(case, repetition):
    env = {**os.environ, **{k: str(v) for k, v in case.get("client_env", {}).items()}}
    client = None
    timer = None
    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as client_out:
        server = subprocess.Popen(case["server"], cwd=ROOT, stdout=server_log, stderr=server_log, start_new_session=True)
        try:
            wait_ready(server, case["ready_address"], server_log, case.get("ready_text"))
            if case.get("prepare"):
                preparation = subprocess.Popen(case["prepare"], cwd=ROOT, env=env,
                                               stdout=sys.stderr, start_new_session=True)
                try:
                    if preparation.wait(timeout=case.get("timeout_s", 300)):
                        raise RuntimeError("preparation failed")
                finally:
                    stop(preparation)
            client = subprocess.Popen(case["client"], cwd=ROOT, env=env, stdout=client_out,
                                      stderr=subprocess.PIPE, text=True, start_new_session=True)
            timer = threading.Timer(case.get("timeout_s", 300), lambda: stop(client))
            timer.start()
            before = after = None
            diagnostics = []
            for line in client.stderr:
                if line.strip() == "MEASURE_START":
                    if before is not None:
                        raise RuntimeError("duplicate start marker")
                    before = sample(server.pid)
                elif line.strip() == "MEASURE_END":
                    if before is None or after is not None:
                        raise RuntimeError("invalid end marker")
                    after = sample(server.pid)
                else:
                    diagnostics.append(line.rstrip())
            code = client.wait()
            if code or before is None or after is None:
                raise RuntimeError(f"load failed: exit={code}, diagnostics={diagnostics}")
            client_out.seek(0)
            output = client_out.read().strip()
            wall = after[0] - before[0]
            return {"kind": "sample", "name": case["name"], "repetition": repetition,
                    "cpu_window_s": wall, "process_cores": (after[1] - before[1]) / wall,
                    "machine_cores": (after[2] - before[2]) / wall,
                    "load": output, "diagnostics": diagnostics, "case": case}
        except Exception:
            server_log.seek(0)
            print(server_log.read(), file=sys.stderr)
            raise
        finally:
            if timer is not None:
                timer.cancel()
            stop(client)
            stop(server)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("--repetitions", type=int, default=7)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()
    cases = json.loads(args.config.read_text())
    if not cases or args.repetitions < 1:
        parser.error("need cases and positive repetitions")
    git = lambda *argv: subprocess.check_output(["git", *argv], cwd=ROOT, text=True).strip()
    sources = hashlib.sha256()
    files = subprocess.check_output(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT).split(b"\0")
    for name in sorted(set(files) - {b""}):
        path = ROOT / os.fsdecode(name)
        sources.update(name + b"\0")
        if path.is_file():
            with path.open("rb") as source:
                sources.update(hashlib.file_digest(source, "sha256").digest())
        else:
            sources.update(b"missing")
    binaries = {}
    for case in cases:
        for command in (case["server"], case["client"], case.get("prepare", [])):
            if not command:
                continue
            executable = command[0]
            path = ROOT / executable if "/" in executable else Path(shutil.which(executable) or executable)
            if str(path) not in binaries:
                with path.open("rb") as binary:
                    binaries[str(path)] = hashlib.file_digest(binary, "sha256").hexdigest()
    print(json.dumps({"kind": "provenance", "revision": git("rev-parse", "HEAD"),
                      "source_sha256": sources.hexdigest(), "executable_sha256": binaries,
                      "config": cases,
                      "status": git("status", "--porcelain"),
                      "diff_sha256": hashlib.sha256(git("diff", "HEAD").encode()).hexdigest(),
                      "kernel": platform.release(), "kernel_cmdline": Path("/proc/cmdline").read_text().strip(),
                      "affinity": sorted(os.sched_getaffinity(0)), "seed": args.seed,
                      "repetitions": args.repetitions}), flush=True)
    rng = random.Random(args.seed)
    for repetition in range(args.repetitions):
        order = list(cases)
        rng.shuffle(order)
        for case in order:
            print(json.dumps(run(case, repetition)), flush=True)


if __name__ == "__main__":
    main()
