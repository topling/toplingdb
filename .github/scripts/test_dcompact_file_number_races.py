#!/usr/bin/env python3

import argparse
import concurrent.futures
import copy
import http.client
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit


def post_raw(target, path, raw_body):
    parsed = urlsplit(target)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=3)
    try:
        conn.request(
            "POST",
            path,
            body=raw_body,
            headers={"Content-Type": "application/json"},
        )
        response = conn.getresponse()
        body = response.read()
        result = {
            "http_status": response.status,
            "body": body.decode("utf-8", errors="replace"),
        }
        try:
            result["json"] = json.loads(result["body"])
        except json.JSONDecodeError:
            pass
        return result
    except Exception as ex:
        return {"error": f"{type(ex).__name__}: {ex}"}
    finally:
        conn.close()


def post_json(target, path, body):
    raw = json.dumps(body, separators=(",", ":")).encode("utf-8")
    return post_raw(target, path, raw)


def response_status(result):
    value = result.get("json")
    if isinstance(value, dict):
        return value.get("status")
    return None


class TestState:
    def __init__(self, args):
        self.target = args.target
        self.report_path = Path(args.report)
        self.done_marker = Path(args.report + ".done")
        self.done_root = Path(args.done_root)
        self.host_log = Path(args.host_log)
        self.lock = threading.RLock()
        self.first_request_claimed = False
        self.report = {
            "target": self.target,
            "active_checks": {},
            "boundary_race": {},
        }
        self.initial_done_files = self.find_done_files()

    def find_done_files(self):
        if not self.done_root.exists():
            return set()
        return {str(path) for path in self.done_root.rglob("compact.done")}

    def save(self):
        with self.lock:
            self.report_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.report_path.with_suffix(self.report_path.suffix + ".tmp")
            tmp.write_text(
                json.dumps(self.report, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            tmp.replace(self.report_path)

    def set_report(self, section, key, value):
        with self.lock:
            self.report[section][key] = value
        self.save()

    def claim_first_request(self, path, body):
        with self.lock:
            if self.first_request_claimed:
                return False
            self.first_request_claimed = True
            self.report["request_path"] = path
            self.report["request_body"] = body
        self.save()
        return True

    def run_concurrent(self, path, body, count):
        with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
            futures = [
                pool.submit(post_json, self.target, path, body)
                for _ in range(count)
            ]
            return [future.result() for future in futures]

    def run_active_checks(self, path, body):
        mutations = {}

        value = copy.deepcopy(body)
        value["dcompact_executor"] = 0
        mutations["wrong_executor"] = value

        value = copy.deepcopy(body)
        value["dbname"] += "-wrong"
        mutations["wrong_dbname"] = value

        value = copy.deepcopy(body)
        value["db_session_id"] += "-wrong"
        mutations["wrong_db_session_id"] = value

        value = copy.deepcopy(body)
        value["job_id"] += 1
        mutations["wrong_job_id"] = value

        value = copy.deepcopy(body)
        value["attempt"] += 1
        mutations["wrong_attempt"] = value

        for name, request in mutations.items():
            self.set_report("active_checks", name, post_json(self.target, path, request))

        value = copy.deepcopy(body)
        del value["dcompact_executor"]
        self.set_report(
            "active_checks",
            "missing_dcompact_executor",
            post_json(self.target, path, value),
        )

        value = copy.deepcopy(body)
        del value["labour_id"]
        self.set_report(
            "active_checks",
            "missing_labour_id",
            post_json(self.target, path, value),
        )

        results = self.run_concurrent(path, body, 12)
        self.set_report("active_checks", "concurrent_valid", results)

    def wait_for_new_done_file(self, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            current = self.find_done_files()
            new_files = current - self.initial_done_files
            if new_files:
                return sorted(new_files)[0]
            time.sleep(0.01)
        return None

    def wait_for_host_install(self, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                text = self.host_log.read_text(encoding="utf-8", errors="replace")
                if "Dcompacted" in text:
                    return True
            except FileNotFoundError:
                pass
            time.sleep(0.01)
        return False

    def run_boundary_race(self, path, body):
        try:
            done_file = self.wait_for_new_done_file(30)
            self.set_report("boundary_race", "compact_done_file", done_file)
            if done_file is None:
                return

            boundary = self.run_concurrent(path, body, 32)
            self.set_report("boundary_race", "at_compact_done", boundary)

            installed = self.wait_for_host_install(15)
            self.set_report("boundary_race", "host_install_observed", installed)
            if not installed:
                return

            stale = self.run_concurrent(path, body, 16)
            self.set_report("boundary_race", "after_unregister", stale)
        except Exception as ex:
            self.set_report(
                "boundary_race", "exception", f"{type(ex).__name__}: {ex}"
            )
        finally:
            self.done_marker.write_text("done\n", encoding="utf-8")


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    state = None

    def log_message(self, fmt, *args):
        print(fmt % args, flush=True)

    def do_GET(self):
        if self.path == "/health":
            body = b"ok\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(length)
        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError:
            body = None

        is_allocate = "dcompact_action=allocate_file_number" in self.path
        if is_allocate and isinstance(body, dict):
            if self.state.claim_first_request(self.path, body):
                try:
                    self.state.run_active_checks(self.path, body)
                except Exception as ex:
                    self.state.set_report(
                        "active_checks", "exception", f"{type(ex).__name__}: {ex}"
                    )
                thread = threading.Thread(
                    target=self.state.run_boundary_race,
                    args=(self.path, body),
                    daemon=True,
                )
                thread.start()

        result = post_raw(self.state.target, self.path, raw_body)
        if "error" in result:
            encoded = result["error"].encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "text/plain")
        else:
            encoded = result["body"].encode("utf-8")
            self.send_response(result["http_status"])
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def require_not_active(name, result, errors):
    if result.get("http_status") != 200 or response_status(result) != "not_active":
        errors.append(f"{name}: expected HTTP 200 status=not_active, got {result}")


def require_ok_unique(name, results, errors):
    numbers = []
    for index, result in enumerate(results):
        if result.get("http_status") != 200 or response_status(result) != "ok":
            errors.append(f"{name}[{index}]: expected HTTP 200 status=ok, got {result}")
            continue
        number = result.get("json", {}).get("file_number")
        if not isinstance(number, int) or number <= 0:
            errors.append(f"{name}[{index}]: invalid file_number in {result}")
        else:
            numbers.append(number)
    if len(numbers) != len(set(numbers)):
        errors.append(f"{name}: duplicate file numbers: {numbers}")


def check_report(path):
    report = json.loads(Path(path).read_text(encoding="utf-8"))
    errors = []
    active = report.get("active_checks", {})

    for name in (
        "wrong_executor",
        "wrong_dbname",
        "wrong_db_session_id",
        "wrong_job_id",
        "wrong_attempt",
    ):
        require_not_active(name, active.get(name, {}), errors)

    for name in ("missing_dcompact_executor", "missing_labour_id"):
        result = active.get(name, {})
        body = result.get("body", "")
        if response_status(result) in ("ok", "not_active") or "Caught Status" not in body:
            errors.append(f"{name}: malformed request was not rejected: {result}")

    concurrent_valid = active.get("concurrent_valid", [])
    if len(concurrent_valid) != 12:
        errors.append(
            f"concurrent_valid: expected 12 responses, got {len(concurrent_valid)}"
        )
    require_ok_unique("concurrent_valid", concurrent_valid, errors)

    race = report.get("boundary_race", {})
    if not race.get("compact_done_file"):
        errors.append("boundary_race: compact.done was not observed")
    if not race.get("host_install_observed"):
        errors.append("boundary_race: host install was not observed")

    boundary = race.get("at_compact_done", [])
    if len(boundary) != 32:
        errors.append(f"at_compact_done: expected 32 responses, got {len(boundary)}")
    boundary_numbers = []
    for index, result in enumerate(boundary):
        status = response_status(result)
        if result.get("http_status") != 200 or status not in ("ok", "not_active"):
            errors.append(
                f"at_compact_done[{index}]: expected ok/not_active, got {result}"
            )
        if status == "ok":
            number = result.get("json", {}).get("file_number")
            if isinstance(number, int) and number > 0:
                boundary_numbers.append(number)
            else:
                errors.append(f"at_compact_done[{index}]: invalid file_number")
    if len(boundary_numbers) != len(set(boundary_numbers)):
        errors.append(f"at_compact_done: duplicate file numbers: {boundary_numbers}")

    stale = race.get("after_unregister", [])
    if len(stale) != 16:
        errors.append(f"after_unregister: expected 16 responses, got {len(stale)}")
    for index, result in enumerate(stale):
        require_not_active(f"after_unregister[{index}]", result, errors)

    if errors:
        print("FAIL")
        for error in errors:
            print(f"- {error}")
        return 1

    print("PASS")
    print("- identity mismatch requests returned not_active")
    print("- malformed requests were rejected")
    print("- concurrent active allocations returned unique file numbers")
    print("- compact.done boundary burst remained well-formed")
    print("- post-unregister replay returned not_active")
    return 0


def run_test(args):
    root = Path(args.root).resolve()
    prefix = Path(args.prefix).resolve() if args.prefix else (
        root / "_local_dcompact_feature_prefix/topling"
    )
    if args.run_root:
        run_root = Path(args.run_root).resolve()
        run_root.mkdir(parents=True, exist_ok=False)
    else:
        run_root = Path(
            tempfile.mkdtemp(prefix="_local_dcompact_fault_race_run.", dir=root)
        )
    worker_root = Path(
        tempfile.mkdtemp(prefix="dcompact-worker-race.", dir="/dev/shm")
    )
    report = run_root / "report.json"
    done_root = Path(args.db_path)
    host_log = done_root / "LOG"

    state_args = argparse.Namespace(
        target=args.target,
        report=str(report),
        done_root=str(done_root),
        host_log=str(host_log),
    )
    state = TestState(state_args)
    ProxyHandler.state = state
    host, port_text = args.listen.rsplit(":", 1)
    server = ThreadingHTTPServer((host, int(port_text)), ProxyHandler)
    server.daemon_threads = True
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    env = os.environ.copy()
    env.update(
        {
            "PREFIX": str(prefix),
            "NUM": str(args.num),
            "WRITE_BUFFER_SIZE": str(args.write_buffer_size),
            "LOGDIR_BASE": str(run_root / "logs"),
            "ENGINES": args.engines,
            "CPU_QUOTA": args.cpu_quota,
            "WORKER_DB_ROOT": str(worker_root),
            "DB_PATH": str(done_root),
            "HOSTER_HTTP_URL": (
                f"http://{args.listen}/CompactionExecutorFactory/dcompact"
            ),
        }
    )

    result = 1
    try:
        subprocess.run(
            ["/usr/bin/bash", str(root / ".github/scripts/run_dcompact_bench.sh")],
            cwd=root,
            env=env,
            check=True,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not state.done_marker.exists():
            time.sleep(0.05)
        if not state.done_marker.exists():
            print("FAIL: fault proxy did not finish boundary test")
        else:
            result = check_report(report)
        return result
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
        shutil.rmtree(worker_root)
        print(f"fault/race test artifacts: {run_root}")


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve")
    serve.add_argument("--listen", default="127.0.0.1:2012")
    serve.add_argument("--target", default="http://127.0.0.1:2011")
    serve.add_argument("--report", required=True)
    serve.add_argument("--done-root", required=True)
    serve.add_argument("--host-log", required=True)

    check = subparsers.add_parser("check")
    check.add_argument("report")

    run = subparsers.add_parser("run")
    run.add_argument(
        "--root",
        default=str(Path(__file__).resolve().parents[2]),
    )
    run.add_argument("--prefix")
    run.add_argument("--run-root")
    run.add_argument("--listen", default="127.0.0.1:2012")
    run.add_argument("--target", default="http://127.0.0.1:2011")
    run.add_argument("--db-path", default="/dev/shm/db_bench_enterprise")
    run.add_argument("--num", type=int, default=1000000)
    run.add_argument("--write-buffer-size", type=int, default=33554432)
    run.add_argument("--engines", default="zipkeyonly")
    run.add_argument("--cpu-quota", default="50%")

    args = parser.parse_args()
    if args.command == "check":
        raise SystemExit(check_report(args.report))
    if args.command == "run":
        raise SystemExit(run_test(args))

    host, port_text = args.listen.rsplit(":", 1)
    state = TestState(args)
    ProxyHandler.state = state
    server = ThreadingHTTPServer((host, int(port_text)), ProxyHandler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()
