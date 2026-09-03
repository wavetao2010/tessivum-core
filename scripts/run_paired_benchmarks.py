#!/usr/bin/env python3
"""Run the process-cold Rust/Cordis paired Core benchmark without a shell."""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

RUNTIME_SCHEMA = "tessivum.core-benchmark-runtime/v1"
PAIRED_SCHEMA = "tessivum.core-benchmark-paired/v1"
WORKLOAD_SCHEMA = "tessivum.core-benchmark-workload/v1"
SAMPLE_TIMEOUT_SECONDS = 120
CLEANUP_TIMEOUT_SECONDS = 5
EXPECTED_CASES = (
    ("scope_create_dispose", "ns", "scopes"),
    ("service_lookup", "operations/s", "serviceLookups"),
    ("event_emit", "operations/s", "eventEmits"),
    ("loader_load", "ns", "loaderEntries"),
    ("loader_update", "ns", "loaderEntries"),
    ("root_dispose", "ns", "rootChildren"),
    ("process_pss_peak", "KiB", "scopes"),
    ("process_pss_residue", "KiB", "scopes"),
    ("residue_after_dispose", "count", "scopes"),
)


def positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rust-bin", required=True, help="release tessivum-bench executable")
    parser.add_argument("--typescript-driver", default=str(root / "oracle" / "paired.ts"))
    parser.add_argument("--bun", default="bun")
    parser.add_argument("--cordis-root", required=True)
    parser.add_argument("--workload", default=str(root / "fixtures" / "benchmarks" / "core-paired.json"))
    parser.add_argument("--samples", type=positive_integer, default=30)
    parser.add_argument("--raw-out", help="write the complete success or failure record here")
    parser.add_argument(
        "--rust-arg",
        action="append",
        default=[],
        help="additional tessivum-bench argument; paired mode itself requires none",
    )
    return parser.parse_args()


def json_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def read_workload(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read workload {path}: {error}") from error
    expected_values = {
        "scopes": 1000,
        "serviceLookups": 256,
        "eventEmits": 256,
        "loaderEntries": 16,
        "rootChildren": 32,
    }
    if not isinstance(value, dict) or set(value) != {"schema", *expected_values}:
        raise ValueError(f"workload must contain exactly schema, {', '.join(expected_values)}")
    if value["schema"] != WORKLOAD_SCHEMA:
        raise ValueError(f"workload schema must be {WORKLOAD_SCHEMA}")
    for field, expected in expected_values.items():
        if not isinstance(value[field], int) or isinstance(value[field], bool) or value[field] != expected:
            raise ValueError(f"workload {field} must be the frozen value {expected}")
    return value


def process_group_members(pgid: int) -> list[int]:
    if sys.platform != "linux":
        return []
    members: list[int] = []
    try:
        entries = list(Path("/proc").iterdir())
    except OSError:
        return []
    for entry in entries:
        if not entry.name.isdecimal():
            continue
        try:
            stat = (entry / "stat").read_text(encoding="utf-8")
            fields = stat.rsplit(")", 1)[1].split()
            if int(fields[2]) == pgid:
                members.append(int(entry.name))
        except (IndexError, OSError, ValueError):
            continue
    return sorted(members)


def stop_process_group(pgid: int) -> list[int]:
    if sys.platform != "linux":
        try:
            os.killpg(pgid, signal.SIGTERM)
            time.sleep(0.05)
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        return []
    members = process_group_members(pgid)
    if not members:
        return []
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return []
    deadline = time.monotonic() + CLEANUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        members = process_group_members(pgid)
        if not members:
            return []
        time.sleep(0.05)
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        return []
    deadline = time.monotonic() + CLEANUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        members = process_group_members(pgid)
        if not members:
            return []
        time.sleep(0.05)
    return process_group_members(pgid)


def run_process(command: list[str], cwd: Path, environment: dict[str, str], timeout: int) -> dict[str, Any]:
    started = time.monotonic()
    try:
        process = subprocess.Popen(
            command,
            cwd=str(cwd),
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
    except OSError as error:
        return {
            "command": command,
            "cwd": str(cwd),
            "durationSeconds": time.monotonic() - started,
            "environment": environment_manifest(environment),
            "exitCode": None,
            "spawnError": str(error),
            "stderr": "",
            "stdout": "",
            "timedOut": False,
            "survivingDescendants": [],
        }
    timed_out = False
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        stop_process_group(process.pid)
        try:
            stdout, stderr = process.communicate(timeout=CLEANUP_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            stop_process_group(process.pid)
            stdout, stderr = b"", b"process cleanup exceeded its deadline"
    descendants = process_group_members(process.pid)
    surviving_descendants = descendants.copy()
    if descendants:
        stop_process_group(process.pid)
    return {
        "command": command,
        "cwd": str(cwd),
        "durationSeconds": time.monotonic() - started,
        "environment": environment_manifest(environment),
        "exitCode": process.returncode,
        "stderr": stderr.decode("utf-8", errors="replace"),
        "stdout": stdout.decode("utf-8", errors="replace"),
        "timedOut": timed_out,
        "survivingDescendants": surviving_descendants,
    }


def revision(path: Path) -> dict[str, Any]:
    command = ["git", "-C", str(path), "rev-parse", "--verify", "HEAD"]
    execution = run_process(command, path.parent, dict(os.environ), timeout=10)
    result: dict[str, Any] = {"command": command, "path": str(path)}
    if execution["timedOut"] or execution["exitCode"] != 0 or execution["survivingDescendants"] or execution.get("spawnError"):
        result["error"] = {
            key: execution[key]
            for key in ("exitCode", "spawnError", "stderr", "stdout", "timedOut", "survivingDescendants")
            if key in execution
        }
        return result
    value = execution["stdout"].strip()
    if not value:
        result["error"] = {"message": "git rev-parse returned no revision"}
        return result
    result["value"] = value
    return result


def collect_revisions(core_root: Path) -> tuple[dict[str, str], list[dict[str, Any]]]:
    roots = {
        "product": core_root.parent / "tessivum",
        "core": core_root,
        "dsh": core_root.parent / "upstream" / "deepseek-harness",
    }
    values: dict[str, str] = {}
    failures: list[dict[str, Any]] = []
    for name, path in roots.items():
        result = revision(path)
        if "value" in result:
            values[name] = result["value"]
        else:
            values[name] = "unavailable"
            failures.append({"stage": "revision", "revision": name, "detail": result})
    return values, failures


def child_environment(revisions: dict[str, str], cordis_root: Path) -> dict[str, str]:
    environment = dict(os.environ)
    environment["CORDIS_VENDOR_ROOT"] = str(cordis_root)
    environment["TESSIVUM_PRODUCT_REVISION"] = revisions["product"]
    environment["TESSIVUM_CORE_REVISION"] = revisions["core"]
    environment["DSH_REVISION"] = revisions["dsh"]
    return environment


def environment_manifest(environment: dict[str, str]) -> dict[str, Any]:
    variables = {}
    for name in sorted((
        "BUN_INSTALL",
        "CARGO_HOME",
        "CORDIS_VENDOR_ROOT",
        "DSH_REVISION",
        "PATH",
        "RUSTFLAGS",
        "RUSTUP_HOME",
        "TESSIVUM_CORE_REVISION",
        "TESSIVUM_PRODUCT_REVISION",
    )):
        if name in environment:
            variables[name] = environment[name]
    return {
        "cpuCount": os.cpu_count(),
        "machine": platform.machine(),
        "memoryBytes": system_memory_bytes(),
        "platform": platform.platform(),
        "python": sys.version,
        "variables": variables,
    }

def system_memory_bytes() -> int | None:
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        return None


def collect_tool_versions(cwd: Path, environment: dict[str, str], bun: str) -> dict[str, Any]:
    versions: dict[str, Any] = {}
    commands = (("rust", ["rustc", "--version"]), ("node", ["node", "--version"]), ("bun", [bun, "--version"]), ("pnpm", ["pnpm", "--version"]))
    for name, command in commands:
        execution = run_process(command, cwd, environment, timeout=10)
        if execution["exitCode"] == 0 and not execution["timedOut"] and not execution["survivingDescendants"] and not execution.get("spawnError"):
            versions[name] = {"command": command, "value": execution["stdout"].strip()}
        else:
            versions[name] = {"command": command, "value": "unavailable", "error": execution}
    return versions


def expected_operations(workload: dict[str, Any]) -> dict[str, int]:
    return {name: workload[field] for name, _unit, field in EXPECTED_CASES}


def validate_driver_report(
    runtime: str,
    report: Any,
    workload: dict[str, Any],
    revisions: dict[str, str],
) -> tuple[dict[str, float], list[str]]:
    errors: list[str] = []
    values: dict[str, float] = {}
    if not isinstance(report, dict):
        return values, ["stdout JSON document is not an object"]
    if report.get("schema") != RUNTIME_SCHEMA:
        errors.append(f"schema must be {RUNTIME_SCHEMA}")
    if report.get("workload") != workload:
        errors.append("driver workload does not exactly match the requested workload")
    if not isinstance(report.get("runtime"), dict):
        errors.append("driver runtime identity is missing")
    if report.get("revisions") != revisions:
        errors.append("driver revisions do not exactly match the runner revisions")
    if not isinstance(report.get("environment"), dict):
        errors.append("driver environment is missing")
    diagnostics = report.get("diagnostics")
    process_pss = diagnostics.get("processPss") if isinstance(diagnostics, dict) else None
    if not isinstance(diagnostics, dict):
        errors.append("driver diagnostics are missing")
    elif sys.platform == "linux":
        if not isinstance(process_pss, dict) or process_pss.get("status") != "available" or process_pss.get("source") != "/proc/self/smaps_rollup":
            errors.append("Linux process PSS is unavailable or not read from /proc/self/smaps_rollup")
    elif not isinstance(process_pss, dict) or process_pss.get("status") != "unavailable":
        errors.append("non-Linux process PSS diagnostics must be explicitly unavailable")

    benchmarks = report.get("benchmarks")
    if not isinstance(benchmarks, list) or len(benchmarks) != len(EXPECTED_CASES):
        return values, errors + ["driver benchmark case set is invalid"]
    names = [benchmark.get("name") if isinstance(benchmark, dict) else None for benchmark in benchmarks]
    expected_names = [name for name, _unit, _field in EXPECTED_CASES]
    if names != expected_names:
        return values, errors + [f"driver benchmark cases must be ordered as {expected_names}"]

    operations = expected_operations(workload)
    for benchmark, (name, unit, _field) in zip(benchmarks, EXPECTED_CASES):
        if not isinstance(benchmark, dict):
            errors.append(f"{name} benchmark is not an object")
            continue
        if benchmark.get("unit") != unit:
            errors.append(f"{name} unit must be {unit}")
        if benchmark.get("operationsPerSample") != operations[name]:
            errors.append(f"{name} operationsPerSample must be {operations[name]}")
        samples = benchmark.get("samples")
        if benchmark.get("status") == "unavailable":
            if sys.platform == "linux" or name not in {"process_pss_peak", "process_pss_residue"}:
                errors.append(f"{name} is unavailable")
            elif samples != [] or any(benchmark.get(field) is not None for field in ("median", "p95", "min", "max")) or not isinstance(benchmark.get("note"), str):
                errors.append(f"{name} unavailable result must contain empty samples, null summaries, and a note")
            continue
        if not isinstance(samples, list) or len(samples) != 1 or not is_number(samples[0]):
            errors.append(f"{name} must contain one finite numeric sample")
            continue
        sample = samples[0]
        if any(benchmark.get(field) != sample for field in ("median", "p95", "min", "max")):
            errors.append(f"{name} single-sample summary does not equal its raw sample")
            continue
        values[name] = sample
    return values, errors


def summarize(name: str, unit: str, operations_per_sample: int, samples: list[float], unavailable: bool = False) -> dict[str, Any]:
    if unavailable:
        return {
            "name": name,
            "unit": unit,
            "operationsPerSample": operations_per_sample,
            "samples": [],
            "median": None,
            "p95": None,
            "min": None,
            "max": None,
            "status": "unavailable",
            "note": "Linux /proc/self/smaps_rollup PSS is unavailable on this platform",
        }
    if not samples:
        return {
            "name": name,
            "unit": unit,
            "operationsPerSample": operations_per_sample,
            "samples": [],
            "median": None,
            "p95": None,
            "min": None,
            "max": None,
        }
    sorted_samples = sorted(samples)
    return {
        "name": name,
        "unit": unit,
        "operationsPerSample": operations_per_sample,
        "samples": samples,
        "median": sorted_samples[len(sorted_samples) // 2],
        "p95": sorted_samples[math.ceil(len(sorted_samples) * 0.95) - 1],
        "min": sorted_samples[0],
        "max": sorted_samples[-1],
    }


def failure_record(runtime: str, repetition: int, execution: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "runtime": runtime,
        "repetition": repetition,
        "reason": reason,
        **execution,
    }


def run(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    core_root = Path(__file__).resolve().parents[1]
    workload_path = Path(args.workload).resolve()
    workload = read_workload(workload_path)
    revisions, revision_failures = collect_revisions(core_root)
    environment = child_environment(revisions, Path(args.cordis_root).resolve())
    tool_versions = collect_tool_versions(core_root, environment, args.bun)
    rust_command = [str(Path(args.rust_bin).resolve()), "--paired-only", "--workload", str(workload_path), "--samples", "1", *args.rust_arg]
    typescript_command = [args.bun, str(Path(args.typescript_driver).resolve()), "--cordis-root", str(Path(args.cordis_root).resolve()), "--workload", str(workload_path), "--samples", "1"]
    commands = {"rust": rust_command, "typescript": typescript_command}
    samples: dict[str, dict[str, list[float]]] = {
        runtime: {name: [] for name, _unit, _field in EXPECTED_CASES}
        for runtime in ("rust", "typescript")
    }
    identities: dict[str, Any] = {}
    runs: list[dict[str, Any]] = []
    failures = revision_failures.copy()

    for repetition in range(args.samples):
        order = ("rust", "typescript") if repetition % 2 == 0 else ("typescript", "rust")
        for runtime in order:
            execution = run_process(commands[runtime], core_root, environment, SAMPLE_TIMEOUT_SECONDS)
            base_run = {"runtime": runtime, "repetition": repetition, **execution}
            if execution["timedOut"]:
                failures.append(failure_record(runtime, repetition, execution, "timeout"))
                runs.append({**base_run, "status": "failure"})
                continue
            if execution.get("spawnError"):
                failures.append(failure_record(runtime, repetition, execution, "spawn failure"))
                runs.append({**base_run, "status": "failure"})
                continue
            if execution["exitCode"] != 0:
                failures.append(failure_record(runtime, repetition, execution, "nonzero exit"))
                runs.append({**base_run, "status": "failure"})
                continue
            if execution["survivingDescendants"]:
                failures.append(failure_record(runtime, repetition, execution, "surviving descendant process"))
                runs.append({**base_run, "status": "failure"})
                continue
            try:
                report = json.loads(execution["stdout"])
            except json.JSONDecodeError as error:
                failures.append(failure_record(runtime, repetition, execution, f"stdout JSON parse failure: {error.msg}"))
                runs.append({**base_run, "status": "failure"})
                continue
            values, errors = validate_driver_report(runtime, report, workload, revisions)
            if errors:
                failures.append({
                    "runtime": runtime,
                    "repetition": repetition,
                    "reason": "driver report validation failure",
                    "errors": errors,
                    **execution,
                })
                runs.append({**base_run, "status": "failure", "report": report})
                continue
            for name, value in values.items():
                samples[runtime][name].append(value)
            identities.setdefault(runtime, report["runtime"])
            runs.append({**base_run, "status": "success", "report": report})

    operations = expected_operations(workload)
    aggregates = {
        runtime: {
            "runtime": identities.get(runtime),
            "benchmarks": [
                summarize(
                    name,
                    unit,
                    operations[name],
                    samples[runtime][name],
                    sys.platform != "linux" and name in {"process_pss_peak", "process_pss_residue"},
                )
                for name, unit, _field in EXPECTED_CASES
            ],
        }
        for runtime in ("rust", "typescript")
    }
    incomplete = [
        {"runtime": runtime, "case": name, "actualSamples": len(samples[runtime][name]), "requiredSamples": args.samples}
        for runtime in ("rust", "typescript")
        for name, _unit, _field in EXPECTED_CASES
        if len(samples[runtime][name]) != args.samples
        and not (sys.platform != "linux" and name in {"process_pss_peak", "process_pss_residue"})
    ]
    if incomplete:
        failures.append({"stage": "aggregation", "reason": "publication requires exactly N successful process-cold samples per runtime and case", "incomplete": incomplete})

    report = {
        "schema": PAIRED_SCHEMA,
        "status": "success" if not failures else "failure",
        "sampleCount": args.samples,
        "workload": workload,
        "environment": {**environment_manifest(environment), "tools": tool_versions},
        "revisions": revisions,
        "commands": commands,
        "runtimes": aggregates,
        "runs": runs,
        "failures": failures,
    }
    return report, 0 if report["status"] == "success" else 1


def write_raw_output(path: Path, document: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(document + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    args = parse_args()
    try:
        report, exit_code = run(args)
    except (OSError, ValueError) as error:
        report = {
            "schema": PAIRED_SCHEMA,
            "status": "failure",
            "failures": [{"stage": "configuration", "reason": str(error)}],
        }
        exit_code = 1
    document = json_text(report)
    if args.raw_out:
        try:
            write_raw_output(Path(args.raw_out), document)
        except OSError as error:
            report["status"] = "failure"
            report.setdefault("failures", []).append({"stage": "raw-out", "reason": str(error)})
            document = json_text(report)
            exit_code = 1
    sys.stdout.write(document + "\n")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

