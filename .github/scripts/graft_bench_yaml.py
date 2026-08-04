#!/usr/bin/env python3
"""Unified runtime graft for ToplingDB bench yaml (CI/local; never edit rockside).

Profiles:
  plain-ci   db_bench-run.yml: write_buffer 128M, target_file_size 32M,
             target_file_size_multiplier 1.5, level_writers CI ladder, cpu knobs,
             strip compaction_executor_factory, dcompact_min_level 20
  avx512-ci  db_bench-avx512-run.yml: write_buffer 512M, target_file_size 32M,
             target_file_size_multiplier 1.5, level_writers CI ladder
  local      run_local_simple_top_pages.sh: target_file_size 32M,
             target_file_size_multiplier 1.5, level_writers CI ladder
  dcompact   run_dcompact_bench.sh temp yaml: target_file_size 32M,
             target_file_size_multiplier 1.5, level_writers CI ladder, cpu knobs,
             optional write_buffer / worker_port / minDictZipValueSize

Usage:
  graft_bench_yaml.py --profile plain-ci --cpu-quota 50% --nproc 8 \\
      --l1-writer simple --dictzip10-out /path/dictzip10.yaml /path/base.yaml
"""
from __future__ import annotations

import argparse
import math
import re
import shutil
import sys
from pathlib import Path

_L1_WRITERS = ("fast", "simple", "light_zip", "zip", "bb")

_CI_LEVEL_WRITERS = (
    "simple",
    "simple",
    "light_zip",
    "light_zip",
    "light_zip",
    "light_zip",
    "zip",
)

_PROFILE_DEFAULTS: dict[str, dict] = {
    "plain-ci": {
        "write_buffer_size": "128M",
        "target_file_size_base": "32M",
        "target_file_size_multiplier": 1.5,
        "level0_slowdown_writes_trigger": 4,
        "level_writers": _CI_LEVEL_WRITERS,
        "cpu_knobs": True,
        "strip_compaction_executor_factory": True,
        "dcompact_min_level": 20,
    },
    "avx512-ci": {
        "write_buffer_size": "512M",
        "target_file_size_base": "32M",
        "target_file_size_multiplier": 1.5,
        "level0_slowdown_writes_trigger": 4,
        "level_writers": _CI_LEVEL_WRITERS,
    },
    "local": {
        "target_file_size_base": "32M",
        "target_file_size_multiplier": 1.5,
        "level0_slowdown_writes_trigger": 4,
        "level_writers": _CI_LEVEL_WRITERS,
    },
    "dcompact": {
        "target_file_size_base": "32M",
        "target_file_size_multiplier": 1.5,
        "level0_slowdown_writes_trigger": 4,
        "level_writers": _CI_LEVEL_WRITERS,
        "cpu_knobs": True,
    },
}


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _replace_min_dict_zip_ci(text: str, val: int = 10) -> str:
    pat = re.compile(
        r"^([ \t]*minDictZipValueSize:[ \t]*)3000([ \t]*(?:#.*)?)?$",
        re.MULTILINE,
    )
    text, n = pat.subn(rf"\g<1>{val}\g<2>", text, count=1)
    if n != 1:
        sys.exit(f"FAIL: expected exactly one minDictZipValueSize: 3000, got {n}")
    return text


def _replace_scalar(
    text: str, key: str, value: str | int, *, required: bool = True
) -> str:
    pat = re.compile(
        rf"^([ \t]*{re.escape(key)}:[ \t]*)([^\s#]+)([ \t]*(?:#.*)?)?$",
        re.MULTILINE,
    )
    text, n = pat.subn(rf"\g<1>{value}\g<3>", text, count=1)
    if required and n != 1:
        sys.exit(f"FAIL: expected exactly one {key}:, got {n}")
    return text


def _format_level_writers(writers: tuple[str, ...]) -> str:
    return "[" + ", ".join(writers) + "]"


def _replace_first_level_writers(text: str, writers: tuple[str, ...]) -> str:
    formatted = _format_level_writers(writers)
    pat = re.compile(
        r"^([ \t]*level_writers:[ \t]*)\[[^\]]*\](.*)$",
        re.MULTILINE,
    )
    text, n = pat.subn(rf"\g<1>{formatted}\g<2>", text, count=1)
    if n != 1:
        sys.exit(f"FAIL: expected exactly one level_writers: to rewrite, got {n}")
    return text


def _first_level_writers(text: str) -> list[str] | None:
    m = re.search(
        r"^[ \t]*level_writers:[ \t]*\[(.*?)\]",
        text,
        re.MULTILINE,
    )
    if not m:
        return None
    return [part.strip() for part in m.group(1).split(",") if part.strip()]


def _apply_cpu_knobs(text: str, db_q: str, nproc: int, l1_writer: str) -> str:
    if l1_writer not in _L1_WRITERS:
        sys.exit(
            f"FAIL: l1_writer must be one of {_L1_WRITERS}, got {l1_writer!r}"
        )
    m = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)%", db_q.strip())
    if not m:
        sys.exit(f"FAIL: db CPUQuota must look like 50%, got {db_q!r}")
    db_cpu = float(m.group(1)) / 100.0
    if nproc <= 0:
        sys.exit(f"FAIL: nproc must be > 0, got {nproc}")
    if db_cpu >= nproc:
        sys.exit(f"FAIL: db_cpu={db_cpu} >= nproc={nproc}")

    worker_cpu = nproc - db_cpu
    knobs = {
        "max_level1_subcompactions": 2,
        "max_background_flushes": 1,
        "max_background_compactions": max(1, min(13, math.ceil(worker_cpu))),
        "dcompact_min_level": 2,
    }
    for key, val in knobs.items():
        pat = re.compile(
            rf"^([ \t]*{re.escape(key)}:[ \t]*)(-?\d+)([ \t]*(?:#.*)?)?$",
            re.MULTILINE,
        )
        text, n = pat.subn(rf"\g<1>{val}\g<3>", text, count=1)
        if key == "dcompact_min_level" and n == 0:
            continue
        if n != 1:
            sys.exit(f"FAIL: expected exactly one {key}:, got {n}")

    text, n = re.subn(
        r"^([ \t]*forceNeedCompact:[ \t]*)\S+",
        r"\g<1>true",
        text,
        flags=re.MULTILINE,
    )
    if n < 1:
        sys.exit(f"FAIL: expected forceNeedCompact:, got {n}")

    print(
        f"graft_bench_yaml: cpu knobs nproc={nproc} db_cpu={db_cpu} "
        f"worker_cpu={worker_cpu} l1_writer={l1_writer}",
        file=sys.stderr,
    )
    return text


def _strip_compaction_executor_factory(text: str) -> str:
    out = [
        line
        for line in text.splitlines(keepends=True)
        if not re.match(r"^[ \t]*compaction_executor_factory:", line)
    ]
    if re.search(r"^[ \t]*compaction_executor_factory:", text, re.MULTILINE):
        print(
            "graft_bench_yaml: stripped compaction_executor_factory",
            file=sys.stderr,
        )
    return "".join(out)


def _sync_worker_port(text: str, port: int) -> str:
    return re.sub(
        r"http://127\.0\.0\.1:\d+",
        f"http://127.0.0.1:{port}",
        text,
    )


def _scalar_value(text: str, key: str) -> str | None:
    m = re.search(
        rf"^[ \t]*{re.escape(key)}:[ \t]*([^\s#]+)",
        text,
        re.MULTILINE,
    )
    return m.group(1) if m else None


def _verify_graft(
    path: Path,
    profile: str,
    *,
    l1_writer: str,
    write_buffer_size: str | None,
    target_file_size_base: str | None,
    target_file_size_multiplier: float | str | None,
    level0_slowdown_writes_trigger: int | None,
    dictzip10_out: Path | None,
    min_dict_zip_value_size: int | None,
) -> None:
    cfg = _PROFILE_DEFAULTS[profile]
    text = _read(path)
    checks: list[str] = []

    expected_wbs = write_buffer_size or cfg.get("write_buffer_size")
    if expected_wbs:
        got = _scalar_value(text, "write_buffer_size")
        if got != expected_wbs:
            sys.exit(
                f"FAIL verify {path}: write_buffer_size={got!r} want {expected_wbs!r}"
            )
        checks.append(f"write_buffer_size={expected_wbs}")

    expected_tfs = target_file_size_base or cfg.get("target_file_size_base")
    if expected_tfs:
        got = _scalar_value(text, "target_file_size_base")
        if got != expected_tfs:
            sys.exit(
                f"FAIL verify {path}: target_file_size_base={got!r} want {expected_tfs!r}"
            )
        checks.append(f"target_file_size_base={expected_tfs}")

    expected_tfm = target_file_size_multiplier or cfg.get(
        "target_file_size_multiplier"
    )
    if expected_tfm is not None:
        got = _scalar_value(text, "target_file_size_multiplier")
        want = str(expected_tfm)
        if got != want:
            sys.exit(
                f"FAIL verify {path}: target_file_size_multiplier={got!r} "
                f"want {want!r}"
            )
        checks.append(f"target_file_size_multiplier={want}")

    expected_l0 = level0_slowdown_writes_trigger if level0_slowdown_writes_trigger is not None else cfg.get(
        "level0_slowdown_writes_trigger"
    )
    if expected_l0 is not None:
        got = _scalar_value(text, "level0_slowdown_writes_trigger")
        want = str(expected_l0)
        if got != want:
            sys.exit(
                f"FAIL verify {path}: level0_slowdown_writes_trigger={got!r} "
                f"want {want!r}"
            )
        checks.append(f"level0_slowdown_writes_trigger={want}")

    expected_lw = cfg.get("level_writers")
    if expected_lw is not None:
        got = _first_level_writers(text)
        want = list(expected_lw)
        if got != want:
            sys.exit(
                f"FAIL verify {path}: level_writers={got!r} want {want!r}"
            )
        checks.append(f"level_writers={_format_level_writers(expected_lw)}")

    if cfg.get("cpu_knobs"):
        if not re.search(
            r"^[ \t]*forceNeedCompact:[ \t]*true\b", text, re.MULTILINE
        ):
            sys.exit(f"FAIL verify {path}: forceNeedCompact: true not found")
        checks.append("forceNeedCompact=true")

    if cfg.get("strip_compaction_executor_factory"):
        if re.search(r"^[ \t]*compaction_executor_factory:", text, re.MULTILINE):
            sys.exit(
                f"FAIL verify {path}: compaction_executor_factory still present"
            )
        checks.append("no compaction_executor_factory")

    if profile == "local":
        if "SimpleTopTable" not in text:
            sys.exit(f"FAIL verify {path}: SimpleTopTable not found")
        checks.append("sanity SimpleTopTable")

    if min_dict_zip_value_size is not None:
        if not re.search(
            rf"^[ \t]*minDictZipValueSize:[ \t]*{min_dict_zip_value_size}\s*$",
            text,
            re.MULTILINE,
        ):
            sys.exit(
                f"FAIL verify {path}: minDictZipValueSize={min_dict_zip_value_size} not found"
            )
        checks.append(f"minDictZipValueSize={min_dict_zip_value_size}")

    if dictzip10_out is not None:
        dz = _read(dictzip10_out)
        if not re.search(
            r"^[ \t]*minDictZipValueSize:[ \t]*10\s*$", dz, re.MULTILINE
        ):
            sys.exit(
                f"FAIL verify {dictzip10_out}: minDictZipValueSize=10 not found"
            )
        if expected_tfm is not None:
            got = _scalar_value(dz, "target_file_size_multiplier")
            want = str(expected_tfm)
            if got != want:
                sys.exit(
                    f"FAIL verify {dictzip10_out}: "
                    f"target_file_size_multiplier={got!r} want {want!r}"
                )
        if expected_l0 is not None:
            got = _scalar_value(dz, "level0_slowdown_writes_trigger")
            want = str(expected_l0)
            if got != want:
                sys.exit(
                    f"FAIL verify {dictzip10_out}: "
                    f"level0_slowdown_writes_trigger={got!r} want {want!r}"
                )
        checks.append("dictzip10_out ok")

    print(
        f"graft_bench_yaml verify: {path} profile={profile} "
        + " ".join(checks),
        file=sys.stderr,
    )


def _graft_file(
    path: Path,
    profile: str,
    *,
    cpu_quota: str | None,
    nproc: int | None,
    l1_writer: str,
    write_buffer_size: str | None,
    target_file_size_base: str | None,
    target_file_size_multiplier: float | str | None,
    level0_slowdown_writes_trigger: int | None,
    min_dict_zip_value_size: int | None,
    worker_port: int | None,
    dcompact_min_level: int | None,
    strip_compaction_executor_factory: bool,
) -> None:
    cfg = dict(_PROFILE_DEFAULTS[profile])
    text = _read(path)
    actions: list[str] = []

    wbs = write_buffer_size or cfg.get("write_buffer_size")
    if wbs:
        text = _replace_scalar(text, "write_buffer_size", wbs)
        actions.append(f"write_buffer_size={wbs}")

    tfs = target_file_size_base or cfg.get("target_file_size_base")
    if tfs:
        text = _replace_scalar(text, "target_file_size_base", tfs)
        actions.append(f"target_file_size_base={tfs}")

    tfm = target_file_size_multiplier if target_file_size_multiplier is not None else cfg.get(
        "target_file_size_multiplier"
    )
    if tfm is not None:
        text = _replace_scalar(text, "target_file_size_multiplier", tfm)
        actions.append(f"target_file_size_multiplier={tfm}")

    l0 = level0_slowdown_writes_trigger if level0_slowdown_writes_trigger is not None else cfg.get(
        "level0_slowdown_writes_trigger"
    )
    if l0 is not None:
        text = _replace_scalar(text, "level0_slowdown_writes_trigger", l0)
        actions.append(f"level0_slowdown_writes_trigger={l0}")

    expected_lw = cfg.get("level_writers")
    if expected_lw is not None:
        text = _replace_first_level_writers(text, expected_lw)
        actions.append(f"level_writers={_format_level_writers(expected_lw)}")

    if worker_port is not None:
        text = _sync_worker_port(text, worker_port)
        actions.append(f"worker_port={worker_port}")

    if cfg.get("cpu_knobs"):
        if cpu_quota is None or nproc is None:
            sys.exit(f"FAIL: profile {profile!r} requires --cpu-quota and --nproc")
        text = _apply_cpu_knobs(text, cpu_quota, nproc, l1_writer)

    dml = dcompact_min_level if dcompact_min_level is not None else cfg.get(
        "dcompact_min_level"
    )
    if dml is not None:
        text = _replace_scalar(text, "dcompact_min_level", dml, required=False)
        actions.append(f"dcompact_min_level={dml}")

    if strip_compaction_executor_factory or cfg.get(
        "strip_compaction_executor_factory"
    ):
        text = _strip_compaction_executor_factory(text)
        if re.search(r"^[ \t]*compaction_executor_factory:", text, re.MULTILINE):
            sys.exit(
                f"FAIL: {path} still has compaction_executor_factory after strip"
            )

    if min_dict_zip_value_size is not None:
        text = _replace_min_dict_zip_ci(text, min_dict_zip_value_size)
        actions.append(f"minDictZipValueSize={min_dict_zip_value_size}")

    _write(path, text)
    print(
        f"graft_bench_yaml: {path} profile={profile} "
        + " ".join(actions),
        file=sys.stderr,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        required=True,
        choices=sorted(_PROFILE_DEFAULTS),
    )
    parser.add_argument("yaml", type=Path)
    parser.add_argument("--cpu-quota")
    parser.add_argument("--nproc", type=int)
    parser.add_argument("--l1-writer", default="simple")
    parser.add_argument("--write-buffer-size", help="e.g. 128M")
    parser.add_argument(
        "--write-buffer-size-bytes",
        type=int,
        help="convert bytes to M for write_buffer_size (dcompact env)",
    )
    parser.add_argument("--target-file-size-base")
    parser.add_argument(
        "--target-file-size-multiplier",
        type=float,
        help="override profile default (CI default: 1.5)",
    )
    parser.add_argument(
        "--level0-slowdown-writes-trigger",
        type=int,
        help="override profile default (CI default: 4)",
    )
    parser.add_argument("--min-dict-zip-value-size", type=int)
    parser.add_argument(
        "--dictzip10-out",
        type=Path,
        help="copy grafted base yaml here and set minDictZipValueSize=10",
    )
    parser.add_argument("--worker-port", type=int)
    parser.add_argument("--dcompact-min-level", type=int)
    parser.add_argument(
        "--strip-compaction-executor-factory",
        action="store_true",
    )
    parser.add_argument(
        "--verify",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="After graft, assert profile expectations (default: on)",
    )
    args = parser.parse_args()

    yaml_path = args.yaml
    if not yaml_path.is_file():
        sys.exit(f"FAIL: yaml not found: {yaml_path}")

    wbs = args.write_buffer_size
    if args.write_buffer_size_bytes is not None:
        wbs = f"{args.write_buffer_size_bytes // 1024 // 1024}M"

    _graft_file(
        yaml_path,
        args.profile,
        cpu_quota=args.cpu_quota,
        nproc=args.nproc,
        l1_writer=args.l1_writer,
        write_buffer_size=wbs,
        target_file_size_base=args.target_file_size_base,
        target_file_size_multiplier=args.target_file_size_multiplier,
        level0_slowdown_writes_trigger=args.level0_slowdown_writes_trigger,
        min_dict_zip_value_size=args.min_dict_zip_value_size,
        worker_port=args.worker_port,
        dcompact_min_level=args.dcompact_min_level,
        strip_compaction_executor_factory=args.strip_compaction_executor_factory,
    )

    if args.dictzip10_out is not None:
        args.dictzip10_out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(yaml_path, args.dictzip10_out)
        text = _read(args.dictzip10_out)
        text = _replace_min_dict_zip_ci(text, 10)
        _write(args.dictzip10_out, text)
        print(
            f"graft_bench_yaml: {args.dictzip10_out} minDictZipValueSize=10",
            file=sys.stderr,
        )

    if args.verify:
        _verify_graft(
            yaml_path,
            args.profile,
            l1_writer=args.l1_writer,
            write_buffer_size=wbs,
            target_file_size_base=args.target_file_size_base,
            target_file_size_multiplier=args.target_file_size_multiplier,
            level0_slowdown_writes_trigger=args.level0_slowdown_writes_trigger,
            dictzip10_out=args.dictzip10_out,
            min_dict_zip_value_size=args.min_dict_zip_value_size,
        )


if __name__ == "__main__":
    main()
