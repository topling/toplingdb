#!/usr/bin/env python3
"""Runtime-only yaml edits for ToplingDB bench (never edit rockside / bench-conf sources).

Stable CI configs live in .github/bench-conf/*.yaml. This tool only applies
machine- or per-pass fields:

  --set-max-background-compactions N
  --worker-port / --hoster-http-url / --write-buffer-size(--bytes)
  --target-file-size-base / --target-file-size-multiplier
  --prefix-level-writers / --fill-level-writers / --rewrite-level-writer
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


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


def _first_level_writers(text: str) -> list[str] | None:
    m = re.search(
        r"^[ \t]*level_writers:[ \t]*\[(.*?)\]",
        text,
        re.MULTILINE,
    )
    if not m:
        return None
    return [part.strip() for part in m.group(1).split(",") if part.strip()]


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


def _map_first_level_writers(text: str, mapping: dict[str, str]) -> str:
    writers = _first_level_writers(text)
    if writers is None:
        sys.exit("FAIL: no level_writers: list found to map")
    mapped = tuple(mapping.get(w, w) for w in writers)
    return _replace_first_level_writers(text, mapped)


def _prefix_first_level_writers(text: str, count: int, value: str) -> str:
    writers = _first_level_writers(text)
    if writers is None:
        sys.exit("FAIL: no level_writers: list found to set prefix")
    if count < 0 or count > len(writers):
        sys.exit(
            f"FAIL: prefix count {count} out of range for "
            f"{len(writers)} level_writers"
        )
    return _replace_first_level_writers(
        text, tuple([value] * count + writers[count:])
    )


def _fill_first_level_writers(
    text: str,
    fill: str,
    *,
    prefix: int = 0,
    prefix_value: str | None = None,
) -> str:
    writers = _first_level_writers(text)
    if writers is None:
        sys.exit("FAIL: no level_writers: list found to fill")
    n = len(writers)
    if prefix < 0 or prefix > n:
        sys.exit(f"FAIL: prefix count {prefix} out of range for {n} level_writers")
    if prefix > 0 and prefix_value is None:
        sys.exit("FAIL: prefix_value required when prefix > 0")
    head = [prefix_value] * prefix if prefix else []
    return _replace_first_level_writers(text, tuple(head + [fill] * (n - prefix)))


def _sync_worker_port(text: str, port: int) -> str:
    return re.sub(
        r"http://127\.0\.0\.1:\d+",
        f"http://127.0.0.1:{port}",
        text,
    )


def _set_hoster_http_url(text: str, url: str) -> str:
    if re.search(r"\s", url):
        sys.exit("FAIL: hoster_http_url must not contain whitespace")
    current = re.compile(
        r"^([ \t]*hoster_http_url:[ \t]*)([^\s#]+)([ \t]*(?:#.*)?)?$",
        re.MULTILINE,
    )
    text, n = current.subn(rf"\g<1>{url}\g<3>", text, count=1)
    if n == 1:
        return text
    anchor = re.compile(r"^([ \t]*)hoster_root:[^\n]*$", re.MULTILINE)
    text, n = anchor.subn(
        lambda match: f"{match.group(0)}\n{match.group(1)}hoster_http_url: {url}",
        text,
        count=1,
    )
    if n != 1:
        sys.exit(
            "FAIL: expected exactly one hoster_root: to anchor "
            f"hoster_http_url, got {n}"
        )
    return text


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("yaml", type=Path)
    parser.add_argument(
        "--set-max-background-compactions",
        type=int,
        metavar="N",
        help="Set max_background_compactions to N (only that scalar)",
    )
    parser.add_argument("--write-buffer-size", help="e.g. 128M")
    parser.add_argument(
        "--write-buffer-size-bytes",
        type=int,
        help="convert bytes to M for write_buffer_size",
    )
    parser.add_argument(
        "--target-file-size-base",
        help="e.g. 128M",
    )
    parser.add_argument(
        "--target-file-size-multiplier",
        help="e.g. 1 or 1.5",
    )
    parser.add_argument("--worker-port", type=int)
    parser.add_argument("--hoster-http-url")
    parser.add_argument(
        "--rewrite-level-writer",
        nargs=2,
        metavar=("FROM", "TO"),
        help="Replace FROM with TO in the first level_writers list",
    )
    parser.add_argument(
        "--fill-level-writers",
        metavar="VALUE",
        help="Fill every slot in the first level_writers list with VALUE",
    )
    parser.add_argument(
        "--prefix-level-writers",
        nargs=2,
        metavar=("COUNT", "VALUE"),
        help="Set the first COUNT slots to VALUE (keeps the rest)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        help="Output path (default: overwrite yaml)",
    )
    args = parser.parse_args()

    yaml_path = args.yaml
    if not yaml_path.is_file():
        sys.exit(f"FAIL: yaml not found: {yaml_path}")

    if args.rewrite_level_writer is not None and (
        args.fill_level_writers is not None or args.prefix_level_writers is not None
    ):
        sys.exit(
            "FAIL: --rewrite-level-writer is mutually exclusive with "
            "--fill-level-writers / --prefix-level-writers"
        )

    scalar_ops = (
        args.set_max_background_compactions is not None
        or args.write_buffer_size is not None
        or args.write_buffer_size_bytes is not None
        or args.target_file_size_base is not None
        or args.target_file_size_multiplier is not None
        or args.worker_port is not None
        or args.hoster_http_url is not None
    )
    lw_ops = (
        args.rewrite_level_writer is not None
        or args.fill_level_writers is not None
        or args.prefix_level_writers is not None
    )
    if not scalar_ops and not lw_ops:
        sys.exit(
            "FAIL: specify at least one of --set-max-background-compactions, "
            "--worker-port, --hoster-http-url, --write-buffer-size[--bytes], "
            "--target-file-size-base, --target-file-size-multiplier, "
            "--prefix-level-writers, --fill-level-writers, "
            "--rewrite-level-writer"
        )

    out = args.out if args.out is not None else yaml_path
    text = _read(yaml_path)
    actions: list[str] = []

    if args.set_max_background_compactions is not None:
        n = args.set_max_background_compactions
        if n < 1:
            sys.exit(f"FAIL: max_background_compactions must be >= 1, got {n}")
        text = _replace_scalar(text, "max_background_compactions", n)
        actions.append(f"max_background_compactions={n}")

    wbs = args.write_buffer_size
    if args.write_buffer_size_bytes is not None:
        wbs = f"{args.write_buffer_size_bytes // 1024 // 1024}M"
    if wbs is not None:
        text = _replace_scalar(text, "write_buffer_size", wbs)
        actions.append(f"write_buffer_size={wbs}")

    if args.target_file_size_base is not None:
        text = _replace_scalar(
            text, "target_file_size_base", args.target_file_size_base
        )
        actions.append(f"target_file_size_base={args.target_file_size_base}")

    if args.target_file_size_multiplier is not None:
        text = _replace_scalar(
            text,
            "target_file_size_multiplier",
            args.target_file_size_multiplier,
        )
        actions.append(
            f"target_file_size_multiplier={args.target_file_size_multiplier}"
        )

    if args.worker_port is not None:
        text = _sync_worker_port(text, args.worker_port)
        actions.append(f"worker_port={args.worker_port}")

    if args.hoster_http_url is not None:
        text = _set_hoster_http_url(text, args.hoster_http_url)
        actions.append(f"hoster_http_url={args.hoster_http_url}")

    if args.rewrite_level_writer is not None:
        fro, to = args.rewrite_level_writer
        if fro == "*":
            sys.exit("FAIL: use --fill-level-writers instead of FROM=*")
        text = _map_first_level_writers(text, {fro: to})
        actions.append(f"level_writers map {fro!r}->{to!r}")

    if args.fill_level_writers is not None or args.prefix_level_writers is not None:
        prefix = 0
        prefix_value = None
        if args.prefix_level_writers is not None:
            count_s, prefix_value = args.prefix_level_writers
            try:
                prefix = int(count_s)
            except ValueError:
                sys.exit(f"FAIL: COUNT must be an integer, got {count_s!r}")
        if args.fill_level_writers is not None:
            text = _fill_first_level_writers(
                text,
                args.fill_level_writers,
                prefix=prefix,
                prefix_value=prefix_value,
            )
            note = f"level_writers fill={args.fill_level_writers!r}"
            if prefix:
                note += f" prefix[{prefix}]={prefix_value!r}"
            actions.append(note)
        else:
            text = _prefix_first_level_writers(text, prefix, prefix_value)
            actions.append(f"level_writers prefix[{prefix}]={prefix_value!r}")

    out.parent.mkdir(parents=True, exist_ok=True)
    _write(out, text)
    lw = _first_level_writers(text)
    extra = ""
    if lw is not None:
        extra = " " + _format_level_writers(tuple(lw))
    print(
        f"graft_bench_yaml: {out} " + " ".join(actions) + extra,
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
