#!/usr/bin/env python3
"""Install the version-matched spool CompactionService into an upstream tree."""

import re
import shutil
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REMOTE_COMPACT_ROOT = SCRIPT_DIR.parent / "rocksdb-remote-compact"
MARKER = "PATCH_DB_BENCH_COMPACTION_SERVICE"
FLAG = """
// PATCH_DB_BENCH_COMPACTION_SERVICE
DEFINE_string(compaction_spool_dir, "",
              "Directory used to exchange remote compaction jobs with "
              "remote_compact_broker");
"""
INCLUDE = '#include "tools/remote_compact/spool_compaction_service.h"\n'
INITIALIZATION = """
    if (!FLAGS_compaction_spool_dir.empty()) {
      options.compaction_service = std::make_shared<SpoolCompactionService>(
          FLAGS_compaction_spool_dir);
      fprintf(stdout, "CompactionService spool: %s\\\\n",
              FLAGS_compaction_spool_dir.c_str());
    }

"""


def version_directory(version: str) -> str:
    if version == "v8.10.2":
        return version
    if version in {"master", "main", "latest"}:
        return "master"
    raise ValueError(f"unsupported RocksDB version: {version}")


def replace_once(source: str, pattern: str, replacement: str,
                 description: str) -> str:
    result, count = re.subn(pattern, replacement, source, count=1,
                            flags=re.MULTILINE)
    if count != 1:
        raise RuntimeError(f"anchor not found for {description}")
    return result


def copy_sources(rocksdb_src: Path, source_dir: Path) -> None:
    destination = rocksdb_src / "tools" / "remote_compact"
    destination.mkdir(parents=True, exist_ok=True)
    for source in source_dir.glob("*"):
        if source.name == "Makefile.fragment":
            continue
        shutil.copy2(source, destination / source.name)


def patch_db_bench(path: Path) -> None:
    source = path.read_text(encoding="utf-8")
    if MARKER in source:
        print(f"[patch_compaction_service] already patched: {path}")
        return

    source = replace_once(
        source,
        r'(#include "rocksdb/db\.h"\n)',
        r"\1" + INCLUDE,
        "SpoolCompactionService include",
    )
    source = replace_once(
        source,
        r'(DEFINE_bool\(histogram,\s*false,\s*"[^"]*"\);)',
        r"\1" + FLAG,
        "compaction_spool_dir gflag",
    )
    source = replace_once(
        source,
        r'^(    if \(FLAGS_use_existing_keys\) \{\n)',
        INITIALIZATION + r"\1",
        "CompactionService initialization",
    )
    path.write_text(source, encoding="utf-8")
    print(f"[patch_compaction_service] patched: {path}")


def patch_makefile(path: Path, fragment: Path) -> None:
    source = path.read_text(encoding="utf-8")
    if MARKER in source:
        print(f"[patch_compaction_service] already patched: {path}")
        return

    source = replace_once(
        source,
        r'^(db_bench: \$\(OBJ_DIR\)/tools/db_bench\.o)',
        r"\1 $(OBJ_DIR)/tools/remote_compact/spool_compaction_service.o",
        "db_bench spool service object",
    )
    source += f"\n# {MARKER}\n" + fragment.read_text(encoding="utf-8")
    path.write_text(source, encoding="utf-8")
    print(f"[patch_compaction_service] patched: {path}")


def patch(rocksdb_src: Path, version: str) -> None:
    source_dir = REMOTE_COMPACT_ROOT / version_directory(version)
    db_bench = rocksdb_src / "tools" / "db_bench_tool.cc"
    makefile = rocksdb_src / "Makefile"
    if not source_dir.is_dir():
        raise RuntimeError(f"source directory does not exist: {source_dir}")
    if not db_bench.is_file() or not makefile.is_file():
        raise RuntimeError(f"not an upstream RocksDB source tree: {rocksdb_src}")

    copy_sources(rocksdb_src, source_dir)
    patch_db_bench(db_bench)
    patch_makefile(makefile, source_dir / "Makefile.fragment")


def main() -> int:
    if len(sys.argv) != 3:
        print(
            f"Usage: {sys.argv[0]} <rocksdb-src> <version: v8.10.2|master>",
            file=sys.stderr,
        )
        return 2
    try:
        patch(Path(sys.argv[1]).resolve(), sys.argv[2])
    except (OSError, RuntimeError, ValueError) as error:
        print(f"[patch_compaction_service] FATAL: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
