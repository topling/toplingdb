#!/usr/bin/env python3
"""Anchored patch: add -compact_target_level flag to upstream RocksDB db_bench_tool.cc.

Inserts:
  1. DEFINE_int32(compact_target_level, ...) after subcompactions flag.
  2. change_level/target_level wiring in Compact(), CompactAll(), filldeterministic.

Exits non-zero if any anchor is missing. Idempotent if already patched.
"""
import re
import sys

ALREADY_PATCHED_MARKER = 'compact_target_level'

FLAG_DEF = (
    '\n'
    'DEFINE_int32(compact_target_level, -1,\n'
    '             "For CompactRange in compact benchmarks, set "\n'
    '             "CompactRangeOptions::target_level (requires change_level). "\n'
    '             "-1 keeps the default behavior.");\n'
)

APPLY_BLOCK = (
    '    if (FLAGS_compact_target_level >= 0) {\n'
    '      cro.change_level = true;\n'
    '      cro.target_level = FLAGS_compact_target_level;\n'
    '    }\n'
)

APPLY_BLOCK_COMPACTION_OPTIONS = APPLY_BLOCK.replace('cro.', 'compactionOptions.')


def patch(path: str) -> None:
    with open(path, 'r') as f:
        src = f.read()

    if ALREADY_PATCHED_MARKER in src:
        print(f"[patch_db_bench_compact_target_level] already patched: {path}")
        return

    anchor1 = re.search(
        r'(static const bool FLAGS_subcompactions_dummy[^\n]*\n'
        r'    RegisterFlagValidator\(&FLAGS_subcompactions[^\n]*\);)',
        src,
    )
    if not anchor1:
        print(
            f"[patch_db_bench_compact_target_level] FATAL: subcompactions anchor not found in {path}",
            file=sys.stderr,
        )
        sys.exit(1)
    insert_pos1 = anchor1.end()
    src = src[:insert_pos1] + FLAG_DEF + src[insert_pos1:]

    compact_fn = re.search(
        r'(void Compact\(ThreadState\* thread\) \{.*?'
        r'cro\.max_subcompactions = static_cast<uint32_t>\(FLAGS_subcompactions\);\n)',
        src,
        re.DOTALL,
    )
    if not compact_fn:
        print(
            f"[patch_db_bench_compact_target_level] FATAL: Compact() anchor not found in {path}",
            file=sys.stderr,
        )
        sys.exit(1)
    src = src[:compact_fn.end()] + APPLY_BLOCK + src[compact_fn.end():]

    compact_all_fn = re.search(
        r'(void CompactAll\(\) \{.*?'
        r'cro\.max_subcompactions = static_cast<uint32_t>\(FLAGS_subcompactions\);\n)',
        src,
        re.DOTALL,
    )
    if not compact_all_fn:
        print(
            f"[patch_db_bench_compact_target_level] FATAL: CompactAll() anchor not found in {path}",
            file=sys.stderr,
        )
        sys.exit(1)
    src = src[:compact_all_fn.end()] + APPLY_BLOCK + src[compact_all_fn.end():]

    filldet = re.search(
        r'(compactionOptions\.max_subcompactions =\n'
        r'          static_cast<uint32_t>\(FLAGS_subcompactions\);\n)',
        src,
    )
    if not filldet:
        print(
            f"[patch_db_bench_compact_target_level] FATAL: filldeterministic anchor not found in {path}",
            file=sys.stderr,
        )
        sys.exit(1)
    src = src[:filldet.end()] + APPLY_BLOCK_COMPACTION_OPTIONS + src[filldet.end():]

    with open(path, 'w') as f:
        f.write(src)

    print(f"[patch_db_bench_compact_target_level] patched successfully: {path}")


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(
            f"Usage: {sys.argv[0]} <path/to/db_bench_tool.cc>",
            file=sys.stderr,
        )
        sys.exit(1)
    patch(sys.argv[1])
