# AI 知识库：dcompact 单机链路与 CI bench 对标

> 面向 AI 助手的事实速查文档。所有结论均有代码出处（文件:行号基于 2026-07 的
> `memtable_as_log_index` 分支），引用前如怀疑过时可按符号名 grep 复核。
> 本文档由「CI 限核分布式 compact 对标」任务沉淀，随任务进展持续更新。

## 1. dcompact 单机（同机 worker）核心机制

### 1.1 路径前缀替换（最重要，配错即 412 或崩溃）

- worker 端把 hoster 传来路径中的 `hoster_root` 前缀替换为
  `NFS_MOUNT_ROOT/instance_name`（`sideplugin/topling-dcompact/tools/dcompact/dcompact_worker.cpp:2111-2113`）。
- **同机恒等映射**：`hoster_root == NFS_MOUNT_ROOT + "/" + instance_name`。
- **hoster_root 必须是 db path 的真父目录**（相等会 basename 越界，
  `dcompact_etcd.cc:1032-1033`）。
- worker 检查 `output_root` 必须以 `hoster_root` 开头，否则 HTTP 412。
- `NFS_DYNAMIC_MOUNT=0` 时不 mount，直接按替换后路径访问。

已落地 CI 模板（`.github/bench-conf/db_bench_enterprise_dcompact_ci.yaml`）：

| 项 | 值 |
|---|---|
| db path | `/dev/shm/db_bench_enterprise` |
| `hoster_root` | `/dev/shm` |
| `instance_name` | `shm` |
| worker env | `NFS_MOUNT_ROOT=/dev`、`NFS_DYNAMIC_MOUNT=0` |
| `http.document_root` | `/dev/shm` |
| `write_buffer_size` | `64M`（CI；本地可用临时副本改为 32M） |

**清理红线**：只准 `rm -rf` db path，禁止 `rm -rf` hoster_root（=/dev/shm）。

### 1.2 Fail-fast 与 min_level

- `allow_fallback_to_local: false` + `http_max_retry: 1` + `http_timeout: 1` +
  `overall_timeout: 3` + `timeout_multiplier: 30`
- `dcompact_min_level: 1`（output ≥ L1 走远端）
- `level_compaction_dynamic_level_bytes: false`

### 1.3 worker / 构建

- 启动：`dcompact_worker.exe -D listening_ports=8080`，必设
  `MAX_PARALLEL_COMPACTIONS`、`NFS_*`、`WORKER_DB_ROOT`
- 探活用 `/stat?html=0`（部分构建无 `/probe`，404）
- `make dcompact_worker`；staging：`.github/scripts/stage_dcompact_worker.sh`
- **terark 已嵌入 `librocksdb.so`**（`Makefile:1413-1414`），无需单独拷 so

## 2. CI 三段式

| 阶段 | 文件 | 要点 |
|------|------|------|
| build | `db_bench-build.yml` | Compile 含 `dcompact_worker`；`PATCH_COMPACTION_SERVICE=1` 编 RocksDB broker/worker；`stage_dcompact_worker.sh` |
| run | `db_bench-dcompact-run.yml` | 只 `workflow_dispatch`（首跑验收前）；消费 `db_bench-bin-plain`；`CPU_QUOTA` 入参 |
| pages | `bench_dcompact_pages.py` | 完全自含；`dcompact/index.html`；共享 `history.json`/`runs/`；**不改** `bench_logs_to_pages.py` |

上游 RocksDB 源码：`build_upstream_rocksdb.sh` 有 `facebook` remote 时 worktree，否则 clone。

联调脚本：
- `.github/scripts/run_dcompact_bench.sh`（Topling 四引擎中的 topling + dictzip10）
- `.github/scripts/run_rocksdb_cs_bench.sh`（RocksDB CompactionService spool）

## 3. cgroup

- 本地：`systemd-run --user --scope -p CPUQuota=$CPU_QUOTA`
- CI：`sudo systemd-run --scope --uid="$(id -u)" -p CPUQuota=$CPU_QUOTA`
- **子进程继承 cgroup**：worker/broker 必须在 cgroup 外启动

## 4. RocksDB CompactionService（Phase 2）

源码：`.github/rocksdb-remote-compact/{v8.10.2,master}/`
- v8.10.2：`StartV2` / `WaitForCompleteV2`
- master：`Schedule` / `Wait`
- patch：`.github/scripts/patch_db_bench_compaction_service.py`
- 启用：`PATCH_COMPACTION_SERVICE=1` → 产出 `remote_compact_broker`、`remote_compact_worker`
- gflag：`-compaction_spool_dir=<dir>`
- **SPOOL_DIR 必须与 DB_PATH 同文件系统**（Install 用 `rename`）。CI 曾用 `/tmp` spool + `/dev/shm` DB → `Invalid cross-device link`；拍板方案 A：spool 改 `/dev/shm/rocksdb-cs-spool-*`
- 本地验证（2026-07-25）：1M fillrandom+compact，`done_jobs=1`，db_bench RC=0

## 5. 本地实测 → CI num（2026-07-25）

| 项 | 值 |
|----|-----|
| 本地 Topling（两引擎×两套件，num=1M，wbs=32M，CPUQuota=25%） | wall ≈ **29s**；worker finished=6→12 |
| 本地 RocksDB v8.10 Phase1 悲观（1M，两套件，25%） | fillrandom **29.72s** + fillseq **26.35s** ≈ **56s**/引擎 |
| CI num 线性外推（目标单引擎约 20min） | **20_000_000**（与占位一致，已回填） |
| memtable_as_log_index × dcompact | **兼容**（本地全流程通过） |

## 6. 验证清单

1. yaml 不变式：db path 真含子 hoster；`hoster == NFS_MOUNT_ROOT/instance`；末段=库名
2. Topling：`Compactions.finished > 0`；fail-fast 下正常跑完即证明远端工作
3. RocksDB CS：spool 中 `state=DONE` 计数 > 0；日志含 `CompactionService spool:`
4. Pages：`bench_dcompact_pages.py` emit/merge；plain 首页不被改写
5. `db_bench-build.yml` 现存文件改动 ≤10%
