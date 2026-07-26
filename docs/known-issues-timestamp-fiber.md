# TOPLINGDB_WITH_TIMESTAMP + fiber MultiGet 分支已知问题

## 背景与定位

当前主线目标是优化**非** `TOPLINGDB_WITH_TIMESTAMP` 路径。fiber MultiGet 分支
（`DBImpl::MultiGetOneCFH`，`db/db_impl/db_impl.cc`，`TOPLINGDB_WITH_FIBER_AIO`
下的默认单 CF MultiGet 路径）**没有 TIMESTAMP 场景的单元测试**，该分支在
`TOPLINGDB_WITH_TIMESTAMP` 构建下的正确性未经验证，已确认的问题很可能不止
下面列出的这些。

在 `TOPLINGDB_WITH_TIMESTAMP` 构建投入使用前，需要对本清单做系统性审计与补测。

## 已确认问题

### 1. fiber MultiGet 不回填 timestamps 出参

- 位置：`DBImpl::MultiGetOneCFH`（`db/db_impl/db_impl.cc`）
  - fiber 分支入口处将 `timestamps[i]` 全部 `clear()`（意图是随后回填）；
  - 但查找路径硬编码 `std::string* timestamp = nullptr;`，贯穿
    `sv->mem->Get` / `sv->imm->Get` / `sv->current->Get` 三级。
- 后果：带 timestamp 的 CF 通过单 CF MultiGet（fiber 路径默认开启）
  返回的 timestamp 永远为空，无法区分墓碑与从未写入。
- 对照：`GetImpl` 按 CF 配置正确传递 timestamp 指针。

## 相关已知点（非 fiber 分支，同属 TIMESTAMP 路径）

- `Version::GetInst`（`db/version_set.cc`）：forward bytewise 优化路径用
  `BytewiseCompare` 而非 `CompareWithoutTimestamp` 做文件边界过滤，
  TIMESTAMP 下的语义等价性未经论证（审计线索，未定性为 bug）。

## 审计与补测清单（启用 TIMESTAMP 前完成）

1. fiber MultiGet：timestamps 回填、`FailIfTsMismatchCf` / `FailIfCfHasTs`
   分支、`GetWithTimestampReadCallback` 的 Refresh 语义。
2. fiber MultiGet 与 `ReadOptions::timestamp` 快照语义的组合
   （`InitLookupKey` 已带 ts 构造 lookup key，但输出侧未验证）。
3. `ToplingMGetCtx` 在 TIMESTAMP 构建下 union `lkey`/`pikey` 的
   构造/析构配对（当前审查认为正确，缺测试固化）。
4. 为 fiber 分支补 TIMESTAMP 单元测试（当前为零覆盖）。
