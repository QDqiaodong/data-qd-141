# 小文件压实与快照安全回收契约（lake_compactor）

仅使用 Python 标准库与 SQLite，不依赖网络服务。代码位于
`src/lake_compactor/`，命令行入口为 `python -m lake_compactor.cli`
（安装后为 `lake-compactor`）。旧对齐、字段计划、CDC、三方合并入口的行为不变。

## 1. 布局与所有权

一个湖（lake）住在一个工具独占目录 *home*：

```
<home>/catalog.db          SQLite 目录册（WAL，synchronous=FULL）
<home>/files/data/         不可变数据 JSONL 文件（d000001.jsonl …）
<home>/files/deletes/      不可变 delete JSONL 文件（x000001.jsonl …）
<home>/files/tmp/          在途写入（目录册绝不引用）
```

**行内容只存在于 JSONL 文件**：目录册只记录文件元数据、快照成员关系、
delete 的存储位置（文件 + 行号）与 pin，绝不保存 value，因此目录册不可能
替代湖文件。home 内的一切文件归工具所有；`gc` 只清理 `files/` 下的文件，
绝不触碰外部源文件。

## 2. 快照与序号

- `init` 创建空快照 0 与目录册；重复 init 拒绝。
- 每次成功的 `append`/`compact` 生成单调递增的 `snapshot_id`（头 = 旧头 +1），
  并以 `expected_snapshot` 校验当前头，失配（stale）整体不提交。
- 每次成功 `append` 分配连续递增的 `append_seq`（即 `data_seq` 来源）；
  失败的 append 不占号；`compact` 不占号。

## 3. append：清单与导入

清单复用仓库严格 JSON 读取（`sequence_align.common.read`：≤8MB、拒绝重复键、
非有限字面量），顶层恰含 `data_files` 与 `deletes` 两个数组：

- `data_files[]` 恰含 `partition`/`path`：`partition` 为 ident（非空、≤128、
  无控制字符、无首尾空白）；`path` 为相对清单所在目录的 JSONL 路径（拒绝绝对
  路径）。源文件拒绝符号链接；解析后的源路径不得落入 home（目录册在 home 内，
  一并拒绝）；源必须存在且为普通文件。
- 数据文件逐行校验：每行恰含 `key`/`value`；`key` 为 ident；`value` 为有限
  JSON（拒绝重复键、NaN/Infinity 字面量及 `1e999` 这类溢出非有限数）。
  **任何非法行使整次 append 失败**，不占 append 序号，已复制文件被清理
  （gc 兜底）。
- `deletes[]` 恰含 `partition`/`key`/`cutoff`：`cutoff` 为 0 至本次 append
  序号的整数（拒绝布尔）。

导入时源文件**逐字节复制**进 `files/data/`，记录字节数与 SHA256；每行获得
不可变身份 `(origin_file_id, 原行号, data_seq)`：导入文件的行身份 = 自身
file_id + 行号 + 本 append 序号。重复 key 是不同数据行，绝不合并。

本次 append 的 delete 集合写成工具拥有的不可变 JSONL 文件（空集合也写空
文件），每行 `{"delete_id","partition","key","cutoff"}`，`delete_id` 全局
单调、永久稳定。新快照通过 `snapshot_deletes` 关联**引用原 delete 文件并
记录有效条目**；旧快照的删除内容永不改写。

## 4. compact：文件重写器

`compact(partition, files, expected_snapshot, target_rows)`：

- 只重写**指定分区中显式选定的至少两个当前 data 文件**；其他分区与未选
  文件绝不重写。选择校验：≥2 个、不重复、都是当前头文件、都属于该分区。
- 删除谓词仅作用于 `partition`、`key` 相同且 `data_seq <= cutoff` 的行。
- 输出保留所有未被当前快照 delete 删除的选中行及其原始身份与 `data_seq`
  （**不得改成压实时间**），按 `partition/key/data_seq/origin_file_id/
  原行号` 确定性排序，按 `target_rows` 切块成多个文件；全部删除时允许
  零输出文件。压实输出是自描述 JSONL：每行恰含
  `{"key","value","origin_file_id","origin_line","data_seq"}`。
- 新快照**恰保留**那些仍命中任一**未选** data 文件原始行（按存储行而非
  可见行判定）的 delete 谓词，其余从新快照移除；旧快照不改写。因此不能
  仅凭本次输出没有匹配行就丢掉仍保护未选文件的 delete；新导入的同 key
  行因更大 `data_seq` 不受旧 delete 影响。

## 5. pin / scan / unpin / expire

- `pin(snapshot_id)`：接受尚未过期的快照（当前头永不过期），返回持久
  token（`pin-` + 32 位十六进制），落库保存，关闭进程再 scan 仍有效。
  快照不存在报 unknown，已过期报 expired——与 expire 竞争时只会得到
  完整有效 token 或明确过期错误（两者都在单个 `BEGIN IMMEDIATE` 内判定）。
- `scan(token)`：仅接受有效 token，返回该固定快照的可见行（含原始身份
  与 `data_seq`），按确定性顺序排序。pin 成功后直到 unpin，scan 即使在
  打开第二个数据文件前暂停，也不会因 gc 缺文件。
- `unpin(token)`：释放 pin，token 立即失效；未知 token 报错。
- `expire(snapshot_id)`：仅使**非当前**快照不可再 pin；既有 pin 继续可读；
  不设自动租期，管理员显式 unpin。对当前头、未知或已过期快照报错。

## 6. gc：依赖追踪与回收

`gc` 在单个写事务内计算可达集并删除文件：

- **受保护** = 当前头 ∪ 未过期快照 ∪ 任一有效 pin 引用快照的 data 文件及
  delete 依赖（经 `snapshot_data_files` / `snapshot_deletes` /
  `delete_entries` 解析到 delete 文件）。`origin_file_id` 只是行级溯源，
  **不**构成保活依赖。
- `files/` 下不在可达集内的文件一律删除：过期未 pin 快照的私有文件、
  崩溃留下的未提交孤儿（tmp 与已改名未提交文件）。正常完成后所有此类
  不可达文件都被删除——不靠永久保留压实输入满足安全性。
- 同事务内剪枝不可达快照的成员关系与无引用的文件/条目目录行；快照头行
  永久保留（一行历史 + 稳定错误）。文件删除幂等、目录剪枝可重跑，gc 在
  任意点被杀死，重开后再跑一次即可收敛。

## 7. 发布协议与崩溃恢复

每次变更（append/compact）在**一个 `BEGIN IMMEDIATE` 事务**内完成：新文件
先写 `files/tmp`、fsync、改名到最终位置、目录 fsync，再插入目录行并切换
头，最后 COMMIT。COMMIT 是唯一原子发布点：之前读者看到旧头，之后看到文件
已完整落盘的新头。

- 崩溃点 A（文件已落盘、头未切换）：WAL 回滚未提交事务，重开后当前头完整
  可读；磁盘上的孤儿文件不被任何快照引用，由 gc 实际删除（不断言 SQLite
  回滚能撤销外部文件）。
- 崩溃点 B（头已切换、旧文件未清理）：新头完整可读；旧文件是否删除完全由
  后续 expire/gc 决定。
- 因为文件产生发生在写事务内，gc（自身也持写锁）绝不可能看到活跃写入者
  的半成品：gc 运行时，磁盘上不在目录册中的文件一定属于已死写入者。
  多进程可同时 pin/compact/append/expire/gc——写路径在数据库锁上串行，
  scan 走 WAL 只读快照 + 内容文件，从不阻塞。

## 8. 完整性与损坏

每次读取工具文件（scan/compact/snapshot_info）都按目录册 SHA256 校验：
缺失、哈希不符、行结构非法、行数不符都抛稳定的 `CorruptionError`
（`corrupt lake file <file_id> (<path>): <原因>`），绝不静默少行。

## 9. 正确性代价（已接受）

- 写者在文件复制期间持有 SQLite 写锁：append/compact 相互串行，长复制会
  推迟并发 pin/expire/gc 的提交（在锁上排队）；scan 不阻塞。
- compact 为精确判定 delete 存活性需重读当前头的全部未选文件，代价是
  O（湖大小） 而非 O（选中文件）。
- 回收只在管理员运行 gc 时发生，无后台线程；pin 无租期，遗忘的 pin 会
  永久保护其快照（设计使然）。
- 快照头行永久保留（每 append/compact 一行小记录），换取 pin 能区分
  “未知快照”与“已过期快照”。

## 10. 测试钩子（仅测试使用）

环境变量 `LAKE_COMPACTOR_FREEZE_AFTER_FILES` /
`LAKE_COMPACTOR_FREEZE_AFTER_COMMIT` / `LAKE_COMPACTOR_FREEZE_AFTER_GC_DELETE`
指向标记文件时，进程在对应点落标记后睡眠等待被杀死，用于真实进程退出
测试；`LAKE_COMPACTOR_SCAN_FILE_DELAY` 让 scan 在读取第二个及之后的数据
文件前睡眠指定秒数，用于 pin/gc 交错测试。生产环境绝不设置。
