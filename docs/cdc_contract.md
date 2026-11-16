# CDC 历史物化器契约（本地 SQLite）

仅使用 Python 标准库与 SQLite，不依赖网络服务。代码位于
`src/cdc_materializer/`，命令行入口为 `cdc-materialize`。

## 1. 批次与记录

一个批次由 `batch_id` 与记录数组组成；每条记录是一个 JSON 对象，字段集合固定：

| 字段 | 要求 |
| --- | --- |
| `event_id` | 非空字符串，全局唯一，已接纳/已拒绝的 id 不可复用 |
| `source` / `entity` | 非空字符串，标识实体 |
| `revision` | 非负整数（布尔值不接受），同一 `(source, entity, revision)` 只能有一条被接纳 |
| `op` | `upsert` 或 `retract` |
| `valid_time` | 日期 `YYYY-MM-DD` 或带偏移的日期时间（`Z`/`+08:00`），统一归一化为 UTC 文本；无时区的日期时间拒绝 |
| `value` | `upsert` 必填，必须是有限可序列化 JSON；`retract` 不允许携带 |
| `target_event_id` | `retract` 必填，指向同一实体已接纳且仍有效的 upsert 事件 |

### 逐条拒绝原因码（稳定契约，只增不改名）

`malformed_record`、`unknown_field`、`invalid_event_id`、`invalid_source`、
`invalid_entity`、`invalid_revision`、`invalid_operation`、
`invalid_valid_time`、`retract_missing_target`、`retract_has_value`、
`event_id_already_recorded`、`duplicate_event_id_in_batch`、
`revision_conflict`、`unknown_retract_target`、
`retract_target_inactive`、`retract_cycle`。

坏记录只拒绝自身，同批合法记录照常接纳。

### 幂等与冲突

- 批次内容按 `{batch_id, records}` 的规范 JSON 计算 SHA-256。相同 `batch_id`
  重投且字节一致：返回首次结果，不重复分配知识序号。
- 相同 `batch_id` 但内容不同：抛 `BatchConflictError`，库内状态不变；修正只能
  以更高 revision 追加新批次。

## 2. 双时态模型

- **知识时间**：每个有接纳记录的批次分配一个连续知识序号
  （`events.knowledge_seq`），序号只增不复用；全拒绝批次不占序号。
- **业务有效时间**：左闭右开。查询在所有 `valid_from <= as_of` 的事实中取
  最大有效起点；后一个起点在其（含）边界自然接管，前一个事实在缺口处仍可回答。
- 同一有效起点的更高 revision：关闭旧事实的知识区间（`close_seq`）并插入新行。
- `retract`：从目标事实的有效时点起使其失效，实现为关闭目标事实的知识区间，
  不插入阻断性空区间；更早的事实因此仍能填补撤回后的时间跨度。

`facts` 行只追加、只关闭，绝不更新或删除，因此历史截止的答案永久不变。

## 3. 物化查询

`query_materialized(sources, entities, as_of, known_through=None)`：

- 对每个 source × entity 显式返回一行；无有效值时 `state="empty"`、值与事件
  字段为 `null`，撤回导致的空值另附 `retracted_by_event_id` 溯源。
- `known_through` 缺省取当前知识头；只使用 `create_seq <= cutoff < close_seq`
  的事实行。
- 每次查询在独立只读连接的单个快照事务内完成；查询期间提交的新批次不会混入。
- 相同入参重跑逐字节稳定（规范 JSON、固定列序、UTC 归一化）。

## 4. 事务与恢复

一次 `ingest_batch` 的全部写入（拒绝台账、知识序号、events、facts 区间重切、
批次登记）在单个 `BEGIN IMMEDIATE` 事务内提交，全成或全败。WAL 模式下
`synchronous=FULL`；进程崩溃后未提交事务由 SQLite 回滚，重试从持久状态恢复，
任何查询都观察不到半完成批次。

## 5. 验收场景

撤回早期事实并在同一有效起点加入更高 revision 后：

- 新截止查询仅返回新事实；
- 旧截止（撤回/修订批次之前的 `known_through`）仍逐字节返回原事实；
- 逐条重放修订不重复分配序号；
- 任何查询不会混用批次提交前后的区间。
