# data-qd-141 · 版本序列差异对齐

Bounded minimum-edit token sequence alignment with replayable operations。业务输入、计算和当前边界详见 [数据契约](docs/contract.md)。

## 安装与演示

Python >=3.11，运行和测试仅用标准库；先确认 `python3 --version`。
每仓建立独立环境，构建工具锁版本。首次安装无公共缓存时可能联网；业务运行不访问网络。

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
.venv/bin/python -m pip check
.venv/bin/sequence-align --source fixtures/input.json --output run/report.json
.venv/bin/sequence-align --plan --source fixtures/plan.json --output run/plan-report.json
.venv/bin/python -m unittest discover -s tests -v
```

本地已有wheel时，安装构建工具可加 `--no-index --find-links <公共wheel缓存目录>`。
修改源码后重新安装。本仓不共享其他仓环境，不依赖Docker、外部账号或持续服务。

输入JSON最多8MB，拒绝重复键、非有限数值和未定义字段；源字节SHA256随结果保留。
输出是可独立读取的JSON；成功后原子替换，输入/计算失败退出2并保留旧报告。
输入和输出不得相同，输出符号链接拒绝。单操作者批处理，不承诺并发发布或断电恢复。
同输入重跑不累加。测试使用临时目录，运行产物在忽略的run/。

src/sequence_align/model.py 为业务模型，alignment.py 提供最小编辑对齐，
planning.py 提供字段迁移计划，common.py 提供受限JSON及报告发布，cli.py 衔接数据链路。
fixtures 为明确自制样例，代码与样例采用MIT许可。

## 当前业务与样例
# Unit edit sequence alignment v1

为版本数据整理者比较有序token快照，before/after各最多500个非空字符串，重复token合法且不去重。插入、删除、替换代价均1，相同匹配0；动态规划计算最小Levenshtein距离，回溯并输出可顺序重放的操作列表。每操作包含before_index/after_index游标和source/target，空侧null。
等价最优路径优先match/replace对角，再delete，再insert，保证确定性而非声称唯一最优脚本。匹配操作也保留，应用脚本必须恰消耗before并重建after；报告计数和distance。样例替换clean→validate并插入audit，距离2。
当前只处理显式token列表，不读取文件树、不检测移动、不合并并发编辑、不声称文本语义或版本控制系统等价；总计算空间O(nm)，界限500避免无界矩阵。

# 字段版本迁移计划 v1

`--plan` 是面向数据平台维护者的新入口：输入 `before`/`after`（最多8个唯一字段）与1至6个消费者，输出分段迁移计划。候选复用旧对齐脚本，严格按前后字段差生成add/drop，共享字段重排不产生候选，replace只是add+drop的显示分组。计划阶段上限8、每阶段最多2项字段变更，固定先add、再切换消费者、后drop；消费者须在窗口内恰好切换一次，无空阶段。有解输出 `satisfiable` 与逐阶段计划，无解输出 `unsatisfiable` 与阻塞条件。细节见 [数据契约](docs/contract.md)，样例见 `fixtures/plan.json`。


# 本地 SQLite CDC 历史物化器

`src/cdc_materializer/` 为数据仓库维护者提供仅依赖标准库与 SQLite 的变更数据捕获
（CDC）历史物化器。完整语义与拒绝原因码见 [CDC 契约](docs/cdc_contract.md)。

能力摘要：

- 按 `source`/`entity`/`revision` 接纳 `upsert`/`retract` 批次；坏记录逐条拒绝
  （稳定原因码），不拖垮整批；已接纳事件不可改写，修正只能追加更高 revision。
- 批次按内容哈希幂等重投；同 id 不同内容报 `BatchConflictError`。
- 每个成功批次分配连续、持久、重试不重复的知识序号；支持按 `event_id` 与
  `(source, entity, revision)` 查询接纳/拒绝结论。
- 双时态区间表只追加、只关闭：修订关闭旧知识版本并重切区间，旧 `known_through`
  查询永久不变；查询走 WAL 快照，绝不混用提交前后的区间。
- 接纳、序号、区间重切、批次登记在单个 `BEGIN IMMEDIATE` 事务内全成或全败，
  崩溃后重试可恢复，查询观察不到半完成批次。

```sh
PYTHONPATH=src python3 -m cdc_materializer.cli ingest \
    --db run/cdc.db --batch-id b1 --input fixtures/cdc_batch_1.json
PYTHONPATH=src python3 -m cdc_materializer.cli ingest \
    --db run/cdc.db --batch-id b2 --input fixtures/cdc_batch_2.json
# 业务时点 + 知识截止两个维度；省略 --known-through 即取当前知识头
PYTHONPATH=src python3 -m cdc_materializer.cli query --db run/cdc.db \
    --sources orders --entities order-7 \
    --as-of 2026-03-01T00:00:00+00:00 --known-through 1
PYTHONPATH=src python3 -m cdc_materializer.cli event --db run/cdc.db --event-id evt-0003
PYTHONPATH=src python3 -m cdc_materializer.cli revision \
    --db run/cdc.db --source orders --entity order-7 --revision 4
```

编程接口：

```python
from cdc_materializer import Materializer

with Materializer("run/cdc.db") as mat:
    mat.ingest_batch("b1", records)                 # 含逐条 accepted/rejected
    mat.query_materialized(["orders"], ["order-7"],
                           "2026-03-01T00:00:00+00:00", known_through=1)
    mat.lookup_event("evt-0001")
    mat.lookup_revision("orders", "order-7", 4)
```

测试（含乱序到达、重复批次、冲突、撤回链/环、空结果、两个截止维度、随机化
oracle 对照与事务失败恢复）：

```sh
PYTHONPATH=src python3 -m unittest tests.test_cdc_materializer -v
```


