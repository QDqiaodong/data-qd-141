# Unit edit sequence alignment v1

为版本数据整理者比较有序token快照，before/after各最多500个非空字符串，重复token合法且不去重。插入、删除、替换代价均1，相同匹配0；动态规划计算最小Levenshtein距离，回溯并输出可顺序重放的操作列表。每操作包含before_index/after_index游标和source/target，空侧null。
等价最优路径优先match/replace对角，再delete，再insert，保证确定性而非声称唯一最优脚本。匹配操作也保留，应用脚本必须恰消耗before并重建after；报告计数和distance。样例替换clean→validate并插入audit，距离2。
当前只处理显式token列表，不读取文件树、不检测移动、不合并并发编辑、不声称文本语义或版本控制系统等价；总计算空间O(nm)，界限500避免无界矩阵。

# 字段版本迁移计划 v1

`--plan` 新增入口，输入严格JSON顶层仅含 `before`、`after`、`consumers`：

- `before`/`after`：最多8个字段字符串，非空、数组内唯一；数组本身也不得为空。重复字段只在新入口拒绝，旧单次对齐入口仍接受重复token。
- `consumers`：1至6个对象，每个恰含唯一 `id`、`reads_before`、`reads_after`、`window`。读取字段为唯一字段数组（可为空），必须是初态/目标字段的子集，否则为非法输入。
- `window`：`[lo, hi]` 两个整数，满足 `1 <= lo <= hi <= 8`。

## 候选

复用旧入口的最小编辑距离与确定性对齐，但对齐只用于命名和排序候选：

- add集合严格为 `after` 中不在 `before` 的字段；drop集合严格为 `before` 中不在 `after` 的字段。
- 共享字段（含仅被重排者）不产生候选。
- replace仅是一个add与一个drop的显示分组（`replace_with`），候选身份仍为add/drop。
- 排序：取该字段首次出现在对齐operations数组中的操作下标，其次add先于drop，再按字段名。稳定编号为该顺序从1开始的序号。

## 阶段

计划使用连续、非空的 `1..H` 阶段，`1 <= H <= 8`。每阶段最多2项字段变更，固定顺序为：先add，再切换消费者，最后drop。切换前须存在该消费者的 `reads_before`，切换后须存在 `reads_after`；每阶段结束时未切换者仍可读 `reads_before`，已切换者可读 `reads_after`。add目标已存在、drop目标不存在均非法；共享字段只能作为对齐重排保留。新增字段必须先于首次引用它的消费者切换，删除字段必须晚于所有仍依赖它的消费者切换。替换按“新增目标字段、切换消费者、删除旧字段”处理。每个消费者必须且只能在窗口内切换一次；空闲阶段（无候选且无切换）不允许。

## 输出

结果JSON含 `status`、`plan`、`reason`，并保留 `after` 的目标顺序：

- 有解：`status` 为 `satisfiable`，`plan` 列出每阶段的 `add`/`switch`/`drop` 与阶段结束字段集合 `fields`，`reason` 为 `null`。
- 无解：`status` 为 `unsatisfiable`，`plan` 为 `null`，`reason` 给出阻塞的候选操作及其前置依赖、窗口或容量条件，搜索已穷尽至多8个非空闲阶段的所有可执行转移。

最少阶段数优先；同阶段数按每阶段“消费者ID升序列表、候选稳定编号升序列表”组成的序列逐阶段作字典序比较。合法输入无解仍原子写JSON并以0退出；输入非法或路径冲突沿用旧CLI约定，退出2且不改已有报告。验证使用独立小规模穷举对照，单个贪心死路不构成无解证据。

旧 `--source` 单次对齐入口及其输入、结果、失败和报告发布行为保持不变。
