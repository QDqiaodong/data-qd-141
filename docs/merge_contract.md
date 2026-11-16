# 三方版本合并工作台 v1

`python -m sequence_align.merge_cli`（安装后也可用 `sequence-merge`）为维护有序
ident 清单的团队提供三方合并：`create`、`refresh`、`resolve`、`inspect`、`export`
五个子命令，持久状态放在单独的 SQLite 工作区（`--db`），与旧对齐、字段计划、CDC
入口完全分开；旧入口的输入、结果和发布行为不变。

输入 JSON 复用 `sequence_align.common.read`：最多 8 MB、拒绝重复键、非有限数值和
未定义字段，源字节 SHA256 随状态保存。数据库路径不得作为 JSON 输入或输出，输出
沿用原子替换、路径冲突与符号链接拒绝；`inspect`/`export` 只读，`export` 不改工作区。
`create`/`refresh`/`resolve` 的 `--output` 先写入同目录临时文件，数据库 COMMIT 后
才原子替换；临时写入失败会使事务回滚，旧报告与工作区都保持不变。JSON 结构校验在
打开数据库之前完成，被拒绝的文档不会创建工作区文件。

## 输入

`create` 的 JSON 顶层恰含 `base`、`left`、`right`、`rules`：

- `base`/`left`/`right`：ident 字符串数组（非空、长度 ≤128、无控制字符），各最多
  500 项，可为空，重复合法。
- `rules`：最多 32 个对象，每个恰含 `kind`/`a`/`b`；`kind` 为 `requires` 或
  `before`，`a`/`b` 为 ident。

`refresh` 的 JSON 顶层恰含 `left`、`right`（规则同上）；`base` 与 `rules` 不可改变。
`resolve` 输入 `{"decisions": [...]}`（也接受裸数组或 `{"decisions": {id: choice}}`
映射）；每个裁决恰含 `conflict_id`、`choice`，manual 另需 `tokens`（ident 数组，可为
空，最多 500）。同批内 `conflict_id` 不得重复。

`refresh`/`resolve` 必须携带 `expected_revision`（`--expected-revision`，或与其它字段
同级的 `expected_revision`；两处同时给出且不同即失败）。

## 分析

复用 `sequence_align.alignment.align` 的距离、确定性路径与游标语义，分别对齐
`base→left` 与 `base→right`；绝不按 token 值集合判断修改。每段连续非 `match` 操作
归为一个 hunk，记录消耗的 base 半开区间 `[l,r)` 与该段产生的替换数组；纯插入为
`[g,g)`。仅在不同侧 hunk 之间连边：两非空区间交集非空；某纯插入 `g` 落在另一非空
区间闭区间 `[l,r]` 内（插入落在删除端点也算）；两次纯插入同 `g`。

取连通分量，范围为成员端点最小包络；在范围的 base 切片上分别应用该侧全部成员
hunk 得到左右投影。只含一侧的分量自动应用；两侧投影完全相同的分量只保留一份；
否则为待裁决冲突。分量之外的 base 按原序保留，不同位置上的相同 token 不合并。
`create`/`refresh` 结果超过 12 个冲突即退出 2 且不改状态。

## 冲突身份

`conflict_id` = 确定性规范 JSON 的 SHA256（UTF-8、对象键排序、`","`/`":"` 分隔、无
额外空白、非 ASCII 不转义）：
`{"base_sha256": 固定 base 内容摘要, "base_range": [l,r],
"hunks": [按侧（left<right）与 base 坐标排序的全部成员 hunk 的
side/base_range/replacement]}`。不含侧边目标下标、分量序号或 left/right 整体摘要。
因此刷新中其它位置的新增修改不会使同一冲突的身份变化，而成员或投影改变、分裂、
合并或转为自动分量时旧身份消失，旧裁决随之失效——不做按序号继承，也不因全局
输入摘要变化而丢弃全部裁决。

## 规则

规则作用于完整合并清单，重复项不去重：

- `requires(a,b)`：清单中出现 `a` 就必须出现 `b`；`a` 缺席时为真。
- `before(a,b)`：两者都出现时每个 `a` 都必须在每个 `b` 之前（最后一个 `a` < 第一个
  `b`）；缺任一者时为真。

`inspect` 输出 `feasible`（存在满足全部 rules 与当前有效裁决的完整清单）以及每个
未决冲突 `left`/`right` 各自是否存在这样的补全；补全只从其他未决冲突的左右投影
选取（manual 是已确裁决，不算臆造）。`resolve` 整批应用后仍需存在完整合法补全才
提交，否则整批拒绝并保持旧 revision。刷新允许产生 `feasible=false` 的待审状态，
仍按局部身份保留裁决，不能用规则校验清空决定。`export` 只在全部冲突已裁决且满足
rules 时按 base 顺序应用自动分量与裁决，输出完整 `tokens` 和来源分段 `sources`；
未决时退出 2 并保留旧输出。

## revision 与并发

`create` 从 `revision=0` 开始；每次成功 `refresh`/`resolve` 递增一次。多个独立进程
可竞争同一工作区：状态转移在单个 `BEGIN IMMEDIATE` 内重新读取 revision、重算分量、
推导继承/失效裁决并写入 revision、输入与裁决，全成或全败。陈旧
`expected_revision`、任意失败（含提交前注入失败）都退出 2 且不改变持久状态。
`inspect`/`export` 用只读连接在单个快照事务中读取同一已提交状态，绝不把刷新后的
分量与刷新前的裁决拼接。
