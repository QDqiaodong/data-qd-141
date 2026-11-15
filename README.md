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
.venv/bin/python -m unittest discover -s tests -v
```

本地已有wheel时，安装构建工具可加 `--no-index --find-links <公共wheel缓存目录>`。
修改源码后重新安装。本仓不共享其他仓环境，不依赖Docker、外部账号或持续服务。

输入JSON最多8MB，拒绝重复键、非有限数值和未定义字段；源字节SHA256随结果保留。
输出是可独立读取的JSON；成功后原子替换，输入/计算失败退出2并保留旧报告。
输入和输出不得相同，输出符号链接拒绝。单操作者批处理，不承诺并发发布或断电恢复。
同输入重跑不累加。测试使用临时目录，运行产物在忽略的run/。

src/sequence_align/model.py 为业务模型，相关算法模块独立组织；common.py 提供受限JSON及报告发布，
cli.py 衔接数据链路。fixtures 为明确自制样例，代码与样例采用MIT许可。

## 当前业务与样例
# Unit edit sequence alignment v1

为版本数据整理者比较有序token快照，before/after各最多500个非空字符串，重复token合法且不去重。插入、删除、替换代价均1，相同匹配0；动态规划计算最小Levenshtein距离，回溯并输出可顺序重放的操作列表。每操作包含before_index/after_index游标和source/target，空侧null。
等价最优路径优先match/replace对角，再delete，再insert，保证确定性而非声称唯一最优脚本。匹配操作也保留，应用脚本必须恰消耗before并重建after；报告计数和distance。样例替换clean→validate并插入audit，距离2。
当前只处理显式token列表，不读取文件树、不检测移动、不合并并发编辑、不声称文本语义或版本控制系统等价；总计算空间O(nm)，界限500避免无界矩阵。

