# Unit edit sequence alignment v1

为版本数据整理者比较有序token快照，before/after各最多500个非空字符串，重复token合法且不去重。插入、删除、替换代价均1，相同匹配0；动态规划计算最小Levenshtein距离，回溯并输出可顺序重放的操作列表。每操作包含before_index/after_index游标和source/target，空侧null。
等价最优路径优先match/replace对角，再delete，再insert，保证确定性而非声称唯一最优脚本。匹配操作也保留，应用脚本必须恰消耗before并重建after；报告计数和distance。样例替换clean→validate并插入audit，距离2。
当前只处理显式token列表，不读取文件树、不检测移动、不合并并发编辑、不声称文本语义或版本控制系统等价；总计算空间O(nm)，界限500避免无界矩阵。
