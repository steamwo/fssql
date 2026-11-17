# fssql 项目测试规范与 DuckDB 对标方案

- 标准版本：1.0
- 制定日期：2026-11-04
- 状态：项目测试与发布验收标准
- 适用范围：引擎、SQL、内建函数、公共 Go API、格式读写、VFS、导出、资源管理，以及 DuckDB 替换能力

## 1. 标准效力与完成定义

本文细化 [Testing Contract](contracts/testing.md) 和 [Release Verification Contract](architecture/release-contract.md)。架构不变量、内存契约和格式契约继续有效；测试不能通过修改预期结果规避这些约束。

“必须”是硬性要求；“目标”是性能建设目标；“可选”不属于当前版本必需范围。本文定义测试要求，不表示相关自动化、用例或测量已经存在。已实现、已运行、已验收必须分别记录。

一项能力完成验收必须同时满足：

1. 能力及适用范围有登记，能够追溯到稳定用例 ID。
2. 正常、边界、错误和相关组合场景有断言。
3. 有 DuckDB 对应语义的能力完成固定版本差分验证。
4. fssql 独有 API、资源和生命周期行为完成自身契约测试。
5. 正确性、内存、失败清理和适用的性能门槛全部通过。
6. 保存可复现的输入、配置、原始结果、版本与报告。

`go test ./...` 成功、单个示例成功、仅行数相同、仅覆盖率达标，都不能代替以上条件。

### 1.1 结果状态

| 状态 | 定义 | 必需能力能否发布 |
|---|---|---|
| PASS | 实际执行且所有断言及门槛通过 | 可以 |
| FAIL | 错误结果、崩溃、超限、泄漏或性能超过门槛 | 不可以 |
| BLOCKED | 缺依赖、参考版本、服务、样本或有效测量环境 | 不可以 |
| NOT_RUN | 没有执行证据 | 不可以 |
| UNSUPPORTED | 明确不在本版本范围，并验证可预测地拒绝 | 不计为支持 |
| ACCEPTED_DEVIATION | 经评审登记的有意契约差异，仍完成两侧行为测试 | 按登记范围判断 |

测试框架的 SKIP 必须在汇总报告映射到上述状态。发布必需测试缺失、全部跳过、筛选表达式匹配零个测试或 benchmark，均不得返回验收成功。开发环境可以跳过外部依赖测试，但报告必须显示未验证范围。

### 1.2 差异登记

复用 `compatibility/deviations.yaml`：`bug` 不能豁免失败；`unsupported_feature` 不能豁免已经声明支持的能力。`intentional_deviation` 必须关联契约、具体用例、参考版本、原因和影响范围。已与参考一致的过时差异必须清理。

性能豁免单独保存在该轮验收报告，必须包含用例、测量证据、影响、评审记录、关联问题和到期版本；不能改变原始测量值，也不能把豁免标为测量通过。

## 2. 参考引擎与环境管理

### 2.1 参考版本

- Stable SQL 基线以 `compatibility/deviations.yaml` 的 `reference.version` 为准；本文制定时为 DuckDB `2.0.0`。
- Python 对标环境以 `test/integration/requirements.txt` 为依赖锁定入口；运行前必须校验实际 DuckDB 版本与对应测试基线相同。
- 替换业务另设调用方兼容档案，固定调用方实际 DuckDB、绑定库、自编译扩展版本及扩展 SHA-256。不得用通用 Python DuckDB 通过代替自编译加密扩展通过。
- 两套参考基线分别汇总，不能把不同版本的结果拼成一份通过报告。
- 基线升级必须单独提交：先在旧基线回归，再运行新旧参考差分，记录语义变化，最后更新依赖、预期结果及登记。不得自动使用 latest 或缺失时降级。
- DuckDB 不可获取或版本不匹配时标记 BLOCKED，不能伪造 golden 或使用 fssql 生成自己的参考答案。

参考引擎和 Python 仅作为测试依赖，不改变生产纯 Go、内部 Vector/DataChunk ABI 的约束。

### 2.2 每次运行必须记录

记录 fssql commit、dirty diff 摘要、参考引擎版本、扩展摘要、Go/Python/绑定版本、依赖锁摘要、OS/内核/架构、CPU 型号及可用核数、RAM、存储类型、文件系统、容器限制、CPU 配额、线程数、内存及临时存储配置、时区、locale、fixture ID/摘要、随机种子、命令和退出状态。

默认语义环境固定为 UTC；其他时区是独立测试维度。性能机器必须排除其他负载、后台 fixture 生成和并行 benchmark。Windows 与 Linux 分别建立基线，不能跨机器或跨 OS 计算回归比率。

配置及报告不得记录凭据、HMAC 密钥、Parquet 密钥或生产数据。fixture 使用合成数据；测试密钥明确标记为公开测试材料。

## 3. 能力、函数和方法覆盖管理

### 3.1 覆盖清单

维护三份版本化清单，作为后续自动化建设交付物：`test/corpus/features.json`、`test/corpus/functions.json`、`test/corpus/public-api.json`。本文不假定这些文件或校验器已经实现。

每条记录至少包含：`id`、能力或符号、支持状态、适用类型/签名、引入版本、优先级、关联契约、参考档案、用例 ID、验证层次、执行入口、负责人角色、已知限制。

- Feature 清单覆盖 SQL 语法、执行算子、格式选项、协议 capability 和处理规则。
- Function 清单按函数名、重载签名、参数形式登记，不能只按名称计数。CAST、CASE、窗口和运算符也作为语义能力登记。
- Public API 清单覆盖可由外部模块访问的函数、方法、接口方法、配置字段和数据类型；不把未导出接收者上的导出方法误算为公共 API。
- 由 Go AST 和函数目录生成符号清单，再人工关联契约与测试。生成清单不能自动生成“正确预期”。
- 每次新增、删除或调整签名必须同步清单。声明支持却没有测试关联时，覆盖校验失败。
- 使用如 `SQL.CAST.DOUBLE_BIGINT.ROUND.001`、`API.BATCH.NEXT_EOF.001`、`PERF.PARQUET.PROJECT_ONE.L` 的稳定 ID；不能仅依赖可能变化的代码行号。

必需能力覆盖率为“具备全部规定场景且通过的条目 / 本版本必需条目”。NOT_RUN、BLOCKED、只有单元测试的条目不能进入已验收分子。语句覆盖率是辅助指标，不代替能力覆盖率。

### 3.2 对标方式

| 对象 | DuckDB 对标内容 | fssql 自身必须验证 |
|---|---|---|
| SQL/函数/运算符 | 结果值、类型、行数、NULL、错误和有承诺的排序 | 表示等价、优化及 spill 等价 |
| 读取/导出格式 | 同一数据的解码值、schema、外部读取结果 | 内存记账、快照、提交及失败清理 |
| `Open`、配置、`Close` | 对应资源配置下执行同一查询；不比较 API 名称 | 无效配置、默认值、幂等性、关闭后的行为 |
| `QueryChunks`、Chunk 访问器 | 同一逻辑值及类型的原生/行式结果映射 | 借用期、越界、错误类型、NULL 与空值 |
| `OpenSQLDB` 与 database/sql | 值、ColumnTypes、错误及声明的适配差异 | Rows.Close、取消、参数/事务等边界 |
| `Exec`、`CopyTo` | SQL 效果、导出行数/值/schema | 原子发布、失败结果、密钥解析时机 |
| TableFunction、BatchReader/Writer | 将相同 schema/值装入 DuckDB 的等价来源后比较查询 | Next/EOF、复制借用数据、只开一次、重扫、关闭 |
| ScalarFunction/HMAC | 等价函数或独立已知向量，比较类型和值 | NULL 策略、确定性、租户隔离、错误传播 |
| FileURI、本地 VFS、MemoryUsed | 没有直接等价 API，不伪造方法级差分 | 路径/边界、capability、预算、实际 heap/RSS |

每个公共方法必须有成功、边界、错误及适用的生命周期用例；纯元数据方法不强制做无意义性能对标。方法影响查询结果时，必须通过公共生产入口完成端到端对标。

## 4. 差分结果比较规范

### 4.1 比较流程

同一 fixture → 分别建立环境 → 保存绑定后的 schema → 全量消费结果 → 规范化 → 比较 → 检查资源清理。必须核对列数、行数及每列类型后比较值，不能只比较扁平字符串数组。

小结果保存完整预期；大结果按有类型标签和长度边界的编码计算摘要，同时保留行数、schema 摘要及诊断样本。无序结果使用排序后的规范行或保留重复次数的分区比较，不能去重，也不能仅 XOR 行哈希。

仅在明确要求输出顺序时逐行比较。`ORDER BY` 存在相同键但缺少完整 tie-breaker 时，只比较已承诺的有序分组；无 ORDER BY 不以当前物理输出顺序作为语义。

### 4.2 类型规则

| 类型 | 规范比较规则 |
|---|---|
| NULL/空值 | NULL 使用独立标签；文本 `NULL`、空串、空 BLOB 均不能与 NULL 合并 |
| 有符号/无符号整数、HUGEINT | 精确整数比较，保留逻辑位宽及符号；禁止经过浮点 |
| DECIMAL | 精确系数与 scale；值及结果 typmod 分别断言，不能通过去掉尾零掩盖类型不符 |
| FLOAT/DOUBLE | 有限值按 `abs(a-b) <= atol + rtol*max(abs(a),abs(b))`；FLOAT 默认 atol/rtol=1e-6，DOUBLE=1e-12 |
| 浮点聚合 | 仅有独立误差依据的用例可显式使用 rtol=1e-9；更宽容差必须逐用例解释，禁止全局放宽 |
| NaN/Infinity/负零 | 单独验证类别及 DuckDB 的排序、比较、转换语义；符号可观察时保留负零符号 |
| VARCHAR | UTF-8 字节及所需字符语义；不做默认 trim、大小写转换或 Unicode 归一化 |
| BLOB | 原始字节；报告可编码为 hex/base64，不能转文本比较 |
| DATE/TIMESTAMP | 日期或带明确单位的整数时间；TIMESTAMP_NS 保留全部纳秒，日历时间与绝对时间点分开 |
| TIMESTAMPTZ | 比较 UTC 时间点并独立验证显示/转换规则，不以不同时区的字符串直接比较 |
| LIST/STRUCT | 递归比较类型、顺序、字段名、NULL；不支持的公共访问方式必须验证明确拒绝 |

容差只用于允许近似的浮点结果，不能用于整数、DECIMAL、抽样哈希、配额、HMAC、BLOB 或时间精度。

Python `datetime` 及某些绑定可能丢失纳秒；参考侧必须通过精确 epoch/unit 投影或保真交换通道取得整数值。不能以转换到 VARCHAR 或先降到微秒来证明原类型保真。

### 4.3 错误比较

比较成功/失败、错误类别和关键上下文，不要求跨引擎错误文本逐字一致。类别至少包含语法、绑定、类型、转换、溢出、除零、输入损坏、I/O、取消、资源超限、权限和不支持。

两侧都报错不自动通过；必须验证确实因预期原因失败。fssql 的 panic、进程退出、死锁、超时不属于正常 SQL 错误。会导致进程退出的回归必须放在子进程测试，父进程断言退出状态及错误传播。

## 5. 功能与函数必测矩阵

### 5.1 SQL 与算子

| 能力 | 必测场景 |
|---|---|
| SELECT/投影/别名 | 常量、列、表达式、重复引用、零行、全 NULL、列顺序及实际类型 |
| WHERE/布尔逻辑 | TRUE/FALSE/UNKNOWN、AND/OR/NOT、比较、IS NULL、短路相关错误分支 |
| CAST | 支持的源/目标组合、精度、正负、半值舍入、上下界、NaN/Inf、时间单位、非法文本 |
| CASE/COALESCE/NULLIF | 命中各分支、NULL 分支、结果统一类型、未选分支不求值、嵌套 |
| GROUP BY/聚合 | 空输入、全 NULL、单组/多组、重复/倾斜键、溢出、宽数值、结果类型 |
| DISTINCT | NULL、重复次数、混合类型、变量长度、内存与 spill 等价 |
| JOIN | 每个已支持 join 类型，空侧、NULL、重复键、多键、倾斜、残余谓词、组合 NULL 安全条件 |
| ORDER/LIMIT/OFFSET | ASC/DESC、NULLS FIRST/LAST、相同键、0/1/超总数、TopN/完整排序及强制 spill |
| CTE/派生表/子查询 | 作用域、列名、嵌套、重复引用、空结果、基数错误、易变表达式只物化一次 |
| 窗口 | 各已支持函数/frame，跨批次分区、排序键、空帧、全 NULL、多种窗口顺序、文件/注册来源 |
| DDL/元数据 | 每个支持语法、重复对象、未知对象、schema 正确、DESCRIBE 不消费来源 |
| 优化器 | 开启优化与参考计划结果相等；文件投影/谓词/LIMIT 的真实 I/O 证据 |

新增能力如果只支持子集，清单必须记录子集并测试其余形式明确拒绝。例如窗口“支持”不能由一个 ROW_NUMBER 用例推导到全部窗口函数。

### 5.2 函数、操作符和时间语义

当前必需函数按下表逐项建立签名清单；以后函数目录增加条目时自动进入覆盖检查。

| 函数组 | 必需项 | 特有边界 |
|---|---|---|
| 字符串 | length、substr、strpos、replace、repeat、lower、hex、`||` | 空串、中文/emoji、字符/字节边界、起点及长度、NULL、超大重复数、输出长度溢出 |
| 数值 | floor、mod、least、greatest、`+ - * / %`、一元正负 | 正负、零、极值、除零、DECIMAL scale、浮点特殊值、混合类型 |
| 条件 | CASE、coalesce、nullif | 三值逻辑、统一结果类型、无效分支、特殊 NULL 策略 |
| 哈希/密码 | md5、HMAC 标量及注册函数 | 独立已知向量、UTF-8、空输入、NULL、密钥不存在、密钥隔离 |
| 时间 | strftime、strptime、date_trunc、timezone、时间 CAST | epoch 前后、闰日、跨日、全部支持格式符、UTC、单位转换、边界溢出 |
| 聚合 | count、sum、avg、min、max | COUNT(*)/COUNT(col)、空/NULL、精确结果类型、分组及 spill |
| 窗口 | row_number、窗口 count/sum 及声明的 frame | 多批次、分区变化、窗口与最终 ORDER BY 不同、COUNT-only 文件扫描 |
| 随机 | random、reservoir、bernoulli 及业务抽样 | seed=0、无种子、重复行、概率 0/1、样本大于总体 |

每个支持签名至少覆盖：普通值、NULL、边界/极值、非法输入、嵌套表达式、相关隐式转换。类型差异显著的重载不能用同一个字符串或整数用例代替。

向量表达式必须验证 FLAT、CONSTANT、DICTIONARY 等价；跨批次测试至少覆盖 0、1、V-1、V、V+1、2V+1 行，V 为引擎实际 vector size。状态性算子不能仅测一个批次。

### 5.3 随机与业务处理

- 哈希抽样和具有完整排序键的确定性抽样：固定输入、种子和规范化规则，精确比较成员及规定的顺序。
- 原生随机抽样：比较数量、取值范围、同种子/同快照顺序复现和统计性质，不要求与 DuckDB 选中同一组成员。
- Reservoir：样本大小=min(k,n)、无越界、按来源行身份验证无重复抽取；来源本身的重复值允许同时出现。
- Bernoulli：重复业务键和重复整行仍是独立事件。概率 0/1 使用精确断言。
- 统计验收固定生成器和种子集合：PR 至少 10 个种子×10,000 行；发布至少 100 个种子×10,000 行。对合并的入样总数使用预先定义的二项分布界限（默认 6 个标准差），不得因失败更换种子。p=0/1 不使用近似界限。
- 分层/整群/系统抽样：覆盖空总体、饱和小层、余数分配、多维 NULL 键、比例和固定数量、重复键、稳定 tie-breaker。
- 脱敏：各规则、同字段规则链顺序、抽样与脱敏顺序、Unicode、全 NULL、原始类型恢复；来源不支持的规则仍必须明确拒绝。

### 5.4 公共 API、格式和来源

- Chunk：每个访问器验证有效值、NULL、错误类型、越界、空批次、宽整数和时间单位；借用值只能在声明的生命周期内使用。
- BatchReader：0 行、末批同时返回数据和 EOF、非 EOF 错误、取消、Close、共享字节切片复用、多次引用、提前结束及 schema 冻结。
- BatchWriter：每个 setter 覆盖类型、NULL、精度及范围、行列边界、字段及批次预算。
- UDF：签名/arity、NULL 策略、确定性/易变性、错误传播、取消及请求隔离。用独立参考函数验证算法，不能只做自身往返。
- CopyTo/SQL COPY：最终行数/字节数/schema、空输出、文件存在、失败零结果、原子发布、关闭/提交失败、不可读的中间文件不被公开。
- CSV：表头、显式列类型、推断、quoted NULL、空串、引号内换行、BOM、CRLF、无末尾换行、自定义分隔符/引用字符、BLOB 及 NullString 碰撞。
- JSON/NDJSON：NULL、缺字段、类型变化、跨块记录、嵌套、损坏尾部及过大字段。
- Parquet：物理与逻辑类型、DECIMAL、unsigned、时间单位/UTC、NULL、页/行组边界、多文件 schema、每种声明支持的 codec/encoding、损坏元数据。
- VFS：本地与 S3 公共路径，以及声明支持的内部协议，按 capability×读写×失败组合验证；不能根据 fake driver 通过推断真实服务通过。
- 原生加密：AES-128/256、现有自编译扩展/实际下游互读、keyId 元数据、错误/缺失密钥、篡改、重试 fresh nonce、压缩加密组合。仅 AES 帧自解密或检查 PARE 前缀不算格式验收。

## 6. 资源、失败与性质测试

### 6.1 内存及临时存储

正常执行、取消、消费者错误、来源错误、UDF 错误、格式错误、内存耗尽、临时配额耗尽和提交错误后，断言 query reservation、任务、I/O、来源 lease、句柄、未完成写入及 query 临时所有权释放。

必须同时测 managed peak、Go heap live/alloc、RSS peak、GC、temp peak；MemoryUsed=0 只证明受管记账归零。大字符串/REPEAT/UDF 输出必须验证先记账再分配，不能在返回 memory-limit 错误前已经分配超大 Go heap。

默认资源档案：受管内存 64 MiB、256 MiB；spill 正常与强制档案分别使用明确配置，不能将禁用 spill 伪称强制 spill。临时预算固定记录；超预算场景预期失败。扫描大文件的缓冲必须有界，footer 等必要元数据单独受限并报告。

### 6.2 失败矩阵

| 注入位置 | 必测失败 | 核心断言 |
|---|---|---|
| 网络/VFS | 超时、短读、断连、对象身份变化、权限失败 | 不混合快照；只按许可重试；释放请求 |
| 文本/Parquet | 分块边界损坏、截断、非法长度/offset | 不越界、不 panic、不无限分配 |
| 来源/UDF | 错误、阻塞后取消、借用区复用 | 停止消费、Close、错误保留上下文 |
| 内存/spill | Reserve/Resize 失败、磁盘满、配额耗尽、损坏 spill | 明确错误、无泄漏、强制 spill 正常结果等价 |
| writer/commit | 写入、flush、close、rename/发布、manifest 保存失败 | 无不完整最终产物、原完整目标按契约保留 |
| 调度/并发 | 慢消费者、队列满、多个 worker、DB.Close 交错 | 不死锁、取消有效、所有权只释放一次 |

恢复测试验证文件级重做、manifest 身份、输出 schema/选项变化及密钥配置标识变化；不能恢复时静默重读一个不可重放来源。

### 6.3 性质、模糊测试和独立预期

必须覆盖序列化往返、相等值具有相等哈希、向量表示等价、优化/参考结果等价、单/多 worker 等价、内存/强制 spill 等价。比较的是完整类型和值，不只计行。

Fuzz 重点为 SQL 词法/解析、CSV 引号边界、JSON framing、Parquet 元数据/页面、spill 解码。失败最小化后保留固定回归；不把 fuzz 的“未崩溃”视为语义证明。

全量 Race 在支持的平台执行，Race/coverage/profile 结果不能进入正式性能样本。

## 7. DuckDB 性能对标规范

### 7.1 必须分开的性能档案

1. **SQL/API 端到端**：打开完成后的查询提交→完整消费；记录客户端绑定、消费方式、批次和传输成本。
2. **数据库/来源导入**：从同一逻辑来源导入相同类型数据，分别报告导入、处理及导出阶段；不能把 CSV 中转与类型化导入混作同一档案。
3. **文件读取**：两侧读取同一协议、同一服务、同一对象集合及物理字节。
4. **导出**：同输入/schema、同格式/codec/压缩级别、相当的 row-group/page 参数；时间包含 flush、close 和承诺的提交。
5. **fssql microbenchmark**：只用于自身 kernel、分配和回归分析。Go `ns/op` 不能直接除以 DuckDB 的整条 SQL 耗时。

Python fetchmany 与 Go Chunk 回调的结果只能标为包含绑定成本的端到端对比。两侧都必须完整消费，不能一侧逐行 SHA/字符串化，另一侧只取首批或记录计数。正确性摘要在独立轮次执行；不能计时 `COUNT(*)` 代替计时原查询的完整结果。

编译、pip 安装、fixture 生成及上传不进入查询计时。启动、初始化、扩展加载和 schema 绑定成本另列；如果计入端到端档案，双方必须采用相同边界。

### 7.2 公平配置

- 固定相同可用 CPU 核数、线程档案（1 和 4；机器不足则明确不适用）、内存配置和临时磁盘条件。
- fssql managed limit 与 DuckDB memory_limit 的统计口径不同，只能标为“相同配置上限”；实际内存公平性还需比较独立进程 RSS/heap，不能宣称两种 cap 完全等价。
- 本地与远端分别测试；禁止用 DuckDB 本地结果耗时对比 fssql S3 耗时。
- 冷缓存、热缓存分别建档，禁止混合。冷缓存需要记录实际重置步骤；无法重置 OS/服务端缓存时只能标为新进程/未控制缓存，不能声称冷缓存。
- 两引擎交替执行，顺序按固定种子随机化，禁止互相并发争抢资源；同一任务的重复轮次使用相同输入和配置。
- 明确 DuckDB 的 pushdown、并行、spill、codec 配置。不能为了对齐结果而无记录地关闭一侧优化。
- 单引擎串行档案与并发查询档案分开；不得以一个引擎的并发吞吐对比另一个的单查询延迟。

### 7.3 数据集

| 档案 | 默认规模 | 主要用途 |
|---|---|---|
| XS | 0、1、V 边界和最多 10,000 行 | 语义、资源失败、PR 快速回归 |
| S | 100,000 行 | 函数/公共 API 吞吐和窄表查询 |
| M | 1,000,000 行 | 日常端到端与远端对标 |
| L | 至少 1 GiB 未压缩逻辑输入 | 默认预算下扫描、spill 和发布验收 |

行数和字节数都要记录；不能仅以“一百万行”代表等价数据量。大文本回归必须另保留原 MEM-001 样本和 256/512 MiB、1 GiB 普通记录文件。超过机器/磁盘能力时标为 BLOCKED，不能降低规模仍标称 L 通过。

每档包括窄表（4 列）、宽表（64 列），以及：0/10%/90% NULL、低/高基数、热点倾斜、可压缩文本、不可压缩字节、变长 Unicode、长字段、整数/宽 DECIMAL/纳秒/unsigned 边界。混合分布与单变量场景分别保留，避免所有查询只在易压缩、无 NULL 的整数列上运行。

fixture 必须记录生成器版本、种子、schema、分布参数、行数、逻辑字节数、物理字节数、文件和 schema 摘要、codec、编码、页/行组布局。缓存参考结果的键必须包含这些信息及参考引擎版本、配置和规范化版本；不能仅按 SQL 或文件长度命中旧参考。

### 7.4 必跑工作负载

| 工作负载 | 必测变体 | 额外断言 |
|---|---|---|
| 扫描 | CSV/NDJSON/Parquet、窄/宽、单/多文件、本地/S3 | 首批延迟、完整读取、峰值内存 |
| 投影/过滤 | 1 列/多列、选择率 0/0.1/1/10/100%、可裁剪/不可裁剪 | 数据页、行组和网络字节真实减少 |
| LIMIT/TopN | 0/1/100、大 offset、支持/不支持的下推、变长排序键 | 提前停止与 residual 正确 |
| 聚合 | 全局/分组、低/高基数、倾斜、COUNT/SUM/AVG | 结果类型、spill 与不 spill |
| JOIN | 1:1、1:N、重复/NULL、多键、NULL 安全、倾斜、大侧 spill | 输出爆炸单独计量；避免按输入吞吐掩盖产出差异 |
| 排序/Distinct | 内存/强制 spill、定长/变长、NULL、TopN | 临时 I/O、分配和输出规模 |
| 函数 | 每个必需函数及重要重载、短/长值、NULL | 内核可 microbenchmark，生产链另做端到端 |
| 窗口/CTE | 全局/分区、跨批次、多排序、多次来源引用 | 来源实际打开/消费次数 |
| 抽样/脱敏 | 各方法、样本比例、同字段规则链、HMAC | 统计性质、规则顺序、空间成本 |
| 导入/导出 | 类型化/显式 schema CSV、CSV/Parquet、无压缩/Snappy、AES-128/256 | 最终字节、互操作和提交时间 |
| 并发/背压 | 并发查询 1/2/4、快/慢消费者、取消 | 吞吐、延迟、RSS、资源公平性 |

没有 DuckDB 等价的资源方法、VFS capability 和生命周期方法，仅执行自身资源/性能回归，不制造伪对标。尚未支持的 Zstd 等能力登记为 UNSUPPORTED，不能进入支持能力的平均分。

### 7.5 测量、统计及指标

- 热缓存每个引擎至少 2 次预热，正式至少 7 个交替配对样本；预热结果保留但不进入统计。
- 短查询每个样本累计运行至少 1 秒并报告迭代次数。延迟 p95 至少需要 100 个独立请求样本；不足时标为样本不足，不以 7 次运行生成正式 p95。
- 冷缓存每次正式样本都执行相同缓存控制步骤，不进行会改变该缓存状态的预热。
- 中位数和 MAD 必须报告。MAD/中位数超过 10% 时该场景测量无效，先排查环境；不能用删掉慢样本使其通过。
- 保存所有有效轮次，错误、超时、OOM 和异常慢样本不得静默剔除。环境故障重跑必须保留故障证据和理由；回归复测使用完整配对集合。
- 至少报告 wall time、首批时间、rows/s、逻辑 MiB/s、CPU 秒、进程 RSS peak、fssql managed peak、Go heap/alloc/GC、temp peak/累计 I/O、网络请求/字节、结果行数/字节、导出物理大小。
- 缺少某项指标记为 null/不可用，不得记为 0。短进程 RSS 使用 OS 进程峰值计数器；采样方式必须注明周期及可能漏峰，不能把采样最大值伪称精确峰值。

定义：`time_ratio = median(fssql_seconds) / median(duckdb_seconds)`，大于 1 表示 fssql 更慢；`speedup = 1 / time_ratio`。RSS、CPU、输出大小分别计算比例，禁止合成未说明权重的总分。

### 7.6 默认验收门槛

这些是本标准确定的门槛及目标，**不是当前仓库已达到的测量结果**。

| 类别 | 默认门槛 | 失败处理 |
|---|---|---|
| 功能/类型/格式 | 必需用例 100% 通过，无未分类差异 | 阻断 |
| 资源/失败 | 不超受管预算和临时配额；无泄漏、panic、死锁 | 阻断 |
| 自身性能回归 | 同机器、同用例相对已接受基线，耗时中位数不得增加超过 10%；有足量样本的 p95 不超过 20% | 完整配对复测确认后阻断 |
| 自身资源回归 | managed/RSS/temp peak 超过基线 10% 时必须解释；违反预算立即阻断 | 无解释或无证据不得验收 |
| DuckDB 建设目标 | 必需核心场景 time_ratio≤2、RSS ratio≤2；同 codec/相当布局输出 size_ratio≤1.5 | 首次建基线逐项记录差距，不能声明目标通过 |
| 产品性能承诺 | 版本发布前在场景清单固定 SLO；无专门约定时沿用上述 DuckDB 建设目标作为替换发布门槛 | 未达到且无有效豁免则阻断替换发布 |

不能通过调整 baseline、增大预算、降低数据规模、修改压缩配置或删除慢用例消除回归。自身基线升级必须保留旧值、原因和评审记录。首次没有自身基线时记录为 baseline-created，不宣称回归验证通过。

物理 I/O 另设确定性门槛：单列扫描不读取无关列数据页；统计确定不匹配的行组读取零数据页；只命中一组时只扫描必要列块；安全 LIMIT 达标后停止，额外读取仅允许记录并受限的预读。footer 和元数据 I/O 单列，不能要求空过滤总网络字节为零。

## 8. 自动化层级与发布执行方案

| 层级 | 触发 | 必须执行 | 外部条件 |
|---|---|---|---|
| L0 开发 | 每次局部修改 | 相关单元、回归、静态检查 | 无强制外部服务 |
| L1 PR | 所有代码 PR | 全量 Go 测试、架构检查、相关公共入口回归、Linux Race、能力清单校验 | 固定 Go 环境；清单校验器建设前人工出具检查表 |
| L2 夜间 | 每日/主干变更 | 固定版本 DuckDB 全量差分、M 档、真实协议、格式互读、性质和定时 fuzz | DuckDB、MinIO/所需协议、相关读取器 |
| L3 发布 | 候选版本 | L1+L2、Windows/Linux、L 档、默认/低预算、强制 spill、失败恢复、统计抽样、加密实际互读、正式性能门槛 | 独立基准机器与全部必需依赖 |

后续 CI 必须将依赖准备失败与测试失败区分，但两者都不能形成通过的发布状态。当前仓库没有因此文档自动获得 CI enforcement；CI、清单生成器、差分扩展和性能判定器必须按第 11 节建设。

变更触发规则：语法/函数改动跑其签名及组合差分；类型改动跑所有搬运、spill、格式和 API 边界；优化器改动跑优化等价及实际 I/O；VFS/writer 改动跑真实协议、互操作及提交失败；内存/调度改动跑并发、Race、取消和资源矩阵。文档-only PR 只需链接、结构和事实核对，不机械运行全部引擎测试。

## 9. 当前可用入口与操作说明

以下是本文制定时已存在的入口，命令从仓库根目录运行。测试筛选必须检查实际匹配数量，外部依赖的 SKIP 不能被当作验收。

### 9.1 本地及公共 API 回归

```sh
go test -count=1 ./...
go test -count=1 ./examples/quickstart ./pkg/gosql
go run ./tools/architecture-check
```

在支持 Race 的测试机器上执行；Race 可能需要 CGO/本机编译器，这是测试工具要求，不改变生产纯 Go 交付要求：

```sh
go test -race -count=1 ./...
go test -coverprofile=.cache/test-coverage.out ./...
go tool cover -func=.cache/test-coverage.out
```

先创建 `.cache`；受限环境可设置 GOCACHE、TEMP/TMP 到工作区内的测试专用目录。不要覆盖 HOME 等系统变量。以上不自动启用外部对标测试。

### 9.2 Stable SQL 真实 DuckDB 差分

先准备虚拟环境，按锁定依赖安装，校验版本。Go runner 优先使用 `FSSQL_PYTHON` 指定的解释器，未设置时使用 `python3`；Windows 上可将该变量指向已安装 DuckDB 2.0.0 的虚拟环境解释器。

PowerShell：

```powershell
$env:FSSQL_REQUIRE_DUCKDB = '1'
go test -count=1 -v ./test/compat -run '^TestDuckDBDifferentialCorpus$'
```

POSIX shell：

```sh
FSSQL_REQUIRE_DUCKDB=1 go test -count=1 -v ./test/compat -run '^TestDuckDBDifferentialCorpus$'
```

当前 `stable.slt` runner 采用扁平文本比较，尚不满足本文完整类型/行边界/纳秒比较规范；必须扩展后才可覆盖全部新类型和函数。已有通过只对当前 corpus 和比较范围有效。

新增能力可使用 `TestDuckDBTypedWorkload` 的严格差分路径：

```powershell
$env:FSSQL_REQUIRE_DUCKDB = '1'
$env:FSSQL_PYTHON = 'python'
$env:FSSQL_DIFF_EVIDENCE = Join-Path $PWD '.cache/typed-diff.jsonl'
go test -count=1 -v ./test/compat -run '^TestDuckDBTypedWorkload$'
```

默认语料为 `test/compat/typed_workload.json`，当前均为合成 SQL；`FSSQL_TYPED_WORKLOAD` 可指定另一份语料文件，建议使用绝对路径。每项需要唯一 `id`、`source`、`capability`、`priority` 和自包含 `sql`；`ordered` 表示 SQL 保证结果顺序，`error_category` 表示预期失败类别。默认两侧都应成功。查询中的 `{batch}` 表示已有的等价 4097 行 fixture：fssql 使用 Go 表函数，DuckDB 使用带明确类型的 SQL 输入。此入口仅针对查询，不提供多语句初始化或参数载荷。

严格路径比较列名、完整类型、逐行逐列值与 NULL；无序结果按保留重复行的多重集合比较。DuckDB 在 SQL 内转换精确数值及时间，避免 Python datetime 截断纳秒；浮点使用 17 位规范文本，NaN/Infinity 单独规范化，正负零视为数值相等。错误用例必须命中指定的 parser/binder/conversion/overflow 类别，未知错误不视为兼容。比较器反例测试不依赖 DuckDB；真实引擎不可用时验收会失败。

JSONL 证据包含两侧完整类型与行、语料来源/能力/优先级、参考版本、Go/平台、测试二进制与语料 SHA256、资源配置。控制台输出总数及通过/失败数。它补充原有 corpus，不将合成测试通过率转换为生产 SQL 覆盖率。

`TestDuckDBTypedNumericCasts` 使用同一严格比较器及 `typed_numeric_cast_workload.json`，覆盖 round、TRY_CAST、DECIMAL 返回类型、精确文本转换、转换失败、子表达式错误及 4097 行跨批次。运行两套严格语料可使用 `-run '^TestDuckDBTyped(Workload|NumericCasts)$'`；`FSSQL_TYPED_WORKLOAD` 会覆盖任一入口的默认语料，因此运行仓库基线时应取消该变量。

`TestDuckDBTypedSetOperations` 使用 `typed_set_workload.json` 验证 UNION/INTERSECT/EXCEPT 的 DISTINCT/ALL、NULL/NaN、列类型、优先级、括号、CTE/相关子查询、排序别名与表达式、类型转换和内层修饰符边界，以及 4097 行跨批次分组计数。运行三套严格语料可使用 `-run '^TestDuckDBTyped(Workload|NumericCasts|SetOperations)$'`。公共 API 的 `TestSetOperations*` 另在小内存策略、强制 spill 和 4 worker 下验证执行、预编译复用、错误与中途取消后的回收；DAG 清理回归检查未启动的兄弟分支会释放资源，并避免重复关闭已完成依赖。

`TestDuckDBTypedTemporal` 使用 `typed_temporal_workload.json` 验证时间文本的精度、偏移处理、非法输入、纳秒范围、标量 date_part 的返回类型、ISO 年周、动态部件名、UTC 时区和 4097 行执行。数值语料另覆盖 FLOAT/DOUBLE→DECIMAL 与 HUGEINT→BOOLEAN。运行四套严格语料可使用 `-run '^TestDuckDBTyped(Workload|NumericCasts|SetOperations|Temporal)$'`。`TestTemporalCompatibility*` 验证预编译参数复用、返回类型、失败路径与资源回收；`TestParquetTimestampOffsetLiteralSemantics` 检查时间偏移与溢出在谓词翻译中的一致性。

深度差分入口（窗口、采样、连接、聚合、函数、类型转换、三值逻辑、子查询、跨 4097 行批次，以及 CSV/JSON/NDJSON/Parquet 双向互操作）：

```sh
FSSQL_REQUIRE_DUCKDB=1 FSSQL_PYTHON=python go test -count=1 -v ./test/compat -run 'TestDuckDB(DifferentialCorpus|DeepDifferential|FileInterop|ParquetTypeInterop)$'
```

真实 MinIO 入口需要注入 `FSSQL_MINIO_ENDPOINT`、`FSSQL_MINIO_BUCKET`、`FSSQL_MINIO_ACCESS_KEY`、`FSSQL_MINIO_SECRET_KEY`：

```sh
FSSQL_REQUIRE_DUCKDB=1 FSSQL_PYTHON=python go test -count=1 -v ./test/compat -run '^TestDuckDBMinIODifferential$'
```

`TestDuckDBDeepDifferential` 固定参考引擎线程数为 1、会话时区为 UTC；fssql 侧用 `batch_rows()` 表函数提供 4097 行，DuckDB 侧用等价的 `UNION ALL` 内联表，避免把解析器线性链性能计入跨批次语义比较。`TestDuckDBFileInterop` 同时验证 fssql 导出→DuckDB 读回、DuckDB 导出→fssql 读回，并把两侧结果与同一份 typed fixture 的规范值比较。`TestDuckDBParquetTypeInterop` 额外覆盖 `DECIMAL(18,2)`、`DECIMAL(38,6)`、`UBIGINT`、`TIMESTAMP_NS` 和 `BLOB` 的双向 Parquet 互读。`TestDuckDBMinIODifferential` 在真实 S3 兼容服务上验证双向 CSV/NDJSON/Parquet、多文件 glob、扫描/过滤/聚合/TopN 和缺失对象错误；测试对象位于唯一前缀并在结束后清理。

### 9.3 大文件及 microbenchmark

以下大文件脚本依赖 `.cache/agent-verification/minio-test-env.json`、生成的 `large-manifest.json` 和已准备的专用 MinIO fixture。运行前检查 endpoint、bucket、版本及输入摘要。脚本含 Windows/WSL/Docker 环境假设，其他平台需先补同等服务采样适配，不能直接声称跨平台可用。

```sh
python test/integration/run_minio_large.py
python test/integration/run_minio_large.py --defaults
python test/integration/run_minio_large.py --workers
python test/integration/run_minio_large.py --race
python test/integration/run_minio_large.py --bench
python test/integration/verify_large_regression.py
python test/integration/profile_minio_large.py
```

独立运行现有 microbenchmark：

```sh
go test ./internal/execution/... -run '^$' -bench . -benchmem -benchtime=1s -count=7
go test ./internal/export ./internal/format/... -run '^$' -bench . -benchmem -benchtime=1s -count=7
```

已有 `benchmark_core.py` 和大文件 `--bench` 默认只有 3 次，适合诊断，不满足本文正式 7 样本规范。已有 fixture 生成器部分 DuckDB 参考查询会将 S3 路径替换为本地路径；其中 `reference_seconds` 不能作为同协议性能对标依据，缓存命中的耗时尤其不能作新测量。

真实 MinIO 小测试由 `FSSQL_MINIO_ENDPOINT` 等已有环境项启用。凭据从测试环境注入，不能抄入报告。

### 9.4 外部格式互操作

```sh
python test/interop/parquet_writer/run.py
```

该现有入口主要验证普通 Parquet 的小样本互读。其他阶段脚本按其实际依赖准备后执行；没有装在锁定环境中的 Spark/Polars 等不能宣称已验证。扩展类型、压缩和自编译加密仍需新增互操作矩阵，不能由此小样本推导通过。

### 9.5 远程单文件导出与恢复

真实 SFTP、WebDAV、FTP/隐式 TLS FTPS、MinIO 的四格式导出、故障注入、进程终止及恢复入口：

```sh
go test -count=1 -v -timeout=15m ./pkg/gosql -run '^TestRealRemote'
```

通过环境注入 `FSSQL_REAL_PROTOCOL`（sftp/webdav/ftp/ftps/s3）、`FSSQL_REAL_ENDPOINT`、专用目录 `FSSQL_REAL_DIR`、`FSSQL_REAL_USER` 和认证信息。SFTP 使用 `FSSQL_REAL_IDENTITY`、`FSSQL_REAL_KNOWN_HOSTS`；其他协议使用 `FSSQL_REAL_PASSWORD`，FTPS 另设可信 CA 文件 `FSSQL_REAL_CA`。S3 endpoint 使用 `s3://fssql-agent-test-...` 的专用 bucket，服务地址通过 `FSSQL_MINIO_ENDPOINT` 提供。

`FSSQL_REAL_MODE` 可选 strict/compatible。默认并发 Apache 使用 compatible；只有验证部署的原子 MOVE 和并发不覆盖保证后，才设置 `FSSQL_REAL_ATOMIC_MOVE_VERIFIED=1`。协议前提及保证边界见 [公共 API](public-api.md#remote-protocols)。

可选 `FSSQL_REAL_RESTART_ARGV` 为 JSON argv 数组，指定重启该专用测试服务并等待就绪的命令。测试会留下随机命名的对象，创建应答丢失用例还会留下未知 ownership；应在专用环境执行并在结束后回收。未配置外部服务时这些用例会跳过，不能计为真实服务验收通过。

## 10. 证据格式与报告模板

建议运行目录：`.cache/test-evidence/<run-id>/`。这是本标准约定的新归档布局；现有脚本输出应在后续收集器中映射，不能假称它们已经写入此目录。

```text
environment.json
inventory.json
cases.jsonl
benchmarks.jsonl
raw/                         原始命令、退出状态、错误及双方输出
profiles/                    CPU/heap/参考引擎 profile
fixtures-manifest.json
acceptance-report.md
benchmark-report.md
memory-report.md
failure-report.md
known-limitations.md
```

原始大数据和私有凭据不提交仓库；合成小样本、生成器、manifest 和最小回归可提交。候选发布的报告及证据摘要提交仓库或关联不可变 CI artifact。PR 原始证据保留至少 90 天，发布证据保留该版本整个支持期且不少于一年。

用例记录至少包含：

```json
{
  "run_id": "example-run",
  "case_id": "SQL.CAST.DOUBLE_BIGINT.ROUND.001",
  "profile": "stable",
  "fixture_id": "numeric-edges-v1",
  "status": "PASS",
  "reference_version": "<实际运行版本>",
  "schema_match": true,
  "rows_match": true,
  "values_match": true,
  "error_class_match": null,
  "raw_artifact": "raw/cast-rounding.json"
}
```

这是格式示例，不是执行结果。性能逐样本记录至少包含 engine、case_id、fixture_id、configuration_id、cache_mode、run_order、sample_index、warmup、seconds、rows、CPU/RSS/temp/I/O 和 error；汇总文件另外保存统计值、比例、阈值、是否适用及判定理由。

验收报告必须列出计划/执行/PASS/FAIL/BLOCKED/NOT_RUN/UNSUPPORTED 数量、失败清单、变化范围及结论；性能报告逐项列双方原始样本和自身历史值，不能只给最好的一次或总体平均。memory/failure 报告列预算、实测、注入位置和清理断言。known-limitations 必须列差异、豁免、未覆盖平台和缺失指标。

## 11. 测试体系补齐实施计划

| 顺序 | 建设内容 | 必须交付 | 完成条件 |
|---|---|---|---|
| T0 | 冻结清单、参考及 fixture 档案 | 三份能力清单、版本检查、生成器摘要、现有用例映射 | 所有声明支持项有状态，无隐式漏项 |
| T1 | 扩展差分 runner | 有类型/行边界的规范结果、schema、错误类别、解释器配置、稳定 ID | NULL 文本碰撞、BLOB、纳秒、DECIMAL、无序重复行等反例均能检测 |
| T2 | 补功能/函数/API 矩阵 | 第 5 节用例、每签名映射、公共入口端到端、业务生成 SQL 语料 | 必需签名和方法场景全部执行并差分；独有 API 契约通过 |
| T3 | 补同条件性能双引擎 runner | 同协议 fixture、交替配对、缓存档案、完整消费、独立进程资源统计 | XS/S/M 的双方原始证据齐备，不复用旧参考耗时 |
| T4 | 大文件、真实协议及格式加密互读 | L 档、MEM/PERF 原复现、强制 spill、实际读取器、失败恢复 | 物理 I/O 和资源门槛实测通过；加密外部基线齐备 |
| T5 | CI 与发布硬门槛 | L1/L2/L3 工作流、零匹配/skip 校验、自动汇总、历史基线比较 | 故意制造缺依赖、语义差异、资源超限和回归均能阻断发布 |

可以先建设测试并暴露已知失败，不能为保持绿色而缩减清单。T0—T5 是测试体系建设顺序，与引擎替换阶段分别追踪；实现功能阶段 6 不意味着测试体系 T4/T5 已完成。

## 12. 变更提交检查表

- [ ] 所改能力/签名/公共方法及相关组合用例已登记。
- [ ] 已保存修复前能失败、修复后通过的最小回归；预期来自独立契约或参考引擎。
- [ ] 已验证结果类型和值，未通过文本化或容差掩盖差异。
- [ ] 已按变更类别执行差分、资源、失败、互操作及适用的性能测试。
- [ ] 已检查实际执行数量、skip、参考版本、dirty 状态和外部服务条件。
- [ ] 报告没有把未运行、缺基线或仅自身往返测试记为通过。
- [ ] 缺陷登记、限制、性能基线变更及有效豁免可追溯。

文档审阅由对应模块维护者负责，新增语义和基线变化需一名非实现者复核。发布结论以当次完整证据为准，不能沿用过期报告或仅引用本规范作为已验证证明。


### 集中兼容性回归

`TestDuckDBTypedBulk` / `typed_bulk_workload.json` 集中覆盖 BY NAME、ANY/ALL/SOME、动态 range/generate_series/unnest、正则与嵌套提取、NULL 逻辑类型、数值文本、IANA/BC/无限时间及 CSV 候选/格式。CSV fixture 的内容 SHA256 写入证据；参考与本地使用同一内容。DECIMAL 比较仅规范化省略的整数位零（.1 → 0.1），保留全部有效数字、scale 和类型；VARCHAR 不做此转换。

`TestBulk*` 公共 API 测试覆盖 TEMP 及快照令牌的会话隔离、参数化 DDL/COPY、时区/线程设置、拥有型嵌套值、错误/取消回收。Core 另覆盖嵌套字典复制后释放原数据，以及 SQLNULL 类型序列化。新增范围应与旧四套 typed、stable/deep 和互操作一起运行，测试启动后保持代码和语料固定；任何失败必须修复并重跑受影响范围，最后记录冻结组合结果。
