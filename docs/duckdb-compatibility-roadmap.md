# DuckDB 兼容性与能力完善方案

日期：2026-11-11

## 实施进度（2026-11-12，集中批次）

按用户确定的范围集中补齐 8 类已登记能力，事务、可更新存储与持久化另立项：

1. BY NAME、ANY/ALL/SOME；数值 range/generate_series 和 LIST unnest 的动态/横向参数。
2. 常用 regexp 函数、string_split 及嵌套提取；正则 scratch/output 遵守查询资源限制。
3. BOOLEAN 数值转换、十六进制/二进制整数文本，以及精确边界检查。
4. IANA 时区、夏令时重叠/间隙、BC/宽年份、无限日期时间。
5. DECIMAL round 的 NULL 逻辑结果、选定纯函数常量折叠、date_part 动态表达式及 LIST→STRUCT 重载。
6. Chunk.Value/List/Struct 拥有型递归访问；嵌套复制与 NULL 类型序列化。
7. CSV 候选列表、显式日期/时间格式与推断/执行一致性；保留默认 ISO 行为。
8. 参数化 DDL/COPY、物理连接 TEMP 隔离与令牌访问控制、SET/RESET 会话时区/线程。

`SQL-SET-001` 已关闭。其他差异项更新为剩余的完整函数/类型/格式边界；database/sql 对嵌套单元格的显式拒绝继续保留。集中实现已完成统一验证，具体结果见文末记录。

## 实施进度（2026-11-12，第四批）

- 补齐时间文本的日期形式、空格/T 分隔、Z、数值偏移、UTC、可选秒与小数精度。无时区类型忽略偏移，有时区类型换算到 UTC；执行、TRY_CAST 和 Parquet 常量谓词共用有范围检查的解析逻辑，避免纳秒时间溢出回绕。DATE 可读取合法时间文本并保留书写日期。
- 补齐 FLOAT/DOUBLE → DECIMAL 和 HUGEINT → BOOLEAN。浮点到 DECIMAL 按参考行为缩放二进制浮点值再舍入；NaN、无穷及越界按 CAST/TRY_CAST 语义处理。
- 新增标量 `date_part` 的 DATE/TIMESTAMP 签名，覆盖日历、时钟、ISO 年周、epoch、julian 及 UTC 时间戳时区部件。区分常量/动态部件的返回类型，纳秒输入按 DuckDB 2.0 标量重载舍入到微秒后提取。
- 新增 `typed_temporal_workload.json`，扩充数值转换语料。部件表达式常量折叠、LIST/STRUCT/INTERVAL 重载、命名时区及完整时间文本语法仍单列为限制（SQL-DATEPART-001 / SQL-TIMESTAMP-001）。本批验证记录见文末。

## 实施进度（2026-11-11，第三批）

- P1 集合操作已支持按位置的 `UNION`、`INTERSECT`、`EXCEPT`，包含默认/显式 DISTINCT 和 ALL。覆盖重复次数、NULL/NaN、类型合并、优先级、括号、分支/整体修饰符、CTE、相关子查询及预编译。
- 执行复用现有外部去重与 NULL 安全 SEMI/ANTI 连接；ALL 通过按整行分组的编号匹配重复次数，排序、连接及 spill 使用原有受控资源路径。类型转换保留内层 DISTINCT/ORDER/LIMIT 的边界；计算排序键在集合去重完成后执行。
- 修复了窗口分组将 NaN 与普通数值误判为同组的问题。错误路径测试还发现并修复了 DAG 提前失败时未启动兄弟分支的排序资源未释放的问题；已运行依赖不会重复关闭。
- 新增 `typed_set_workload.json` 及 `TestDuckDBTypedSetOperations`；UNION BY NAME 的剩余边界记录为 `SQL-SET-001`。其余函数、P2/P3 与完整 DuckDB 兼容性仍未完成。

## 实施进度（2026-11-11，第二批）

- P1 新增 `round`：有符号整数、FLOAT/DOUBLE、DECIMAL；无符号输入提升到 BIGINT/HUGEINT。精确整数舍入不经过浮点，DECIMAL 返回类型在绑定时确定，整数/DECIMAL 溢出明确失败。
- P1 新增受限转换矩阵的 `TRY_CAST`，转换失败返回 NULL；输入表达式和资源错误继续传播。CAST 与 TRY_CAST 使用不同函数 ID，避免常量优化和 Parquet 下推错误套用抛错语义。补齐了整数文本的小数/指数形式、精确舍入、常用布尔文本和浮点文本溢出处理。
- 新增 `typed_numeric_cast_workload.json` 及 `TestDuckDBTypedNumericCasts`。DECIMAL 常量精度的剩余限制记录为 `SQL-ROUND-001`；缺失的转换内核和文本形式记录为 `SQL-CAST-001`。时间偏移解析缺口仍保留，未扩大为全量 DuckDB 函数或转换兼容声明。

## 实施进度（2026-11-11，首批）

- P0 已落地严格差分入口 `TestDuckDBTypedWorkload`：保留列名、完整类型、行边界和真实 NULL；无序结果保留重复次数；精确数值和纳秒时间不经过有损 Python 类型；预期错误按类别验证，未知错误明确失败。语料可通过 `FSSQL_TYPED_WORKLOAD` 替换，当前仓库语料全部标记为合成样本，尚未采集真实调用方工作负载。
- 证据输出复用 `FSSQL_DIFF_EVIDENCE`，记录资源配置、Go/平台、参考版本、语料与测试二进制 SHA256，以及逐用例结果。运行入口与格式见 [测试规范](testing-standard.md#92-stable-sql-真实-duckdb-差分)。旧 `stable.slt` 仍保留原比较方式，不能据此宣称它已完成严格类型差分。
- P1 首批实现 `upper(VARCHAR)` 和 `abs(numeric)`；覆盖 Unicode（含 `ß` → `ẞ`）、NULL、空结果、数值类型保持、精确宽数值、最小负值溢出、预编译复用、字典向量及 4097 行跨批次。其余函数、集合操作及 P2/P3 尚未完成，继续按下面的阶段推进。
- 新发现的差异：SQL 文本转换到 TIMESTAMPTZ 不接受 `+00` 和 `T...Z` 两种形式，已登记为 `SQL-TIMESTAMP-001`。严格时间基线使用已支持的空格分隔、无偏移文本，不将其通过当作偏移解析兼容。

本节记录已经实现的范围；实际验证结果与命令见文末的首批验证记录。

## 目标与范围

以 DuckDB 2.0.0 为当前参考版本，优先让已有分析查询在 fssql 上正确执行，逐步扩展经过验证的 SQL 子集。兼容性包括语法、绑定、结果类型和值、NULL、错误及文件互操作；仅能解析或运行不算兼容。新增能力应沿用现有执行模型、受控内存、VFS 能力检查和公开 API 约束。

不以本方案承诺完整 DuckDB 替代，也不将目标版本自动扩展到其他 DuckDB 版本。事务、可更新表、持久化数据库和外部数据库 scanner 属于独立产品能力，只有明确场景与语义后才立项。

## 当前基线与证据

- 已有非递归 CTE、`UNION ALL`、多种相关子查询、LATERAL、查询参数、CSV/JSON/NDJSON/Parquet 读取和 COPY 等能力；具体边界以 [公共 API](public-api.md) 为准。
- [兼容差异登记](../compatibility/deviations.yaml) 记录了事务、函数、子查询及 CSV 推断等缺口；[迁移说明](contracts/duckdb-migration.md) 记录 TEMP、CTAS、数据源与导出差异。登记内容是已知项，不代表完整差异清单。
- README 记录过针对 DuckDB 2.0.0 的 262/262 差分用例一致；结论仅覆盖该次数据与用例。本方案制定时未重新运行测试。[测试规范](testing-standard.md)指出现有 `stable.slt` 的扁平文本比较不足以验证完整类型与行边界。
- 公共 `Begin/BeginTx` 不支持事务；Catalog 内已有单语句元数据事务。`Materialize` 可复用一份显式物化结果，但不提供通用外部数据源事务快照。

## 排序原则

先收集真实调用方 SQL 和数据样本，按出现频率、是否阻断迁移、语义风险及实现成本排序。以下顺序是缺少工作负载数据时的默认顺序，不是兼容覆盖率承诺。每项开始前核对当前代码与登记，避免重复实现或依据过时文档判断。

| 优先级 | 工作 | 交付结果 |
| --- | --- | --- |
| P0 | 差分基础设施、工作负载清单 | 能按语句和能力统计结果并复现差异，固定参考版本 |
| P1 | 高频函数与表达式 | 按实际需求分批支持，先处理登记中的常用函数 |
| P1 | 集合操作 | 支持 `UNION`、`INTERSECT`、`EXCEPT` 的已定义范围 |
| P2 | 子查询、CSV 与数据类型边界 | 补齐高频缺口，保持内存和错误语义明确 |
| P2 | 导出、TEMP 和公开 API 差异 | 先明确契约，再按实际迁移需求实现 |
| P3 | 跨语句快照或 Catalog 事务 | 有具体一致性需求时设计并单独立项 |
| 按需 | 可变表、完整事务、持久化和 scanner | 需单独评估存储、恢复、隔离及安全，不混入 SQL 兼容补丁 |

## P0：建立可复现的兼容基线

1. 收集调用方实际 SQL，脱敏后保留参数类型、输入 schema、数据规模、依赖的 session 设置及 DuckDB 版本。无法收集时使用可复现的代表性 SQL，并标记为合成样本。为每条 SQL 标识来源、重要程度及所属能力，不把用例通过数等同于真实工作负载覆盖率。
2. 固定 DuckDB 2.0.0、线程数、时区、依赖版本和输入数据；记录 fssql 版本、平台、配置、命令及输入指纹。区分解析失败、绑定失败、执行失败、类型差异、值差异、错误类别差异和顺序差异。
3. 扩展或复用现有深度差分路径，比较列数、列名、类型、行边界、NULL、精确数值和时间精度。仅当 SQL 保证顺序时比较行序；无序结果按多重集合比较，保留重复行。对浮点特殊值采用明确的比较规则。预期失败必须同时验证两端的失败类别，不能因都报错就默认语义一致。
4. 为每个差异建立最小复现、预期结果及状态：待确认、缺失、缺陷、有意差异、已修复。更新 `compatibility/deviations.yaml`、公开文档和测试证据，避免把尚未验证的能力标记为完成。

**验收：** 可在固定环境重复运行；报告逐条给出通过、失败、跳过及原因；新增比较器自身有类型、NULL、重复行、顺序和错误的反例测试。真实 DuckDB 不可用时明确失败或标记未运行，不能以跳过冒充通过。

## P1：高频 SQL

### 函数与表达式

已支持 `upper`、`abs`、`round`、受限 `TRY_CAST`、标量及常量列表 `date_part`、常用正则函数和 `string_split`。后续按实际工作负载补齐已登记的签名与转换边界。新增签名逐项验证隐式转换、返回类型、NULL、溢出、非法输入、Unicode 及日期与时区行为，并覆盖常量、列、参数、空输入和跨批次执行。

实现沿现有 parser/binder 和表达式执行链路，优先复用现有向量与内存管理。函数兼容以逐签名差分通过为准，未覆盖的重载仍记录为不支持。

### 集合操作

已支持按位置的 `UNION`、`INTERSECT`、`EXCEPT` 及其 `ALL` 变体，也支持 `UNION BY NAME` 和 `UNION ALL BY NAME`。验收覆盖列数与类型合并、列名来源、NULL/NaN、优先级和括号、尾部 `ORDER BY/LIMIT/OFFSET`、空输入及跨批次。去重或计数必须遵守受控内存与 spill 策略。后续类型、语法和 API 扩展按差异登记推进。

**P1 验收：** 每个已声明的函数签名及集合操作都有 DuckDB 差分、错误边界和公共 API 回归；在默认及受限内存配置下完成相关资源清理验证。未完成的变体单列，不扩大兼容声明。

## P2：迁移中的剩余能力

1. **子查询：** 已支持 `ANY/ALL/SOME` 的相关与非相关子查询，以及数值 `range`、`generate_series` 和 LIST `unnest` 的横向参数。新增关联形态时验证空集合、NULL、类型转换、生命周期和资源边界。
2. **CSV：** 按需求支持类型候选和日期格式选项；覆盖混合格式、非法值、采样偏差及显式 schema 优先级。对不同推断策略的变更明确版本和默认行为，不能静默改变已有用户结果。
3. **类型与公开 API：** 原生 Chunk 支持独立持有的 LIST/STRUCT 值，`database/sql` 对非 NULL 嵌套值返回错误。按实际调用需求评估行式表示，并保留 DECIMAL/HUGEINT 的精确性。
4. **TEMP、导出和配置：** 对连接隔离、命名遮蔽、目标覆盖、压缩默认值及配置项分别定义兼容模式和现有默认行为。需要改变默认值时单独设计迁移策略。远端文件、加密互通及 Linux 采样兼容应按实际环境验收，未验证项保持开放。

**P2 验收：** 每项有明确的 API/SQL 合约和负例；文件能力使用双向互读测试，涉及远端服务时记录真实服务及环境，不能用本地结果代替远端验收。

## 事务与存储能力的决策门

当前优先完善单语句一致性和导出发布。出现以下需求时，再分别设计：

- 多次查询需重复读取同一份输入：先评估现有 `Materialize`；若不能满足，设计只读快照，明确外部文件版本、变更检测和资源生命周期。
- 多个 Catalog 操作需一起提交：设计 Catalog 多语句事务，定义连接归属、读己之写、冲突、取消及回滚；不承诺外部文件或 COPY 随之回滚。
- 可变表或 Catalog 与外部导出一起提交：先确定原子性范围、并发隔离、故障恢复、持久化和外部系统能力，再决定是否需要 WAL/MVCC 或其他机制。

只有完成对应语义、故障注入、并发和恢复验证后，才开放相关事务 API。当前不以空实现或仅包装 Catalog 的方式宣称通用事务支持。

## 验证与发布门槛

每个增量遵循：最小复现和参考结果 → 语义与范围 → 实现 → 针对性单元/集成/差分测试 → 更新差异登记和文档。涉及并发、资源或文件发布的变更分别补 race、资源回收、取消及故障测试；性能变化用固定数据和环境单独测量，正确性通过不等于性能达标。

基线与深度差分入口见 [测试规范](testing-standard.md#92-stable-sql-真实-duckdb-差分)。例如在已准备 DuckDB 2.0.0 和 Python 环境的 PowerShell 中运行：

```powershell
$env:FSSQL_REQUIRE_DUCKDB = '1'
$env:FSSQL_PYTHON = 'python'
go test -count=1 -v ./test/compat -run 'TestDuckDB(DifferentialCorpus|DeepDifferential|FileInterop|ParquetTypeInterop)$'
```

环境、Python 入口和额外服务要求以测试规范及 runner 实际行为为准。发布前记录执行命令、版本、样本、通过/失败/跳过和未验证项；已知非预期语义差异未解决时，不能把相关能力标记为兼容。阶段完成以其验收项和证据为准，不设未经测量的工期或覆盖率。

## 首批验证记录

2026-11-11，Windows/amd64，Go 1.27.3，使用工作区已有虚拟环境中的 DuckDB 2.0.0；Go 构建缓存及测试临时目录指向工作区 `.cache`，避免受限环境的用户缓存权限问题。

以下检查通过：

```powershell
go test -count=1 ./internal/function ./internal/execution/expression ./pkg/gosql
go test -race -count=1 ./internal/execution/expression ./pkg/gosql -run '^Test(UpperAbs|StringBatch)'

$env:FSSQL_REQUIRE_DUCKDB = '1'
$env:FSSQL_PYTHON = Join-Path $PWD '.cache/agent-verification/venv/Scripts/python.exe'
$env:FSSQL_DIFF_EVIDENCE = Join-Path $PWD '.cache/compatibility-roadmap/regression.jsonl'
go test -count=1 ./test/compat -run 'Test(StableSQLLogicCorpus|DeviationRegistry|TypedComparisonRejectsHiddenMismatches|DuckDB(ReferenceMatchesDeviationRegistry|DifferentialCorpus|DeepDifferential|TypedWorkload|FileInterop|ParquetTypeInterop))$'
```

该次 JSONL 记录 266 条差分全部一致：基础 SQL 144、深度 SQL 88、文件互操作 7、Parquet 类型互操作 2、新增严格语料 25。各路径的比较强度不同，其中原有基础/深度路径不等同于新增严格类型比较。严格语料包含跨批次、错误边界及精确类型，错误用例通过表示两侧均以预期类别失败。比较器反例测试、公共 API 回归与上述定向 race 通过。

本轮没有运行全部仓库测试、真实远端服务验收或性能基准，也没有将其余函数、集合操作及事务阶段标为完成。`SQL-TIMESTAMP-001` 仍待处理。

## 第二批验证记录

2026-11-11，沿用首批的 Windows/amd64、Go 1.27.3 和 DuckDB 2.0.0 环境。以下检查通过：

```powershell
go test -count=1 ./internal/function ./internal/sql/parser ./internal/sql/binder ./internal/execution/expression ./internal/session ./pkg/gosql
go test -race -count=1 ./internal/execution/expression ./internal/session ./pkg/gosql -run '^Test(RoundTryCast|ParquetPushdown|ExecuteSQLConstantTemporalCast)'

$env:FSSQL_REQUIRE_DUCKDB = '1'
$env:FSSQL_PYTHON = Join-Path $PWD '.cache/agent-verification/venv/Scripts/python.exe'
$env:FSSQL_DIFF_EVIDENCE = Join-Path $PWD '.cache/compatibility-roadmap/round-try-regression-final.jsonl'
go test -count=1 ./test/compat -run 'Test(StableSQLLogicCorpus|DeviationRegistry|TypedComparisonRejectsHiddenMismatches|DuckDB(ReferenceMatchesDeviationRegistry|DifferentialCorpus|DeepDifferential|TypedWorkload|TypedNumericCasts|FileInterop|ParquetTypeInterop))$'
```

TIMESTAMP_NS 保留边界补充后，单独重跑了 `TestRoundTryCast*`、`TestDuckDBTypedNumericCasts`，并对公共 API 的 `TestRoundTryCast*` 再次运行 race。最终组合回归使用固定代码和语料重新编译执行，通过 297 条差分：基础 SQL 144、深度 SQL 88、文件互操作 7、Parquet 类型互操作 2、严格语料 56（首批 25 + 本批 31）。完整 JSONL 位于上述证据路径，未发现差异。

新用例覆盖 DECIMAL 返回类型随 precision 改变、负精度舍入、溢出、浮点特殊值、精确文本转换、逐行失败转 NULL、子表达式错误传播、TIMESTAMP_NS 范围/保留边界及 4097 行跨批次。公共 API 回归另验证了预编译复用、资源错误传播、执行后内存回收和未实现内核的显式拒绝。

本批未运行全仓库测试、性能基准或真实远端验收；`SQL-ROUND-001`、`SQL-CAST-001`、`SQL-TIMESTAMP-001` 仍作为明确边界保留。后续阶段与集合操作尚未完成。

## 第三批验证记录

2026-11-11，Windows/amd64、Go 1.27.3、DuckDB 2.0.0。本轮九个受影响包的完整测试分批通过：lexer、parser、binder、planner、expression、pipeline、lowering、session、gosql。初次公共 API 回归发现旧断言仍将普通 UNION 视为不支持，已改为保留 UNION BY NAME 的拒绝检查，并完整重跑 `go test -count=1 ./pkg/gosql` 通过；普通 UNION 的成功行为由新测试覆盖。

复现验证范围的命令：

```powershell
go test -count=1 ./internal/sql/lexer ./internal/sql/parser ./internal/sql/binder ./internal/planner ./internal/execution/expression ./internal/execution/pipeline ./internal/execution/lowering ./internal/session ./pkg/gosql
go test -race -count=1 ./internal/execution/pipeline ./internal/execution/lowering ./pkg/gosql -run '^Test(RunDAG|.*DependencyFailure|SetOperation|Union)'

$env:FSSQL_REQUIRE_DUCKDB = '1'
$env:FSSQL_PYTHON = Join-Path $PWD '.cache/agent-verification/venv/Scripts/python.exe'
$env:FSSQL_DIFF_EVIDENCE = Join-Path $PWD '.cache/compatibility-roadmap/set-regression-final.jsonl'
go test -count=1 ./test/compat -run 'Test(StableSQLLogicCorpus|DeviationRegistry|TypedComparisonRejectsHiddenMismatches|DuckDB(ReferenceMatchesDeviationRegistry|DifferentialCorpus|DeepDifferential|TypedWorkload|TypedNumericCasts|TypedSetOperations|FileInterop|ParquetTypeInterop))$'
```

定向 race 与最终组合差分均通过。最终 JSONL 记录 356 条差分全部一致：基础 SQL 144、深度 SQL 88、文件互操作 7、Parquet 类型互操作 2、严格语料 115（首批 25、第二批 31、本批 59）。本批覆盖 DISTINCT/ALL 重复次数、NULL/NaN、宽整数与时间类型合并、CTE/相关查询、排序规则、类型转换屏障及 4097 行跨批次分组计数。

公共 API 测试还覆盖 4 worker、低内存策略/强制 spill、预编译复用、转换错误和中途取消后的内存归零。DAG 回归验证未启动兄弟分支的释放及已完成依赖只关闭一次。另重跑 UNION ALL 长链计划深度测试与差异登记校验通过。

本轮未运行全仓库测试、性能基准或真实远端验收；UNION BY NAME、其余函数和 P2/P3 仍未完成，不构成完整 DuckDB SQL 兼容声明。

## 第四批验证记录

2026-11-12，Windows/amd64、Go 1.27.3、DuckDB 2.0.0。Go 缓存和临时目录沿用工作区 `.cache`。以下检查全部通过：

```powershell
go test -count=1 ./internal/semantics ./internal/function ./internal/sql/binder ./internal/execution/expression ./internal/session ./pkg/gosql

go test -race -count=1 ./internal/semantics ./internal/session ./pkg/gosql -run '^Test(ParseTimestamp|ParseDateTimestamp|TemporalCompatibility|ParquetTimestamp|RoundTryCast)'

$env:FSSQL_REQUIRE_DUCKDB = '1'
$env:FSSQL_PYTHON = Join-Path $PWD '.cache/agent-verification/venv/Scripts/python.exe'
$env:FSSQL_DIFF_EVIDENCE = Join-Path $PWD '.cache/compatibility-roadmap/temporal-casts-frozen-final.jsonl'

go test -count=1 ./test/compat -run 'Test(StableSQLLogicCorpus|DeviationRegistry|TypedComparisonRejectsHiddenMismatches|DuckDB(ReferenceMatchesDeviationRegistry|DifferentialCorpus|DeepDifferential|TypedWorkload|TypedNumericCasts|TypedSetOperations|TypedTemporal|FileInterop|ParquetTypeInterop))$'
```

最终组合回归在固定代码和语料下通过 400 条差分：基础 SQL 144、深度 SQL 88、文件互操作 7、Parquet 类型互操作 2、严格语料 159（基础 25、数值转换 39、集合操作 59、时间 36）。JSONL 无失败记录，四份严格语料的 SHA256 与当前文件一致，严格语料使用同一测试二进制。

新回归覆盖时间偏移是否应用、日期文本的时间后缀、分数截断、纳秒范围/保留端点、普通 CAST 错误和 TRY_CAST NULL、浮点 DECIMAL 舍入/宽度/特殊值、宽整数布尔转换，以及 date_part 的 ISO 年周、动态部件、预编译参数复用和跨 4097 行批次。直接转型的 NULL 时间常量返回 DOUBLE；非法常量部件名在空输入时也会报转换错误。公共 API 负例检查错误后的托管内存归零；Parquet 谓词回归验证解析与执行一致。

参考语义以 DuckDB 2.0.0 实测为准，相关说明见 [时间类型](https://duckdb.org/docs/2.0/sql/data_types/timestamp) 和 [日期部件](https://duckdb.org/docs/2.0/sql/functions/datepart)。命名时区、完整时间文本语法、date_part 更广的常量折叠及嵌套/INTERVAL 重载仍保留为明确边界。正则、string_split、BY NAME、ANY/ALL 及 P2/P3 的其余事项仍未完成。

本批未运行全仓库测试、性能基准、Linux 或真实生产远端验收；400 条有限语料通过不构成完整 DuckDB 兼容声明。`git diff --check` 通过。

## 集中批次验证记录

2026-11-12，Windows/amd64、Go 1.27.3、DuckDB 2.0.0。用户确认本轮为 8 类已登记兼容缺口，事务、可更新存储及持久化不在本轮范围。按要求先集中实现，再统一测试；测试发现的问题均修正后重新验证。

以下最终检查全部通过：

```powershell
$env:GOCACHE = Join-Path $PWD '.cache/go-build'
$env:GOTMPDIR = Join-Path $PWD '.cache/go-tmp'
$env:TEMP = Join-Path $PWD '.cache/test-tmp'
$env:TMP = $env:TEMP

go test -count=1 ./...

go test -race -count=1 ./internal/core/vector ./internal/execution/pipeline ./internal/session ./pkg/gosql -run '^Test(Bulk|RunDAG|SetOperation|Union|RoundTryCast|TemporalCompatibility|ParquetTimestamp)'

$env:FSSQL_REQUIRE_DUCKDB = '1'
$env:FSSQL_PYTHON = Join-Path $PWD '.cache/agent-verification/venv/Scripts/python.exe'
$env:FSSQL_DIFF_EVIDENCE = Join-Path $PWD '.cache/compatibility-roadmap/bulk-frozen-final.jsonl'

go test -count=1 ./test/compat -run 'Test(StableSQLLogicCorpus|DeviationRegistry|TypedComparisonRejectsHiddenMismatches|DuckDB(ReferenceMatchesDeviationRegistry|DifferentialCorpus|DeepDifferential|TypedWorkload|TypedNumericCasts|TypedSetOperations|TypedTemporal|TypedBulk|FileInterop|ParquetTypeInterop))$'
```

冻结后的组合差分 476/476 一致：基础 SQL 144、深度 SQL 88、文件互操作 7、Parquet 类型互操作 2、严格语料 235（基础 25、数值转换 39、集合操作 59、时间 36、本轮 76）。证据中的五份语料 SHA256 与当前文件一致，严格语料来自同一测试二进制；CSV fixture 另外记录内容指纹。

统一测试修正了：BY NAME 列名大小写与全 NULL 结果类型、表函数输出越过所有权边界时的资源释放、参数化 view 的类型/列名/空值性固化、COPY 选项占位符、默认 CSV 推断行为，以及过时的不支持断言。新增回归还覆盖 NULL/空集合的量化比较、横向参数、取消、嵌套复制后原数据释放、SQLNULL 序列化、TEMP 元数据及快照令牌的连接隔离、物理连接关闭回收、会话配置和 SQL 参数注入边界。

支持范围以公共 API 与更新后的差异登记为准：此轮交付上述 8 类中的已定义功能，保留未知函数/配置、尚不存在的类型、任意 UDF 绑定期执行等明确边界；database/sql 对嵌套单元格继续显式拒绝，原生拥有型值接口提供完整递归读取。有限差分通过不等于任意 DuckDB SQL 兼容。

`git diff --check` 通过。本轮未进行性能基准、Linux 实机或完整生产远端服务验收；相关开放验收项没有用本地测试替代。
