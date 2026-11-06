# fssql 能力完善任务：承接 edge-agent-go 的 DuckDB 替换

更新日期：2026-11-06。状态：已补齐显式 reservoir 兼容路径、加密 COPY SQL 入口与独立加密格式验收；真实 Linux DuckDB 的完整采样差分和 Java 加密互通尚未执行。详细范围见下表。

## 目标与范围

将 `D:\WORK_FILE\zwyb_file\edge-agent-go` 当前生产路径中的 DuckDB 替换为 fssql，在 `CGO_ENABLED=0` 下完成构建和导出。保留 typed 输入、对象输入、规则处理、sampling、字段探测、CSV/Parquet 导出和加密 Parquet 的业务能力。

本计划以完善 fssql 为主体，edge-agent-go 是需求来源和集成验收场景。优先在引擎与公开 API 中解决类型、输入复用、导出和 SQL 能力缺口，不以适配层自行维护类型系统、spill 或导出器来代替。本轮暂不建设 ATTACH 或 PostgreSQL/MySQL scanner；其余 SQL 兼容入口按下文任务明确跟踪。

状态只对应列出的证据与范围；引擎包测试通过不代表 edge-agent-go 生产替换或所有 DuckDB 行为已经兼容。

## 兼容边界与前置决策

- 字段元数据和 typed 值必须保持类型、精度、NULL 和时间语义。
- CSV 必须区分 NULL 与空字符串；已建立 UTC、FORCE_QUOTE、BLOB 和时间类型的逐字节参考样本，兼容输出通过 CopyOptions 显式启用。
- 原生 SAMPLE 使用不同随机实现，相同 seed 不保证与 DuckDB 选中相同行。严格逐行一致的要求若保留，须新增兼容实现；不得仅凭 fssql 内部可复现就验收。
- MD5 算法相同不代表 sampling 结果必然相同，仍需验证规范化文本、编码、排序、阈值和并列值处理。
- 加密 Parquet 的 API 可用不代表现有 Java 消费端可读，必须以真实消费者互通测试验收。

## 原始 13 项逐项对照

| 原项 | 本项目缺口或限制 | fssql 改造任务 |
| --- | --- | --- |
| 1 | 无 SET；UTC、threads 配置与默认值不同 | F7：完善 Config 映射和默认行为契约；无需为接入建设通用 SET |
| 2 | VIEW 无 OR REPLACE/TEMP；无 CTAS | F4：先补可跨语句复用的物化；F8：补相应 DDL 入口 |
| 3 | 一次性 typed 输入不能被校验与主查询重复消费 | F4：提供显式物化对象和独立 reader，不让适配层另写 spill |
| 4 | COPY 缺 FORCE_QUOTE *、USE_TMP_FILE false、加密 SQL 选项 | F3：补强制引用；F6/F7：完善 CopyTo/ParquetKeys；保留原子提交 |
| 5 | 无 IS NOT DISTINCT FROM | F5：原生实现 NULL-safe 比较，OR 分支仅作临时绕行 |
| 6 | reader 名称、JSON format、columns 语法不同 | F8：补常用兼容入口和选项支持矩阵 |
| 7 | 无 ATTACH、PostgreSQL/MySQL scanner | F9：明确不支持；当前 ExportDB 无生产调用，暂不实施 |
| 8 | DESCRIBE 丢失 BLOB、时间戳修饰 | F1：修复类型描述，并覆盖 unsigned 与嵌套字段 |
| 9 | database/sql 类型名缺精度、时区名称不同 | F2：修复引擎公开元数据，不把补全推给客户端 |
| 10 | CSV BLOB、UTC 时间文本和 CAST 不同 | F3：补兼容输出和转换语义 |
| 11 | SAMPLE 的 RNG 与 DuckDB 不同 | F6a：独立实现和验证随机采样兼容路径 |
| 12 | Parquet 默认 identity 而非 snappy | F7：明确默认值及兼容配置，回读验证真实 codec |
| 13 | file://、RootDir、目标不得预创建 | F7：完善路径与提交契约、示例和失败回归 |

两处核对修正：TableFunction.Open 可以在不同查询中重新调用，限制在于上游是否可重读；当前 database/sql 已支持 `read_csv(?, header=?, columns=?)` 参数绑定，不能将所有选项概括为只接受直接书写的 SQL 字面量。

## fssql 主任务总表

下表为当前实施与验证状态。F6a 的验收限定于列明的版本、标准库、输入顺序及分批；F8 的完整 TEMP 连接隔离仍是明确缺口。F6b 按用户“先维持现状，标记任务”的要求保持现有加密实现，暂缓外部消费端验收。

| 项目 | 当前状态 | 证据或限制 |
| --- | --- | --- |
| F0 | 双版本基线已建立，端到端验收未完成 | DuckDB 2.0.0 与调用方 1.5.5 分开生成；调用方绑定、commit、扩展 SHA-256 已记录 |
| F1/F2 | 已实现，定向验证通过 | 完整 DESCRIBE/ColumnTypes；双版本类型对照与 Parquet 回读 schema |
| F3 | 已实现，定向验证通过 | 原生默认格式保留；ForceQuote/DuckDBCompatible、UTC CAST，双版本 CSV 字节对照 |
| F4 | 已实现，定向与 race 验证通过 | Materialize/SQL/Close；独立 reader、租约、取消、限额、8,193 行 spill、DB.Close |
| F5 | 已实现，定向验证通过 | 原语法展开改为单次求值；NULL 真值、nullability 与 cluster 连接 |
| F6a | 显式兼容路径已实现，固定范围差分通过 | 原生 reservoir 默认不变；SampleCompatibility 区分 libstdc++/MSVC。真实 Windows DuckDB 1.5.5/2.0.0 对照，另有 GCC shuffle 独立参照；尚未执行真实 Linux 引擎完整差分 |
| F6b | 用户要求暂缓 | 已找到 1.5.5 Linux 扩展并记录摘要；未执行 Java 互通，不标为通过 |
| F6c | 已实现并验证 | FORMAT ENCRYPTED_PARQUET / ENCRYPTION_CONFIG / from_base64；SQL/API 复用 writer，验证 raw key 校验、清理、条件写与原子提交 |
| F6d | 独立格式验收通过 | 独立 AES-GCM/AAD/Thrift 路径验证完整文件，覆盖多列/页/行组、压缩、NULL、错误 key/AAD 与篡改 |
| F7 | 已实现调用契约与回归 | Config 映射、CopyTo、显式 snappy、真实 codec 检查、路径与提交示例 |
| F8 | 已实现所列入口，存在明确范围限制 | CTAS/OR REPLACE/TEMP 的 DB 生命周期实现；reader 别名、columns 结构体、参数绑定通过。TEMP 不提供连接私有命名空间/同名遮蔽 |
| F9 | 已记录不支持，拒绝行为通过 | ATTACH、postgres_scan、mysql_scan 明确报错 |

公开调用与限制见 [duckdb-migration.md](contracts/duckdb-migration.md)。本轮没有修改 edge-agent-go，也没有把其端到端替换标记为完成。

| ID | 任务 | 主要落点 | 验收重点 |
| --- | --- | --- | --- |
| F0 | 固定版本与差分基线 | compatibility、test、相关包测试 | 13 项都有复现及比较标准 |
| F1 | 完整 DESCRIBE 类型文本 | internal/execution/result | 类型修饰和嵌套信息不丢失 |
| F2 | database/sql 元数据 | pkg/gosql/database_sql.go | DECIMAL(p,s)、时区类型名正确 |
| F3 | CSV 引用、格式与 CAST | pkg/gosql/copy.go、internal/export、表达式执行 | FORCE_QUOTE、NULL、BLOB、UTC 文本 |
| F4 | 跨语句类型化物化 | gosql、catalog、源运行时、临时存储 | 同一快照、独立 reader、有界 spill |
| F5 | NULL-safe 比较 | parser、binder、lowering、表达式执行 | cluster join 可原生执行 |
| F6a | DuckDB sampling 兼容 | runtime_sample_phase5.go | 固定 seed 的选中结果对照 |
| F6b | 加密 Parquet 互通 | Parquet writer、crypto、CopyTo | 现有 Java 消费端实际可读 |
| F6c | 加密 SQL 入口兼容 | COPY parser、binder、session、writer 选项 | 原有加密 SQL 可运行，与 API 写出使用同一实现 |
| F6d | 原生加密格式契约 | Parquet writer、crypto、独立格式测试 | 逐模块 key/nonce/AAD、压缩顺序及明文/密文边界正确 |
| F7 | 配置、压缩、文件契约 | Config、CopyTo、LocalFiles、文档 | 公开 API 可完成接入且失败语义清楚 |
| F8 | 必要 DDL 与 reader 入口 | parser、binder、catalog、reader 绑定 | CTAS、VIEW 组合及常用 reader 用法 |
| F9 | scanner 范围边界 | 能力矩阵 | 明确不支持，暂不建设 |

## fssql 主任务明细

### F0：差分基线

- [ ] 固定 fssql、DuckDB、edge-agent-go、加密扩展和 Java 消费端版本；记录输入、规则、seed 和配置。
- [ ] 将原始 13 项变成可复现用例；区分当前已知失败与尚未验证的推断。
- [ ] 类型文本精确比较；CSV 分别比较解析值、NULL/空串、引用规则与字节；Parquet 比较类型、值、codec 和消费端可读性。
- [ ] sampling 分别比较选中集合和有契约的行序；没有 ORDER BY 时不默认要求行序。

### F1：DESCRIBE

- [x] 修复 Binary、Unsigned、Timestamp 单位及 UTC 修饰的识别，输出 BLOB、unsigned 类型、TIMESTAMP_NS、TIMESTAMP WITH TIME ZONE。
- [x] 保留 DECIMAL(p,s)、普通 TIMESTAMP 和 LIST/STRUCT 内部完整类型，维持深度/长度上限。
- [x] 覆盖类型格式化、typed source、CAST 和可用的文件扫描路径；验证 typedCSVProjection 分支及 BLOB→UNKNOWN 分类。

### F2：database/sql 类型元数据

- [x] ColumnTypeDatabaseTypeName 基于完整 schema 生成类型文本，不能对所有类型只返回 Type.ID。
- [x] 与 F1 统一格式化规则，覆盖 DECIMAL 精度/scale、时区时间戳、纳秒、unsigned、BLOB 和嵌套类型。
- [x] 保持 native Type.ID 的机器可读含义，不为显示名称破坏 TableFunction 注册。
- [x] 验证 DescribeOutput；InspectObject 走 DESCRIBE，由 F1 同时闭环。客户端无需补造丢失的元数据。

### F3：CSV 与文本转换

- [x] CopyOptions 增加 FORCE_QUOTE * 所需能力，贯通选项传递层和 CSV writer；明确 header、非 NULL、NULL 和空串的引用规则。
- [x] 若支持 SQL COPY 对应选项，复用同一导出实现。
- [x] 分别固定对象路径默认 CSV 和 typed 显式投影的基线，补齐 BLOB、TIMESTAMPTZ 的文本差异，不能混淆两种输出契约。
- [x] 核对并修复 CAST(TIMESTAMPTZ AS VARCHAR) 与 timezone('UTC', ...) 后 CAST 的类型/文本语义，覆盖 +00、小数秒和分隔符。
- [x] 若必须保留已发布原生格式，提供明确兼容选项与默认值；不静默破坏既有公开行为。
- [x] 回归 NULL、空串、空 BLOB、引号、分隔符、CR/LF、Unicode、负时间与精度边界，保持流式写出和原子提交。

### F4：跨语句物化与重放

现有查询级物化见 [ADR-0009](architecture/adr-0009-query-scoped-registered-table-materialization.md)：它解决同一 SQL 的多次引用，查询结束后清理，不能供下一条语句复用。

- [x] 提供显式创建、查询和释放的类型化物化对象，使 KeyValidation 与主查询读取同一快照。
- [x] 复用 native chunk 序列化、managed memory、spill/page store；不以 CSV 作为中间格式，不要求适配层自行实现 spill。
- [x] 上游仅消费一次；发布后可创建独立 reader，覆盖跨语句和单查询重复引用。
- [x] 保留 DECIMAL、unsigned、BLOB、NULL、TIMESTAMP_NS、TIMESTAMPTZ 及辅助列，纳入内存/临时空间配额与统计。
- [x] 物化成功后才发布；失败、取消和超限不留半成品，也不重新请求外部数据库取得另一快照。
- [x] 明确对象释放、DB.Close、重复释放及使用已关闭对象的行为，回收 reader、内存、spill 与 catalog 引用。
- [x] 提供“注册一次性输入→物化→校验→主查询→释放”示例及大输入回归。

### F5：IS NOT DISTINCT FROM

- [x] 贯通 parser/AST、binder、lowering 和执行；两边 NULL 为 true，单边 NULL 为 false，否则按等值比较。
- [x] 验证绑定、结果 nullability、NULL 常量、批执行与现有数值/字符串/时间比较规则。
- [x] 覆盖 SELECT、WHERE、JOIN ON 和 cluster 多列 NULL 键，与 OR/IS NULL 分支及 DuckDB 对照。
- [x] 共用语法分支时配套处理 IS DISTINCT FROM，不能仅语法接受而执行失败。

### F6a：原生 SAMPLE 兼容

Bernoulli/system 保留已验证的 PCG 路径。reservoir 默认仍使用既有 Go 随机流；通过 Config.SampleCompatibility 显式启用兼容路径：`duckdb` 对应 libstdc++，`duckdb-msvc` 对应固定 Windows wheel 使用的 MSVC。兼容路径实现初始 shuffle、按输入批次 replacement、122,880 行后的加权 skip/heap，以及按 100,000 行分段的百分比采样和尾段重采样；数据仍使用原生 chunk 序列化与 spill store，候选元数据计入内存配额。

PCG 实现参考固定版本 DuckDB 的 [RandomEngine](https://github.com/duckdb/duckdb/blob/v1.5.5/src/common/random_engine.cpp)，reservoir 参考同版本 `reservoir_sample.cpp` / `base_reservoir_sample.cpp`。`std::shuffle` 是标准库相关行为，不能只按 DuckDB 版本假定跨平台结果相同。固定 seed 必须同时匹配输入顺序与实际分批；本轮真实引擎参考固定 threads=1，不承诺不同分批或不同并行到达顺序仍选中相同行。兼容路径拒绝超过 signed int64 的显式 seed。

- [x] 按实际使用方法核对 RNG、seed、随机数消费、批次与并行边界；显式区分标准库 profile。
- [x] 实现显式 reservoir 兼容路径；Bernoulli/system 复用已有验证路径，不以 MD5 代替随机算法。
- [x] 对照 Windows DuckDB 1.5.5/2.0.0：三个 seed、空输入、0/100%、超出输入的行数、137/2048 行分批、最多 200,001 行、快速/慢速替换及百分比分段。GCC 15.2.0 的 std::shuffle 独立参照覆盖两种分布分支和随机数消费。
- [x] MD5 另以两版本真实引擎对照调用方使用的长度前缀文本、DECIMAL(38,6)、负时间/毫秒、Unicode/组合字符、NULL/空串、阈值等号与重复键并列排序；另用 Go crypto/md5 校验 UTF-8 字节摘要。
- [ ] 在目标 Linux DuckDB 二进制上执行完整 reservoir 差分；当前 libstdc++ shuffle 参照不等价于已完成该外部引擎验收。

### F6b：加密 Parquet 互通

状态：用户要求维持现状，暂缓互通验收。下面任务继续保留为未完成；不修改现有加密格式，不以 writer 成功代替消费端读取。

- [ ] 复用 CopyOptions.Encryption/ParquetKeys，核对 keyId、密钥规范化、算法、footer 和 key metadata。
- [ ] 由现有 Java 消费端读取实际产物；不兼容时定位并修复本项目 writer/crypto 差异。
- [ ] 验证错误 keyId、错误密钥、密文篡改、NULL 和类型；失败不回退明文，密钥不进入 SQL、日志和 manifest。
- [ ] 保存互通证据，不能仅凭 writer 成功或文件存在验收。

### F6c：加密 SQL 入口兼容

状态：已实现并通过定向验证；Java 消费端互通仍由 F6b 独立跟踪。SQL 只接受所列静态结构体与 from_base64 字符串常量，不提供任意运行时选项求值。

目标是兼容 edge-agent-go 当前自编译 DuckDB 扩展的调用形态：

```sql
COPY (SELECT ...) TO 'file:///.../out.parquet' (
  FORMAT ENCRYPTED_PARQUET,
  ENCRYPTION_CONFIG {
    footer_key_value: from_base64('raw AES key 的 Base64'),
    master_key_id: 'key-001'
  }
);
```

- [x] 补齐 COPY 选项中的 ENCRYPTION_CONFIG 结构体解析、FORMAT ENCRYPTED_PARQUET 和 from_base64 常量解码；限定所需表达式范围，不扩大为任意运行时函数求值。
- [x] 校验必填字段、重复/未知字段、严格标准 Base64（带 padding、无换行）、raw key 长度和格式组合；直接使用解码后的 raw AES key，按 16/32 字节映射当前支持的 AES-128/256-GCM，不做口令派生或 DEK 包装。
- [x] 将绑定结果映射到现有 ParquetEncryptionConfig 和同一 writer；Go API 的 ParquetKeys 解析路径继续可用，不要求 SQL 调用方额外注册回调。
- [x] 诊断及 bound/AST 格式化脱敏，manifest 仅保存算法/KeyID。绑定失败清理已解码副本；执行结束或查询关闭清理 bound key，writer 的独立副本在 Close/Abort/失败时清理。调用方 SQL 与不可变字符串 AST 由调用方/prepared statement 生命周期持有并由 Go GC 回收，不承诺原地抹除；调用方不得记录含 Base64 的完整 SQL。
- [x] 对照 SQL/API 两种入口，验证相同的格式、算法和 KeyID。独立检查 file AAD/nonce 与解密值，不以密文字节相等作为等价标准。
- [x] 覆盖 AES-128/256、snappy/identity、空结果、4,101 行两列多页/三行组、上游失败/取消、重复目标与临时文件清理；缺少加密配置或校验失败报错，不回退明文。

验证发现本地旧 AtomicPublishWrite 允许替换已有目标，已为 SQL COPY/CopyTo 单文件路径强制 IfNoneMatch，并由本地 driver 在发布时使用原子 hard link 条件创建。并发打开的 writer 只能有一个成功发布，失败方只删除自己的 staging 文件；普通显式替换写保持原契约。

验收：原 SQL 形态可执行并产生符合 F6d 的加密文件；保留 COPY 的条件写和原子提交。Java 消费端互通仍由 F6b 独立验收。

### F6d：原生 Parquet 加密格式契约

用户指定的目标：同一把 raw AES key 独立加密所有列的 Page Header、压缩后的 Page Data，以及序列化的 FileMetaData；每个 crypto module 独立生成随机 nonce，使用对应的标准 module AAD。FileCryptoMetaData 保持明文，只保存算法、file unique AAD、KeyID 等非密钥元数据。

2026-11-05 先完成源码核对，2026-11-06 补齐下列完整文件的独立格式验收；两者均不代表 Java 互通通过。标准依据为 [Apache Parquet Modular Encryption](https://parquet.apache.org/docs/file-format/data-pages/encryption/)，尤其 4.4.2 与 5.4。

| 项目 | 当前实现 |
| --- | --- |
| 密钥 | 16/32 字节 raw AES key；所有列与 footer 使用同一值，无列级派生/随机 DEK |
| 数据页 | PLAIN/RLE 编码后按选项压缩，再加密 page body；identity 表示无压缩 |
| 页头/footer | Thrift 序列化后各自独立 AES-GCM 加密 |
| nonce | 每次 sealParquetModule 用 crypto/rand 生成 12 字节 nonce |
| file unique AAD | 每个 writer 用 crypto/rand 生成 8 字节文件标识 |
| footer AAD | file unique ID + 模块号 0，无行组/列/页序号 |
| Page Data AAD | file unique ID + 模块号 2 + 行组/列/页序号，各序号为 2 字节小端 |
| Page Header AAD | 同上，模块号为 4 |
| FileCryptoMetaData | 明文保存 AES_GCM_V1、aad_file_unique、key_metadata；不含 raw key 或数据 schema |
| Key metadata | 当前 JSON 含 masterKeyID、footer key ID、duckdbDirectKey 等标志；wrappedDEK 为空，未写入密钥 |
| 文件封装 | PARE + 加密页模块 + 明文 FileCryptoMetaData + 加密 FileMetaData + 二者总长度 + PARE |

- [x] 根据实际 writer/crypto 源码核对上述 key、nonce、AAD 和明文/密文边界。
- [x] `test/parquetcheck` 不导入生产 writer、AAD 或 Thrift 编解码器，独立构造标准 AAD、AES-GCM 解密并解析 compact Thrift；检查完整文件的明文 crypto metadata、密文 footer、页头、页体与字节边界。
- [x] SQL/API 产物覆盖多列、多页、多行组，逐模块检查 nonce 唯一、AAD、解密后再解压的数据与 NULL/原始逻辑值；每个模块均验证错误 key、修改 AAD 的每个字节、nonce/密文/tag 篡改失败。
- [x] 当前只生成数据页（模块 2）、数据页头（模块 4）及 footer（模块 0）。没有 dictionary/index/bloom 模块；未来增加时须补齐对应标准 module type/AAD 和独立测试。

备注：当前普通 Parquet reader 仅支持 PAR1，不提供加密回读；独立测试工具仅供格式验收，不是新公开 reader。Java 消费端互通按用户要求继续暂缓。

### F7：配置与导出契约

- [x] 列出生产 SET 的 Config 映射、默认值、无对应项及删除理由；验证 UTC、Workers、内存和临时目录行为。
- [x] 提供 CSV、snappy Parquet、加密 Parquet 的 CopyTo 示例，不要求拼接 DuckDB COPY。
- [x] 明确当前 Parquet 默认 identity；兼容调用显式 snappy 并回读 codec。全局默认是否调整单独评估兼容影响。
- [x] 完善 FileURI、RootDir、目标不得预创建的示例，验证目标冲突、越界路径、取消与失败清理。
- [x] 保留条件写和原子发布，不为 USE_TMP_FILE false 降低提交保证；接入发现必要配置缺失时补具体 API。

### F8：DDL 与 reader 兼容入口

已实现的 TEMP 使用 native DB 作为会话边界，所有对象随 DB 关闭释放。完整 DuckDB 的连接私有 TEMP 命名空间、与 main 同名对象的遮蔽仍未实现，不能据当前测试宣称支持。

- [x] 基于 F4 增加 CTAS 入口，明确表生命周期、schema 与失败原子性，不顺带扩展完整 DML/Appender。
- [ ] 补实际需要的 OR REPLACE VIEW、TEMP VIEW/TABLE 组合，明确替换失败时旧对象、依赖及隔离行为。
- [x] 允许先交付物化 API，但 SQL 缺口仍单列，不以普通 VIEW 绕行宣称已经兼容。
- [x] read_csv_auto 复用 read_csv；read_json_auto 的 newline_delimited 用法复用 read_ndjson，明确其他 format 支持范围。
- [x] 为实际使用的 reader 选项建立矩阵；columns 结构体语法按需求补绑定转换，与字符串 schema 共用扫描逻辑。
- [x] 回归已有 database/sql 参数绑定，不重复建设；不支持的选项明确报错。

### F9：scanner 边界

- [x] 记录 ATTACH、PostgreSQL/MySQL scanner 不支持，并复查 ExportDB 当前无生产调用的结论。
- [x] 对相关入口明确拒绝，不声称 TableFunction 等价实现 scanner；未来启用时另立连接、认证、类型、推送与快照任务。

## 主任务最终验收

- [ ] fssql 相关测试与生产构建在 CGO_ENABLED=0 下通过；类型、CSV、采样和加密具备差分/互通证据。
- [ ] 13 项逐项记录完成、明确不支持或尚未完成，不能只按迁移能运行来关闭任务。
- [ ] edge-agent-go 仅作必要调用适配与端到端验证，不另写类型系统、spill 或导出器替代上述引擎任务。
- [ ] 覆盖 typed/对象输入、规则、采样、字段探测、CSV/Parquet、大输入、取消、超限与失败清理。

## 已执行的初步验证

2026-11-06，F6 追加验证（Go 1.25.11，windows/amd64）：

- `CGO_ENABLED=0 go test ./...` 与 `CGO_ENABLED=0 go build ./...` 通过，最终日志为 `.cache/edge-migration-verification/f6-go-test.log` / `f6-go-build.log`。初次全量运行发现旧 COPY 测试驱动未声明/实现条件创建，已修正并增加拒绝覆盖测试；另一次 Windows manifest 替换 Access denied 在单独及最终全量重跑中通过，没有为此修改 manifest 实现。
- 定向 `go test -race` 覆盖加密 SQL/API、bound key 清理、采样取消/超限/失败回收、慢速 reservoir 和本地竞争发布；通过，日志为 `.cache/edge-migration-verification/f6-race.log` / `f6-race-slow.log`。仅 race 检查开启 CGO，生产无此依赖。
- `TestDuckDBReservoirReference`：两版本共 150 个真实 Windows DuckDB 参考用例；比较选中集合摘要与固定执行条件下的输出顺序摘要。参考由 `test/integration/generate_reservoir_reference.py` 生成，版本/平台不匹配即拒绝。
- `TestDuckDBReservoirLibstdcppShuffle`：GCC 15.2.0 独立 std::shuffle 程序生成 15 组参考，覆盖 paired/fallback 分支、结果与后续 RNG 状态。C++ 仅用于离线生成参考，不是 Go 构建或测试依赖。
- `TestSamplingCanonicalHashReference`：两版本真实 DuckDB 生成规范化文本、MD5、Top-N、阈值及并列键参考；`TestEncryptedCopySQLAndAPI` / `test/parquetcheck` 完成 F6c/F6d 独立文件验收。加密原语、密钥及格式处理继续复用原 writer，未修改现有加密格式。

尚未执行目标 Linux DuckDB 的完整 reservoir 差分、Java 消费端加密互通或 edge-agent-go 生产替换，不因上述测试通过关闭这些外部验收。

本轮实施后的验证：

- `CGO_ENABLED=0 go test ./...` 通过；日志保存在工作区 `.cache/edge-migration-verification/go-test.log`。
- 定向 `go test -race` 已覆盖 gosql 新功能、内存 catalog 和类型格式化；race 工具需要 CGO，仅测试时启用，不引入生产 CGO 依赖。
- `TestEdgeMigrationReference`：对照两套真实 DuckDB 生成的类型、CSV 和 UTC CAST；验证 snappy 实际 codec 与 Parquet schema 回读。
- `TestMaterialized*`：跨语句快照、resident/spill 两种路径、已绑定 reader 租约、上游错误、取消、临时空间超限、DB 关闭及 8,193 行逐行重放。
- `TestMigration*`：CTAS、替换失败保留旧视图、reader 兼容入口、参数绑定、NULL-safe 单次求值、CSV FORCE_QUOTE 和 scanner 拒绝。
- `TestCatalogAtomicViewReplacement`：内存与 SQLite adapter 的创建/替换/回滚；SQLite adapter 测试使用现有 database/sql 测试驱动，不声称已执行外部 SQLite 引擎。

参考答案由 `test/integration/generate_edge_migration_reference.py` 使用实际 DuckDB 生成，版本不符即失败；文件为 `test/corpus/edge-agent-migration-reference.json`（2.0.0）与 `edge-agent-caller-reference.json`（1.5.5）。调用方专用依赖锁在 `test/integration/requirements-edge-agent.txt`。没有执行 Java 加密互通或 edge-agent-go 生产构建。

以下保留改造前的初步检查记录：

2026-11-05，fssql 运行以下相关包测试，设置 `CGO_ENABLED=0`：

```text
go test ./pkg/gosql ./internal/execution/result ./internal/export
```

首次运行中，`internal/execution/result` 和 `internal/export` 通过；`pkg/gosql` 有 5 个文件相关用例因系统临时目录访问被拒绝而失败。将 TEMP/TMP 指向工作区 `.cache/migration-test-tmp` 后，单独重跑 `go test ./pkg/gosql` 通过。

以上只证明相关包在本次环境的无 CGO 测试结果，不代表已完成 edge-agent-go 替换、全项目测试、DuckDB 差分或加密消费端验证。
