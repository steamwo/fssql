# DuckDB 调用场景的公开能力

## 配置与默认值

fssql 不执行通用 SET。引擎时间函数使用 UTC；没有可变会话时区。DuckDB 的 `SET TimeZone='UTC'` 不需要重复执行。`threads` 对应 Config.Workers；未指定时执行器默认 1。内存、临时空间和目录分别使用 MemoryLimitBytes、TempLimitBytes、SpillMemoryBudgetBytes、TempDir、ManifestDir。扩展加载与 force_mbedtls_unsafe 无对应项；加密导出支持 CopyTo/ParquetKeys 与下列受限 SQL 入口。

SQL COPY 和 CopyTo 的 Parquet 默认仍为 identity；需要 DuckDB 默认行为时显式使用 `Compression: "snappy"`，或 SQL `COMPRESSION snappy`。不静默修改原生 API 的已有默认值。

## 类型元数据与 CSV

DESCRIBE 和 database/sql 的 DatabaseTypeName 共用完整类型名格式化规则。DECIMAL 输出精度/scale，UTC 时间点使用 TIMESTAMP WITH TIME ZONE。原生 Type.ID 仍使用 DECIMAL、TIMESTAMPTZ 等稳定机器标识。

CopyOptions.ForceQuote 对所有非 NULL CSV 字段和表头加引号；NULL 仍是未引用空字段，空字符串为引用空字段。SQL 支持 `FORCE_QUOTE *`。

CopyOptions.DuckDBCompatible 对 CSV 使用 DuckDB BLOB 文本（可打印 ASCII 加 `\xHH` 转义）和 UTC 时间点文本。未启用时保留原生 CSV 的完整大写 hex 和 RFC3339 时间表示。该选项与 ForceQuote 独立；两项仅对 CSV 有效。

CAST(TIMESTAMPTZ AS VARCHAR) 在 UTC 文本后保留 +00；timezone('UTC', TIMESTAMPTZ) 返回无时区 TIMESTAMP，CAST 后不含后缀。与旧实现相比，这是有意修复的类型/文本语义。

## 一次性输入的跨语句复用

```go
snapshot, err := db.Materialize(ctx, "SELECT * FROM input_rows()")
if err != nil { return err }
defer snapshot.Close()
// 两次执行 snapshot.SQL()，均读取同一份不可变输入；上游只读取一次。
_, err = db.QueryChunks(ctx, snapshot.SQL(), validateChunk)
if err != nil { return err }
_, err = db.CopyTo(ctx, snapshot.SQL(), destination, gosql.CopyOptions{
    Format: gosql.CopyFormatCSV,
    ForceQuote: true,
    DuckDBCompatible: true,
})
```

Materialize 使用原生 chunk 序列化，受共享内存/临时空间配额约束；超过 SpillMemoryBudgetBytes 后溢写。发布前发生任何错误都清理半成品。成功后的 snapshot 必须 Close，DB.Close 也会回收。Close 后禁止新绑定，已绑定查询的 reader 在查询清理时释放最后一份存储租约。snapshot.SQL() 只可在创建它的 DB 中执行。

`CREATE [TEMP|TEMPORARY] TABLE name AS SELECT ...` 使用同一物化实现，在完成读取后原子发布 catalog 对象。它是只读快照，不提供 INSERT/Appender。`CREATE [OR REPLACE] [TEMP|TEMPORARY] VIEW` 复用普通视图与事务 catalog；替换失败保留旧视图，不能替换真实表。

当前 native DB 是会话边界，所有内存 catalog 对象均为 DB 生命周期。TEMP 不提供额外的 database/sql 连接私有命名空间或与 main 同名对象的遮蔽；需要会话隔离时使用独立 DB。该限制必须在兼容验收中单列，不能按完整 DuckDB TEMP 语义宣称支持。

## Reader 兼容入口

| 入口 | 处理 |
| --- | --- |
| read_csv_auto | 规范化为 read_csv，复用绑定和扫描 |
| read_csv(..., columns='id BIGINT,note VARCHAR') | 字符串 schema |
| read_csv(..., columns={'id':'BIGINT','note':'VARCHAR'}) | 静态结构体语法转换到相同 schema 绑定路径 |
| read_csv_auto(?, header=?, columns=?) | database/sql 参数绑定；columns 参数为字符串 schema |
| read_json_auto(..., format='newline_delimited') | 规范化为 read_ndjson |
| read_json_auto(...) | 规范化为 read_json；其他 format 值明确拒绝 |

CSV 选项以实际绑定器为准，不识别的选项明确拒绝；JSON 别名不静默接受其他命名选项。结构体 columns 的类型值必须是静态字符串。

## 文件与提交

LocalFiles.RootDir 必须是绝对路径。输入和输出使用 FileURI 生成的 file:// URI，路径受 RootDir 限制。只创建父目录并生成唯一目标名，不预创建输出文件。目标已存在时拒绝覆盖；CopyTo 成功返回表示原子发布已完成。USE_TMP_FILE false 不映射为放弃原子提交。

```go
destination, err := gosql.FileURI(filepath.Join(root, "result.parquet"))
if err != nil { return err }
result, err := db.CopyTo(ctx, query, destination, gosql.CopyOptions{
    Format: gosql.CopyFormatParquet, Compression: "snappy",
})
```

API 加密输出在上述 options 中设置 Encryption，在 Config.ParquetKeys 中按 keyId 和 algorithm 解析密钥。推荐此方式避免 SQL 字符串携带密钥；外部 Java 消费端互通按用户要求暂缓验收。

兼容 SQL 支持以下形态，无需注册 ParquetKeys：

```sql
COPY (SELECT ...) TO 'file:///.../out.parquet' (
  FORMAT ENCRYPTED_PARQUET, COMPRESSION snappy,
  ENCRYPTION_CONFIG {
    footer_key_value: from_base64('raw AES key 的标准 Base64'),
    master_key_id: 'key-001'
  }
);
```

ENCRYPTION_CONFIG 必须与显式 FORMAT ENCRYPTED_PARQUET 同时使用，且只允许上述两个字段与静态字符串常量。Base64 必须带标准 padding、无换行；解码后直接使用 16/32 字节 raw key，分别映射 AES-128/256-GCM，不派生或包装密钥。缺项、重复/未知字段、错误 Base64/key 长度、CSV 专用选项或其他格式组合均拒绝，不降级为明文。压缩默认 identity，也可显式 snappy。

绑定产生的 raw key 自有副本在绑定失败、执行结束或查询关闭时清理；writer 独立副本在 Close/Abort/失败时清理。诊断、bound/AST 格式化与 manifest 不输出 raw key/Base64；完整 SQL/prepared AST 仍持有调用方的不可变字符串，由其生命周期与 Go GC 回收，不能承诺原地抹除，调用方不得记录这类完整 SQL。COPY/CopyTo 单文件路径在最终发布时条件创建目标，拒绝覆盖已有对象并保持原子可见性；本地文件系统须支持同目录 hard link。

原生加密目标及已验证行为：所有列和 footer 直接使用同一 raw AES key；数据页先压缩再加密，页头与 FileMetaData 先序列化再独立加密；每个模块生成独立随机 12 字节 nonce，使用标准 module AAD。FileCryptoMetaData 保持明文，保存 AES_GCM_V1、file unique AAD 和 KeyID 等元数据，不保存 raw key。`test/parquetcheck` 使用独立 AES-GCM/AAD/compact Thrift 路径完成 SQL/API 完整文件验证，包括原始值、压缩顺序、多列/页/行组和逐模块认证失败；这不代表已完成 Java 互通或提供公开加密 reader。当前仅生成数据页、数据页头及 footer，不生成 dictionary/index/bloom 模块。

## 随机采样

reservoir 默认保持原生 Go 随机流。Config.SampleCompatibility 可显式设为 `duckdb`（libstdc++）或 `duckdb-msvc`（MSVC）；它们固定 DuckDB 1.5.5/2.0.0 的初始 shuffle、按批 replacement、慢速加权 skip/heap 和百分比分段/重采样路径。Bernoulli/system 继续使用已有 PCG 实现。

固定 seed 的相同结果要求相同输入顺序、实际分批及标准库 profile；不同并行到达顺序不在逐行一致保证内，兼容路径显式 seed 上限为 signed int64。当前真实引擎差分使用 Windows 两版本、threads=1、137/2048 行输入批、三个 seed、空输入与最多 200,001 行；libstdc++ 的 std::shuffle 另有 GCC 15.2.0 独立参照。真实目标 Linux DuckDB 的完整差分仍未执行，不能将 MSVC 结果当作其验收。原始 MD5 采样 SQL 另有规范化文本、Unicode、DECIMAL/时间、阈值与并列键差分，不以 MD5 替代 SAMPLE 算法。

## 未承诺能力

ATTACH、PostgreSQL/MySQL scanner 明确不支持。TableFunction 不是 scanner 的等价替代。SAMPLE 的跨引擎兼容以差分证据的具体方法、版本、标准库与输入分批为准；完整 TEMP 隔离仍未实现。
