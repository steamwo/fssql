# DuckDB 调用场景的公开能力

## 配置与默认值

fssql 不执行通用 SET。引擎时间函数使用 UTC；没有可变会话时区。DuckDB 的 `SET TimeZone='UTC'` 不需要重复执行。`threads` 对应 Config.Workers；未指定时执行器默认 1。内存、临时空间和目录分别使用 MemoryLimitBytes、TempLimitBytes、SpillMemoryBudgetBytes、TempDir、ManifestDir。扩展加载与 force_mbedtls_unsafe 无对应项；加密导出通过 CopyTo 和 ParquetKeys 完成。

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

加密输出在上述 options 中设置 Encryption，在 Config.ParquetKeys 中按 keyId 和 algorithm 解析密钥；不能把密钥拼入 SQL。既有加密能力维持现状，外部 Java 消费端互通按用户要求暂缓验收。

上句描述当前可用 API。原有 `FORMAT ENCRYPTED_PARQUET, ENCRYPTION_CONFIG {footer_key_value: from_base64(...), master_key_id: ...}` 加密 SQL 入口当前尚不支持，已列入 F6c 计划；这项 SQL 兼容建设与暂缓的 Java 互通分开推进。

原生加密目标及当前源码行为：所有列和 footer 直接使用同一 raw AES key；数据页先压缩再加密，页头与 FileMetaData 先序列化再独立加密；每个模块生成独立随机 12 字节 nonce，使用标准 module AAD。FileCryptoMetaData 保持明文，保存 AES_GCM_V1、file unique AAD 和 KeyID 等元数据，不保存 raw key。完整文件的独立标准验收仍待 F6d 补齐。

## 未承诺能力

ATTACH、PostgreSQL/MySQL scanner 明确不支持。TableFunction 不是 scanner 的等价替代。原生 SAMPLE 的跨引擎兼容以差分证据的具体方法、版本和输入分批为准；reservoir 与完整 TEMP 隔离不能据通用测试通过认定兼容。
