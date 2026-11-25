package session

import (
	"context"
	"fmt"
	"sync"

	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/format/csv"
	jsonformat "github.com/steamwo/fssql/internal/format/json"
	"github.com/steamwo/fssql/internal/format/parquet"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
	"github.com/steamwo/fssql/internal/storage/fileset"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// sourceBindFunc 是 Stable table-function adapter 对真实 format Bind 的最小依赖。
// 返回字段和 DatasetFingerprint；SQL Binder 不接触 concrete format binding type。
type sourceBindFunc func(context.Context, string) ([]types.StructField, string, error)

// fileSetBindFunc 是 glob() 对统一 FileSetPlanner 的最小依赖。
type fileSetBindFunc func(context.Context, string) (*fileset.ScanSet, error)

// StableTableFunctionResolver 把 FunctionCatalog 的 Stable table function 映射到真实格式 Bind。
// 它属于 session/engine composition boundary，不把 Parquet/CSV/JSON 类型泄漏进 SQL Binder ABI。
type StableTableFunctionResolver struct {
	parquet    sourceBindFunc  // parquet 复用 parquet.SourceBinder.Bind。
	csv        sourceBindFunc  // csv 复用 csv.SourceBinder.Bind。
	json       sourceBindFunc  // json 复用 JSONBinder.Bind。
	ndjson     sourceBindFunc  // ndjson 复用 NDJSONBinder.Bind。
	glob       fileSetBindFunc // glob 只冻结并返回 FileSetPlanner ScanSet。
	statistics *boundSourceStatistics
}

// Only the statement's own bindings populate these snapshot estimates.
type boundSourceStatistics struct {
	entries sync.Map
}

func (s *boundSourceStatistics) Lookup(_ context.Context, fp stats.DatasetFingerprint) (stats.RelationStatistics, bool, error) {
	value, ok := s.entries.Load(fp)
	if !ok {
		return stats.RelationStatistics{}, false, nil
	}
	return value.(stats.RelationStatistics), true, nil
}

var _ binder.TableFunctionResolver = (*StableTableFunctionResolver)(nil)

// NewStableTableFunctionResolver 创建生产 resolver；所有 reader 必须显式配置，禁止缺失时静默降级。
func NewStableTableFunctionResolver(
	registry *vfs.Registry,
	parquetBinder *parquet.SourceBinder,
	csvBinder *csv.SourceBinder,
	jsonBinder *jsonformat.JSONBinder,
	ndjsonBinder *jsonformat.NDJSONBinder,
) (*StableTableFunctionResolver, error) {
	if registry == nil || parquetBinder == nil || csvBinder == nil || jsonBinder == nil || ndjsonBinder == nil {
		return nil, fmt.Errorf("session: Stable table function resolver requires VFS and all Stable source binders")
	}
	planner := fileset.NewPlanner(registry)
	statistics := &boundSourceStatistics{}
	return &StableTableFunctionResolver{
		statistics: statistics,
		parquet: func(ctx context.Context, source string) ([]types.StructField, string, error) {
			binding, err := parquetBinder.Bind(ctx, source, parquet.SourceBindOptions{})
			if err != nil {
				return nil, "", err
			}
			if rows, ok := binding.RowCount(); ok {
				fp := stats.DatasetFingerprint(binding.Fingerprint())
				statistics.entries.Store(fp, stats.RelationStatistics{Fingerprint: fp, Source: stats.StorageNative, Confidence: 1, Rows: rows})
			}
			return cloneStructFields(binding.Schema().Fields), binding.Fingerprint(), nil
		},
		csv: func(ctx context.Context, source string) ([]types.StructField, string, error) {
			binding, err := csvBinder.Bind(ctx, source, csv.SourceBindOptions{})
			if err != nil {
				return nil, "", err
			}
			// A coarse byte-to-row estimate provides relative input sizes without
			// sampling explicit-schema CSVs or requiring an extra file read.
			var rows uint64
			for _, file := range binding.ScanSet().Files() {
				if file.Size > 0 {
					estimate := uint64(file.Size-1)/32 + 1
					if estimate > ^uint64(0)-rows {
						rows = ^uint64(0)
						break
					}
					rows += estimate
				}
			}
			if rows > 0 {
				fp := stats.DatasetFingerprint(binding.Fingerprint())
				statistics.entries.LoadOrStore(fp, stats.RelationStatistics{Fingerprint: fp, Source: stats.StorageEstimated, Confidence: 0.2, Rows: rows})
			}
			return cloneStructFields(binding.Schema().Fields), binding.Fingerprint(), nil
		},
		json: func(ctx context.Context, source string) ([]types.StructField, string, error) {
			binding, err := jsonBinder.Bind(ctx, source, jsonformat.JSONBindOptions{})
			if err != nil {
				return nil, "", err
			}
			return cloneStructFields(binding.Schema().Fields), binding.Fingerprint(), nil
		},
		ndjson: func(ctx context.Context, source string) ([]types.StructField, string, error) {
			binding, err := ndjsonBinder.Bind(ctx, source, jsonformat.NDJSONBindOptions{})
			if err != nil {
				return nil, "", err
			}
			return cloneStructFields(binding.Schema().Fields), binding.Fingerprint(), nil
		},
		glob: func(ctx context.Context, source string) (*fileset.ScanSet, error) {
			return planner.Plan(ctx, source, fileset.PlanOptions{})
		},
	}, nil
}

// BindTableFunction 实现 SQL Binder 的 generic TableFunctionResolver。
// Stable reader 的 schema/fingerprint 只能来自真实 Bind；glob schema 是冻结的单列 file VARCHAR contract。
func (r *StableTableFunctionResolver) BindTableFunction(ctx context.Context, def function.Definition, args []binder.TableFunctionArgument) (binder.ResolvedRelation, error) {
	if r == nil {
		return binder.ResolvedRelation{}, fmt.Errorf("session: nil Stable table function resolver")
	}
	if err := ctx.Err(); err != nil {
		return binder.ResolvedRelation{}, err
	}
	if def.Kind != function.Table {
		return binder.ResolvedRelation{}, fmt.Errorf("session: function %s is not a table function", def.Name)
	}
	if len(args) != 1 || args[0].Type.ID != types.Varchar || args[0].Value == "" {
		return binder.ResolvedRelation{}, fmt.Errorf("session: table function %s requires one non-empty VARCHAR source", def.Name)
	}
	source := args[0].Value

	if def.ID == function.GlobID {
		if r.glob == nil {
			return binder.ResolvedRelation{}, fmt.Errorf("session: glob FileSetPlanner is not configured")
		}
		scanSet, err := r.glob(ctx, source)
		if err != nil {
			return binder.ResolvedRelation{}, err
		}
		if scanSet == nil || scanSet.Fingerprint() == "" {
			return binder.ResolvedRelation{}, fmt.Errorf("session: glob returned incomplete ScanSet")
		}
		return binder.ResolvedRelation{
			ID:          relationID(def.Name, scanSet.Fingerprint()),
			Columns:     []binder.RelationColumn{{Name: "file", Type: types.LogicalType{ID: types.Varchar}, Nullable: false}},
			Fingerprint: stats.DatasetFingerprint(scanSet.Fingerprint()),
		}, nil
	}

	bind, err := r.readerBind(def.ID)
	if err != nil {
		return binder.ResolvedRelation{}, err
	}
	fields, fingerprint, err := bind(ctx, source)
	if err != nil {
		return binder.ResolvedRelation{}, err
	}
	if len(fields) == 0 || fingerprint == "" {
		return binder.ResolvedRelation{}, fmt.Errorf("session: table function %s returned incomplete schema identity", def.Name)
	}
	columns := make([]binder.RelationColumn, len(fields))
	for i, field := range fields {
		if field.Name == "" || field.Type.ID == types.Invalid {
			return binder.ResolvedRelation{}, fmt.Errorf("session: table function %s returned invalid column %d", def.Name, i)
		}
		// Format schema metadata 当前不携带统一 NOT NULL bit；保守标记 nullable，不能凭格式猜约束。
		columns[i] = binder.RelationColumn{Name: field.Name, Type: cloneLogicalType(field.Type), Nullable: true}
	}
	return binder.ResolvedRelation{
		ID:          relationID(def.Name, fingerprint),
		Columns:     columns,
		Fingerprint: stats.DatasetFingerprint(fingerprint),
	}, nil
}

// readerBind 按 FunctionCatalog stable ID 选择真实 Bind callback；未知 table function 不做名称猜测。
func (r *StableTableFunctionResolver) readerBind(id function.ID) (sourceBindFunc, error) {
	var bind sourceBindFunc
	switch id {
	case function.ReadParquetID:
		bind = r.parquet
	case function.ReadCSVID:
		bind = r.csv
	case function.ReadJSONID:
		bind = r.json
	case function.ReadNDJSONID:
		bind = r.ndjson
	default:
		return nil, fmt.Errorf("session: unsupported Stable table function id %d", id)
	}
	if bind == nil {
		return nil, fmt.Errorf("session: Stable table function id %d is not configured", id)
	}
	return bind, nil
}

// relationID 将真实 dataset fingerprint 纳入 relation identity，避免同名函数跨 snapshot 混淆。
func relationID(name, fingerprint string) string { return "tablefunc:" + name + ":" + fingerprint }

// cloneStructFields 深拷贝复杂 LogicalType metadata，避免 adapter 泄漏 format binding 内部 slice/pointer ownership。
func cloneStructFields(fields []types.StructField) []types.StructField {
	out := make([]types.StructField, len(fields))
	for i, field := range fields {
		out[i] = types.StructField{Name: field.Name, Type: cloneLogicalType(field.Type)}
	}
	return out
}

// cloneLogicalType 递归复制 LIST/STRUCT metadata。
func cloneLogicalType(t types.LogicalType) types.LogicalType {
	out := t
	if t.Child != nil {
		child := cloneLogicalType(*t.Child)
		out.Child = &child
	}
	if t.Fields != nil {
		out.Fields = cloneStructFields(t.Fields)
	}
	return out
}
