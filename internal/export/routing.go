package export

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

var ErrInvalidRouting = errors.New("export: invalid file routing")

// LayoutMode 描述 Phase 4 已冻结的单文件、多文件和分区数据集三种布局。
type LayoutMode string

const (
	LayoutSingleFile  LayoutMode = "single_file"
	LayoutMultiFile   LayoutMode = "multi_file"
	LayoutPartitioned LayoutMode = "partitioned"
)

// RoutingConfig 保存文件路由所需的稳定参数；不包含协议、压缩或格式 writer 实现。
type RoutingConfig struct {
	// Mode 选择 SingleFile / MultiFile / Partitioned 布局。
	Mode LayoutMode
	// Prefix 是 MultiFile/Partitioned 相对于数据集根目录的可选前缀。
	Prefix string
	// SingleFilePath 是 SingleFile 模式唯一允许的相对文件路径。
	SingleFilePath string
	// Extension 是 MultiFile/Partitioned 生成 part 文件时使用的不带点扩展名。
	Extension string
	// PartitionColumns 是 Partitioned 模式参与目录路由的查询输出列下标。
	PartitionColumns []int
}

// FileRoute 描述一个 DataChunk 中应写入同一逻辑文件的行集合。
type FileRoute struct {
	// Path 是相对于数据集根目录的稳定目标路径。
	Path string
	// Rows 为 nil 表示整个 chunk；非 nil 时保存要写入的行号，最多一个 DataChunk 大小。
	Rows []uint32
}

// FileRouter 只负责逻辑文件身份和行分组，不持有 writer、VFS handle 或普通堆上的大块输出缓冲。
type FileRouter struct {
	// config 是构造时归一化后的不可变路由参数。
	config RoutingConfig
	// schema 是绑定后查询输出 schema 的副本，用于分区键类型校验和编码。
	schema []Column
}

// NewFileRouter 创建路由器，并在打开任何远端对象前拒绝非法布局或分区列。
func NewFileRouter(config RoutingConfig, schema []Column) (*FileRouter, error) {
	if len(schema) == 0 {
		return nil, fmt.Errorf("%w: empty schema", ErrInvalidRouting)
	}
	var err error
	config.Prefix, err = normalizeRelativePath(config.Prefix, true)
	if err != nil {
		return nil, err
	}
	config.SingleFilePath, err = normalizeRelativePath(config.SingleFilePath, true)
	if err != nil {
		return nil, err
	}
	config.Extension = strings.TrimPrefix(strings.TrimSpace(config.Extension), ".")
	if strings.ContainsAny(config.Extension, `/\\`) || config.Extension == "." || config.Extension == ".." {
		return nil, fmt.Errorf("%w: invalid extension %q", ErrInvalidRouting, config.Extension)
	}
	switch config.Mode {
	case LayoutSingleFile:
		if config.SingleFilePath == "" || config.SingleFilePath == "." {
			return nil, fmt.Errorf("%w: single-file path is required", ErrInvalidRouting)
		}
		if len(config.PartitionColumns) != 0 {
			return nil, fmt.Errorf("%w: single-file layout cannot use partition columns", ErrInvalidRouting)
		}
	case LayoutMultiFile:
		if config.Extension == "" {
			return nil, fmt.Errorf("%w: multi-file extension is required", ErrInvalidRouting)
		}
		if len(config.PartitionColumns) != 0 {
			return nil, fmt.Errorf("%w: multi-file layout cannot use partition columns", ErrInvalidRouting)
		}
	case LayoutPartitioned:
		if config.Extension == "" || len(config.PartitionColumns) == 0 {
			return nil, fmt.Errorf("%w: partitioned layout requires extension and partition columns", ErrInvalidRouting)
		}
		seen := make(map[int]struct{}, len(config.PartitionColumns))
		for _, column := range config.PartitionColumns {
			if column < 0 || column >= len(schema) {
				return nil, fmt.Errorf("%w: partition column %d out of range", ErrInvalidRouting, column)
			}
			if _, ok := seen[column]; ok {
				return nil, fmt.Errorf("%w: duplicate partition column %d", ErrInvalidRouting, column)
			}
			seen[column] = struct{}{}
			switch schema[column].Type.ID {
			case types.List, types.Struct:
				return nil, fmt.Errorf("%w: nested partition column %q is unsupported", ErrInvalidRouting, schema[column].Name)
			}
		}
	default:
		return nil, fmt.Errorf("%w: unknown layout %q", ErrInvalidRouting, config.Mode)
	}
	copied := append([]Column(nil), schema...)
	config.PartitionColumns = append([]int(nil), config.PartitionColumns...)
	return &FileRouter{config: config, schema: copied}, nil
}

// RouteChunk 为一个有稳定 segmentID 的输出片段生成文件路由。
// MultiFile/Partitioned 强制调用者提供稳定 segmentID，避免把并行 chunk 到达顺序偷偷变成 resume 文件身份。
func (r *FileRouter) RouteChunk(chunk *vector.DataChunk, segmentID string) ([]FileRoute, error) {
	if r == nil || chunk == nil {
		return nil, fmt.Errorf("%w: nil router or chunk", ErrInvalidRouting)
	}
	if len(chunk.Columns) != len(r.schema) {
		return nil, fmt.Errorf("%w: chunk/schema column mismatch", ErrInvalidRouting)
	}
	for i := range chunk.Columns {
		if chunk.Columns[i] == nil || chunk.Columns[i].Count() != chunk.Cardinality {
			return nil, fmt.Errorf("%w: invalid chunk column %d", ErrInvalidRouting, i)
		}
	}
	if r.config.Mode == LayoutSingleFile {
		return []FileRoute{{Path: r.config.SingleFilePath}}, nil
	}
	if !validSegmentID(segmentID) {
		return nil, fmt.Errorf("%w: invalid stable segment id %q", ErrInvalidRouting, segmentID)
	}
	fileName := "part-" + segmentID + "." + r.config.Extension
	if r.config.Mode == LayoutMultiFile {
		return []FileRoute{{Path: joinRelative(r.config.Prefix, fileName)}}, nil
	}
	groups := make(map[string][]uint32)
	for row := 0; row < chunk.Cardinality; row++ {
		partitionPath, err := r.partitionPath(chunk, row)
		if err != nil {
			return nil, err
		}
		groups[partitionPath] = append(groups[partitionPath], uint32(row))
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	routes := make([]FileRoute, 0, len(keys))
	for _, key := range keys {
		rows := append([]uint32(nil), groups[key]...)
		routes = append(routes, FileRoute{Path: joinRelative(r.config.Prefix, key, fileName), Rows: rows})
	}
	return routes, nil
}

// partitionPath 把一行的分区键编码成稳定的 hive-style 相对目录。
func (r *FileRouter) partitionPath(chunk *vector.DataChunk, row int) (string, error) {
	parts := make([]string, 0, len(r.config.PartitionColumns))
	for _, column := range r.config.PartitionColumns {
		value, err := partitionValue(chunk.Columns[column], r.schema[column].Type, row)
		if err != nil {
			return "", fmt.Errorf("%w: partition column %q: %v", ErrInvalidRouting, r.schema[column].Name, err)
		}
		name := url.PathEscape(r.schema[column].Name)
		parts = append(parts, name+"="+url.PathEscape(value))
	}
	return path.Join(parts...), nil
}

// partitionValue 使用核心 Vector 的标量语义生成稳定目录值；NULL 使用固定 sentinel，LIST/STRUCT 在构造时已拒绝。
func partitionValue(vec *vector.Vector, typ types.LogicalType, row int) (string, error) {
	if !vec.IsValid(row) {
		return "__HIVE_DEFAULT_PARTITION__", nil
	}
	var tmp [96]byte
	switch typ.ID {
	case types.Boolean:
		v, _ := vec.Bool(row)
		return strconv.FormatBool(v), nil
	case types.TinyInt:
		v, _ := vec.Int8(row)
		return strconv.FormatInt(int64(v), 10), nil
	case types.SmallInt:
		v, _ := vec.Int16(row)
		return strconv.FormatInt(int64(v), 10), nil
	case types.Integer:
		v, _ := vec.Int32(row)
		return strconv.FormatInt(int64(v), 10), nil
	case types.BigInt:
		v, _ := vec.Int64(row)
		return strconv.FormatInt(v, 10), nil
	case types.HugeInt:
		v, _ := vec.Int128(row)
		return string(appendInt128(tmp[:0], v)), nil
	case types.Float:
		v, _ := vec.Float32(row)
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return "", errors.New("non-finite FLOAT partition key")
		}
		return strconv.FormatFloat(float64(v), 'g', -1, 32), nil
	case types.Double:
		v, _ := vec.Float64(row)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return "", errors.New("non-finite DOUBLE partition key")
		}
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	case types.Decimal:
		if typ.Decimal.Width <= 18 {
			v, _ := vec.Int64(row)
			return string(appendScaledInt64(tmp[:0], v, int(typ.Decimal.Scale))), nil
		}
		v, _ := vec.Int128(row)
		return string(appendScaledInt128(tmp[:0], v, int(typ.Decimal.Scale))), nil
	case types.Varchar:
		v, _ := vec.String(row)
		return v, nil
	case types.DateType:
		v, _ := vec.Int32(row)
		return time.Unix(int64(v)*86400, 0).UTC().Format("2006-01-02"), nil
	case types.TimestampType:
		v, _ := vec.Int64(row)
		return time.UnixMicro(v).UTC().Format("2006-01-02 15:04:05.999999"), nil
	default:
		return "", fmt.Errorf("unsupported partition type %d", typ.ID)
	}
}

// validSegmentID 只接受文件名安全的内部稳定标识，禁止路径穿越和隐式目录创建。
func validSegmentID(value string) bool {
	if value == "" || value == "." || value == ".." {
		return false
	}
	for i := 0; i < len(value); i++ {
		b := value[i]
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '-' || b == '_' {
			continue
		}
		return false
	}
	return true
}

// normalizeRelativePath 规范化数据集内相对路径，并拒绝绝对路径、反斜杠和父目录穿越。
func normalizeRelativePath(value string, allowEmpty bool) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		if allowEmpty {
			return "", nil
		}
		return "", fmt.Errorf("%w: empty relative path", ErrInvalidRouting)
	}
	if strings.HasPrefix(value, "/") || strings.Contains(value, `\`) {
		return "", fmt.Errorf("%w: unsafe relative path %q", ErrInvalidRouting, value)
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("%w: unsafe relative path %q", ErrInvalidRouting, value)
	}
	return cleaned, nil
}

func joinRelative(parts ...string) string {
	nonempty := parts[:0]
	for _, part := range parts {
		if part != "" {
			nonempty = append(nonempty, part)
		}
	}
	if len(nonempty) == 0 {
		return ""
	}
	return path.Join(nonempty...)
}
