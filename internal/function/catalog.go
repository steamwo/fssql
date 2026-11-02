package function

import (
	"fmt"
	"strings"
	"sync"

	"github.com/steamwo/fssql/internal/core/types"
)

// Kind 定义函数在 SQL 绑定阶段的类别。
type Kind uint8

const (
	Scalar Kind = iota
	Aggregate
	Table
)

// ID 是内建函数的稳定内部标识；它不构成外部 ABI。
type ID uint16

const (
	InvalidID ID = iota
	CountID
	SumID
	AvgID
	MinID
	MaxID
	ReadParquetID
	ReadCSVID
	ReadJSONID
	ReadNDJSONID
	GlobID
)

// ArgumentType 表示 Binder 传给函数重载解析器的参数类型。
// SQLNull 只存在于 binding 阶段，不进入 core LogicalType/Vector ABI。
type ArgumentType struct {
	Type    types.LogicalType // Type 是普通参数的逻辑类型。
	SQLNull bool              // SQLNull 表示未获得上下文类型的 NULL literal。
}

// BindInput 是函数重载解析所需的纯类型信息。
type BindInput struct {
	Arguments []ArgumentType // Arguments 按 SQL 参数顺序排列。
	Star      bool           // Star 表示 COUNT(*) 一类星号调用。
	Distinct  bool           // Distinct 表示 DISTINCT aggregate 参数语法。
}

// BindResult 是函数绑定后的稳定类型结果。
type BindResult struct {
	ReturnType types.LogicalType   // ReturnType 是函数输出类型。
	Arguments  []types.LogicalType // Arguments 是消除 SQLNULL/隐式转换后的参数类型。
}

// BindFunc 负责纯类型重载解析；不得执行 I/O 或创建 runtime operator。
type BindFunc func(BindInput) (BindResult, error)

// Definition 描述 FunctionCatalog 中的一个函数名/类别。
type Definition struct {
	ID      ID       // ID 是内部稳定标识。
	Name    string   // Name 使用不带 schema 的规范化小写名称。
	Kind    Kind     // Kind 区分 Scalar/Aggregate/Table。
	MinArgs int      // MinArgs 是非 Star 调用的最少参数数。
	MaxArgs int      // MaxArgs 是最大参数数；-1 表示 variadic。
	Bind    BindFunc // Bind 为空表示该阶段只登记名称，具体 Bind 由后续 adapter 提供。
}

// Catalog 是线程安全的函数元数据目录；查询发生在控制面绑定阶段。
type Catalog struct {
	mu     sync.RWMutex          // mu 保护 definitions。
	byName map[string]Definition // byName 使用小写函数名索引定义。
}

// NewCatalog 创建空 FunctionCatalog。
func NewCatalog() *Catalog { return &Catalog{byName: make(map[string]Definition)} }

// NewBuiltinCatalog 创建 v1 Stable 内建函数目录。
func NewBuiltinCatalog() *Catalog {
	c := NewCatalog()
	mustRegister := func(d Definition) {
		if err := c.Register(d); err != nil {
			panic(err)
		}
	}
	mustRegister(Definition{ID: CountID, Name: "count", Kind: Aggregate, MinArgs: 1, MaxArgs: 1, Bind: bindCount})
	mustRegister(Definition{ID: SumID, Name: "sum", Kind: Aggregate, MinArgs: 1, MaxArgs: 1, Bind: bindSum})
	mustRegister(Definition{ID: AvgID, Name: "avg", Kind: Aggregate, MinArgs: 1, MaxArgs: 1, Bind: bindAvg})
	mustRegister(Definition{ID: MinID, Name: "min", Kind: Aggregate, MinArgs: 1, MaxArgs: 1, Bind: bindMinMax})
	mustRegister(Definition{ID: MaxID, Name: "max", Kind: Aggregate, MinArgs: 1, MaxArgs: 1, Bind: bindMinMax})
	// TableFunction 的真实 Bind 必须复用各格式现有 Bind/Schema Resolution；此处只登记稳定名称和 arity。
	mustRegister(Definition{ID: ReadParquetID, Name: "read_parquet", Kind: Table, MinArgs: 1, MaxArgs: 1})
	mustRegister(Definition{ID: ReadCSVID, Name: "read_csv", Kind: Table, MinArgs: 1, MaxArgs: 1})
	mustRegister(Definition{ID: ReadJSONID, Name: "read_json", Kind: Table, MinArgs: 1, MaxArgs: 1})
	mustRegister(Definition{ID: ReadNDJSONID, Name: "read_ndjson", Kind: Table, MinArgs: 1, MaxArgs: 1})
	mustRegister(Definition{ID: GlobID, Name: "glob", Kind: Table, MinArgs: 1, MaxArgs: 1})
	return c
}

// Register 注册函数定义并拒绝名称重复或无效 arity。
func (c *Catalog) Register(d Definition) error {
	if c == nil {
		return fmt.Errorf("function catalog is nil")
	}
	name := strings.ToLower(strings.TrimSpace(d.Name))
	if name == "" || d.ID == InvalidID {
		return fmt.Errorf("invalid function definition")
	}
	if d.MinArgs < 0 || (d.MaxArgs >= 0 && d.MaxArgs < d.MinArgs) {
		return fmt.Errorf("invalid arity for %s", name)
	}
	d.Name = name
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.byName[name]; exists {
		return fmt.Errorf("function %s already registered", name)
	}
	c.byName[name] = d
	return nil
}

// Lookup 按 SQL 不区分大小写规则查找函数定义。
func (c *Catalog) Lookup(name string) (Definition, bool) {
	if c == nil {
		return Definition{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	d, ok := c.byName[strings.ToLower(name)]
	return d, ok
}

// BindCall 验证函数类别/arity 后执行纯类型绑定。
func (c *Catalog) BindCall(name string, kind Kind, in BindInput) (Definition, BindResult, error) {
	d, ok := c.Lookup(name)
	if !ok {
		return Definition{}, BindResult{}, fmt.Errorf("unknown function %s", name)
	}
	if d.Kind != kind {
		return Definition{}, BindResult{}, fmt.Errorf("function %s has kind %d, want %d", d.Name, d.Kind, kind)
	}
	// v1 Stable 只冻结 SELECT DISTINCT，没有冻结 aggregate-level DISTINCT。
	// 在 FunctionCatalog 绑定阶段显式拒绝，避免 Binder 丢失标志后被静默当普通 aggregate 执行。
	if in.Distinct {
		return Definition{}, BindResult{}, fmt.Errorf("aggregate DISTINCT is not part of Stable v1")
	}
	argc := len(in.Arguments)
	if in.Star {
		if d.ID != CountID || argc != 0 {
			return Definition{}, BindResult{}, fmt.Errorf("star argument is only valid for count(*)")
		}
	} else if argc < d.MinArgs || (d.MaxArgs >= 0 && argc > d.MaxArgs) {
		return Definition{}, BindResult{}, fmt.Errorf("function %s expects %s, got %d", d.Name, arityText(d), argc)
	}
	if d.Bind == nil {
		return d, BindResult{}, nil
	}
	r, err := d.Bind(in)
	return d, r, err
}

// arityText 将定义的 arity 转为诊断字符串。
func arityText(d Definition) string {
	if d.MaxArgs == d.MinArgs {
		return fmt.Sprintf("%d arguments", d.MinArgs)
	}
	if d.MaxArgs < 0 {
		return fmt.Sprintf("at least %d arguments", d.MinArgs)
	}
	return fmt.Sprintf("%d..%d arguments", d.MinArgs, d.MaxArgs)
}

// bindCount 绑定 COUNT/COUNT(*) 的固定 BIGINT 返回类型。
func bindCount(in BindInput) (BindResult, error) {
	if in.Star {
		return BindResult{ReturnType: types.LogicalType{ID: types.BigInt}}, nil
	}
	if len(in.Arguments) != 1 {
		return BindResult{}, fmt.Errorf("count expects one argument")
	}
	args := make([]types.LogicalType, 1)
	if !in.Arguments[0].SQLNull {
		args[0] = in.Arguments[0].Type
	}
	return BindResult{ReturnType: types.LogicalType{ID: types.BigInt}, Arguments: args}, nil
}

// bindSum 绑定 SUM，并与执行层 aggregate.Bind 的 DuckDB-compatible widening 对齐。
func bindSum(in BindInput) (BindResult, error) {
	arg, err := oneTypedArgument("sum", in)
	if err != nil {
		return BindResult{}, err
	}
	var out types.LogicalType
	switch arg.ID {
	case types.TinyInt, types.SmallInt, types.Integer, types.BigInt, types.HugeInt:
		out = types.LogicalType{ID: types.HugeInt}
	case types.Float, types.Double:
		out = types.LogicalType{ID: types.Double}
	case types.Decimal:
		out, err = types.NewDecimal(38, arg.Decimal.Scale)
		if err != nil {
			return BindResult{}, err
		}
	default:
		return BindResult{}, fmt.Errorf("sum unsupported type %d", arg.ID)
	}
	return BindResult{ReturnType: out, Arguments: []types.LogicalType{arg}}, nil
}

// bindAvg 绑定 AVG；所有已支持 numeric 输入返回 DOUBLE。
func bindAvg(in BindInput) (BindResult, error) {
	arg, err := oneTypedArgument("avg", in)
	if err != nil {
		return BindResult{}, err
	}
	if !arg.IsNumeric() {
		return BindResult{}, fmt.Errorf("avg unsupported type %d", arg.ID)
	}
	return BindResult{ReturnType: types.LogicalType{ID: types.Double}, Arguments: []types.LogicalType{arg}}, nil
}

// bindMinMax 绑定 MIN/MAX，并保留可比较输入的 logical identity。
func bindMinMax(in BindInput) (BindResult, error) {
	arg, err := oneTypedArgument("min/max", in)
	if err != nil {
		return BindResult{}, err
	}
	switch arg.ID {
	case types.Boolean, types.TinyInt, types.SmallInt, types.Integer, types.BigInt, types.HugeInt, types.Float, types.Double, types.Decimal, types.Varchar, types.DateType, types.TimestampType:
		return BindResult{ReturnType: arg, Arguments: []types.LogicalType{arg}}, nil
	default:
		return BindResult{}, fmt.Errorf("min/max unsupported type %d", arg.ID)
	}
}

// oneTypedArgument 要求单参数已经获得真实 LogicalType；SQLNULL 必须先由 Binder 上下文消解。
func oneTypedArgument(name string, in BindInput) (types.LogicalType, error) {
	if in.Star || len(in.Arguments) != 1 {
		return types.LogicalType{}, fmt.Errorf("%s expects one argument", name)
	}
	if in.Arguments[0].SQLNull {
		return types.LogicalType{}, fmt.Errorf("%s cannot infer type from SQLNULL without context", name)
	}
	return in.Arguments[0].Type, nil
}