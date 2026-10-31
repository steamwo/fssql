package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"

	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/format/scan"
)

type ProjectionPlan struct {
	ColumnIndexes []int
	LeafPaths     []string
}

func PlanProjection(tree *SchemaTree, rg RowGroup, requested []string) (ProjectionPlan, error) {
	if tree == nil {
		return ProjectionPlan{}, fmt.Errorf("parquet: nil schema")
	}
	want := make(map[string]struct{}, len(requested))
	for _, name := range requested {
		name = strings.TrimSpace(name)
		if name != "" {
			want[name] = struct{}{}
		}
	}
	plan := ProjectionPlan{}
	for i, c := range rg.Columns {
		if c.MetaData == nil {
			continue
		}
		path := strings.Join(c.MetaData.PathInSchema, ".")
		selectCol := len(want) == 0
		if !selectCol {
			for w := range want {
				if path == w || strings.HasPrefix(path, w+".") || strings.HasPrefix(w, path+".") {
					selectCol = true
					break
				}
			}
		}
		if selectCol {
			plan.ColumnIndexes = append(plan.ColumnIndexes, i)
			plan.LeafPaths = append(plan.LeafPaths, path)
		}
	}
	if len(want) > 0 {
		for w := range want {
			found := false
			for _, leaf := range tree.Leaves {
				p := strings.Join(leaf.Path, ".")
				if p == w || strings.HasPrefix(p, w+".") || strings.HasPrefix(w, p+".") {
					found = true
					break
				}
			}
			if !found {
				return ProjectionPlan{}, fmt.Errorf("parquet: projected column %q not found", w)
			}
		}
	}
	return plan, nil
}

type PruneResult struct {
	Truth       scan.Truth
	Evaluated   int
	Unsupported int
}

func EvaluateRowGroup(tree *SchemaTree, rg RowGroup, filters []scan.Filter) PruneResult {
	res := PruneResult{Truth: scan.AlwaysTrue}
	for _, f := range filters {
		t := evaluateFilter(tree, rg, f)
		if t == scan.MaybeTrue {
			res.Unsupported++
		} else {
			res.Evaluated++
		}
		res.Truth = scan.And(res.Truth, t)
		if res.Truth == scan.AlwaysFalse {
			return res
		}
	}
	return res
}

func evaluateFilter(tree *SchemaTree, rg RowGroup, f scan.Filter) scan.Truth {
	leaf := findLeafByName(tree, f.Column)
	if leaf == nil {
		return scan.MaybeTrue
	}
	var meta *ColumnMetaData
	for i := range rg.Columns {
		c := rg.Columns[i].MetaData
		if c != nil && strings.Join(c.PathInSchema, ".") == strings.Join(leaf.Path, ".") {
			meta = c
			break
		}
	}
	if meta == nil || meta.Statistics == nil {
		return scan.MaybeTrue
	}
	st := meta.Statistics
	nullsKnown := st.NullCount != nil
	nulls := int64(0)
	if nullsKnown {
		nulls = *st.NullCount
	}
	switch f.Op {
	case scan.IsNull:
		if !nullsKnown {
			return scan.MaybeTrue
		}
		if nulls == 0 {
			return scan.AlwaysFalse
		}
		if nulls == rg.NumRows {
			return scan.AlwaysTrue
		}
		return scan.MaybeTrue
	case scan.IsNotNull:
		if !nullsKnown {
			return scan.MaybeTrue
		}
		if nulls == rg.NumRows {
			return scan.AlwaysFalse
		}
		if nulls == 0 {
			return scan.AlwaysTrue
		}
		return scan.MaybeTrue
	}
	minb, maxb := statsBounds(st)
	if minb == nil || maxb == nil || f.Value.Null {
		return scan.MaybeTrue
	}
	minv, ok1 := decodeStatValue(leaf, minb)
	maxv, ok2 := decodeStatValue(leaf, maxb)
	if !ok1 || !ok2 {
		return scan.MaybeTrue
	}
	cmpMin, ok := compareLiteral(minv, f.Value)
	if !ok {
		return scan.MaybeTrue
	}
	cmpMax, ok := compareLiteral(maxv, f.Value)
	if !ok {
		return scan.MaybeTrue
	}
	noNulls := nullsKnown && nulls == 0
	switch f.Op {
	case scan.Equal:
		if cmpMin > 0 || cmpMax < 0 {
			return scan.AlwaysFalse
		}
		if noNulls && cmpMin == 0 && cmpMax == 0 {
			return scan.AlwaysTrue
		}
	case scan.NotEqual:
		if noNulls && cmpMin == 0 && cmpMax == 0 {
			return scan.AlwaysFalse
		}
		if noNulls && (cmpMax < 0 || cmpMin > 0) {
			return scan.AlwaysTrue
		}
	case scan.LessThan:
		if cmpMin >= 0 {
			return scan.AlwaysFalse
		}
		if noNulls && cmpMax < 0 {
			return scan.AlwaysTrue
		}
	case scan.LessEqual:
		if cmpMin > 0 {
			return scan.AlwaysFalse
		}
		if noNulls && cmpMax <= 0 {
			return scan.AlwaysTrue
		}
	case scan.GreaterThan:
		if cmpMax <= 0 {
			return scan.AlwaysFalse
		}
		if noNulls && cmpMin > 0 {
			return scan.AlwaysTrue
		}
	case scan.GreaterEqual:
		if cmpMax < 0 {
			return scan.AlwaysFalse
		}
		if noNulls && cmpMin >= 0 {
			return scan.AlwaysTrue
		}
	default:
		return scan.MaybeTrue
	}
	return scan.MaybeTrue
}

func findLeafByName(tree *SchemaTree, name string) *SchemaNode {
	if tree == nil {
		return nil
	}
	for _, l := range tree.Leaves {
		p := strings.Join(l.Path, ".")
		if p == name || l.Name == name {
			return l
		}
	}
	return nil
}
func statsBounds(s *Statistics) ([]byte, []byte) {
	minb, maxb := s.MinValue, s.MaxValue
	if len(minb) == 0 {
		minb = s.Min
	}
	if len(maxb) == 0 {
		maxb = s.Max
	}
	return minb, maxb
}

type statValue struct {
	kind coretypes.LogicalTypeID
	i    int64
	f    float64
	b    bool
	s    string
}

func decodeStatValue(leaf *SchemaNode, b []byte) (statValue, bool) {
	if leaf == nil || leaf.Element.Type == nil {
		return statValue{}, false
	}
	kind := leaf.Logical.ID
	switch *leaf.Element.Type {
	case TypeBoolean:
		if len(b) < 1 {
			return statValue{}, false
		}
		return statValue{kind: kind, b: b[0] != 0}, true
	case TypeInt32:
		if len(b) != 4 {
			return statValue{}, false
		}
		return statValue{kind: kind, i: int64(int32(binary.LittleEndian.Uint32(b)))}, true
	case TypeInt64:
		if len(b) != 8 {
			return statValue{}, false
		}
		return statValue{kind: kind, i: int64(binary.LittleEndian.Uint64(b))}, true
	case TypeFloat:
		if len(b) != 4 {
			return statValue{}, false
		}
		return statValue{kind: kind, f: float64(math.Float32frombits(binary.LittleEndian.Uint32(b)))}, true
	case TypeDouble:
		if len(b) != 8 {
			return statValue{}, false
		}
		return statValue{kind: kind, f: math.Float64frombits(binary.LittleEndian.Uint64(b))}, true
	case TypeByteArray:
		if kind != coretypes.Varchar {
			return statValue{}, false
		}
		return statValue{kind: kind, s: string(b)}, true
	default:
		return statValue{}, false
	}
}
func compareLiteral(v statValue, l scan.Literal) (int, bool) {
	switch v.kind {
	case coretypes.Boolean:
		if l.Type.ID != coretypes.Boolean {
			return 0, false
		}
		if v.b == l.Bool {
			return 0, true
		}
		if !v.b {
			return -1, true
		}
		return 1, true
	case coretypes.Integer, coretypes.BigInt, coretypes.DateType, coretypes.TimestampType:
		if l.Type.ID != v.kind && !(v.kind == coretypes.Integer && l.Type.ID == coretypes.BigInt) {
			return 0, false
		}
		if v.i < l.Int64 {
			return -1, true
		}
		if v.i > l.Int64 {
			return 1, true
		}
		return 0, true
	case coretypes.Float, coretypes.Double:
		if l.Type.ID != v.kind && !(v.kind == coretypes.Float && l.Type.ID == coretypes.Double) {
			return 0, false
		}
		if math.IsNaN(v.f) || math.IsNaN(l.Float64) {
			return 0, false
		}
		if v.f < l.Float64 {
			return -1, true
		}
		if v.f > l.Float64 {
			return 1, true
		}
		return 0, true
	case coretypes.Varchar:
		if l.Type.ID != coretypes.Varchar {
			return 0, false
		}
		return bytes.Compare([]byte(v.s), []byte(l.String)), true
	default:
		return 0, false
	}
}

type ResolvedSchema struct {
	Fields      []coretypes.StructField
	SourceCount int
	UnionByName bool
}

func ResolveSchemas(trees []*SchemaTree, unionByName bool) (ResolvedSchema, error) {
	if len(trees) == 0 {
		return ResolvedSchema{}, fmt.Errorf("parquet: no schemas")
	}
	if !unionByName {
		base := trees[0].Root.Logical.Fields
		for i := 1; i < len(trees); i++ {
			if !sameFields(base, trees[i].Root.Logical.Fields) {
				return ResolvedSchema{}, fmt.Errorf("parquet: schema mismatch in file %d", i)
			}
		}
		return ResolvedSchema{Fields: append([]coretypes.StructField(nil), base...), SourceCount: len(trees)}, nil
	}
	byName := map[string]coretypes.LogicalType{}
	order := []string{}
	for i, t := range trees {
		if t == nil || t.Root == nil {
			return ResolvedSchema{}, fmt.Errorf("parquet: nil schema %d", i)
		}
		for _, f := range t.Root.Logical.Fields {
			if old, ok := byName[f.Name]; ok {
				if !sameType(old, f.Type) {
					return ResolvedSchema{}, fmt.Errorf("parquet: incompatible union_by_name column %s", f.Name)
				}
			} else {
				byName[f.Name] = f.Type
				order = append(order, f.Name)
			}
		}
	}
	sort.Strings(order)
	fields := make([]coretypes.StructField, 0, len(order))
	for _, n := range order {
		fields = append(fields, coretypes.StructField{Name: n, Type: byName[n]})
	}
	return ResolvedSchema{Fields: fields, SourceCount: len(trees), UnionByName: true}, nil
}
func sameFields(a, b []coretypes.StructField) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || !sameType(a[i].Type, b[i].Type) {
			return false
		}
	}
	return true
}
func sameType(a, b coretypes.LogicalType) bool {
	if a.ID != b.ID || a.Decimal != b.Decimal {
		return false
	}
	if (a.Child == nil) != (b.Child == nil) {
		return false
	}
	if a.Child != nil && !sameType(*a.Child, *b.Child) {
		return false
	}
	return sameFields(a.Fields, b.Fields)
}
