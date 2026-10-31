package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type compactWriter struct {
	b    []byte
	last int16
}

func putVarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}
func zig(v int64) uint64 { return uint64(uint64(v<<1) ^ uint64(v>>63)) }
func (w *compactWriter) field(id int16, typ byte) {
	d := id - w.last
	if d > 0 && d <= 15 {
		w.b = append(w.b, byte(d<<4)|typ)
	} else {
		w.b = append(w.b, typ)
		w.b = putVarint(w.b, zig(int64(id)))
	}
	w.last = id
}
func (w *compactWriter) i32Field(id int16, v int32) {
	w.field(id, ctI32)
	w.b = putVarint(w.b, zig(int64(v)))
}
func (w *compactWriter) i64Field(id int16, v int64) {
	w.field(id, ctI64)
	w.b = putVarint(w.b, zig(v))
}
func (w *compactWriter) strField(id int16, s string) {
	w.field(id, ctBinary)
	w.b = putVarint(w.b, uint64(len(s)))
	w.b = append(w.b, s...)
}
func (w *compactWriter) stop() { w.b = append(w.b, ctStop) }
func (w *compactWriter) listHeader(id int16, typ byte, n int) {
	w.field(id, ctList)
	if n < 15 {
		w.b = append(w.b, byte(n<<4)|typ)
	} else {
		w.b = append(w.b, 0xf0|typ)
		w.b = putVarint(w.b, uint64(n))
	}
}
func appendStruct(dst []byte, fn func(*compactWriter)) []byte {
	x := &compactWriter{b: dst}
	fn(x)
	x.stop()
	return x.b
}

func minimalMetadata() []byte {
	w := &compactWriter{}
	w.i32Field(1, 1)
	w.listHeader(2, ctStruct, 2)
	w.b = appendStruct(w.b, func(s *compactWriter) { s.strField(4, "schema"); s.i32Field(5, 1) })
	w.b = appendStruct(w.b, func(s *compactWriter) { s.i32Field(1, int32(TypeInt64)); s.i32Field(3, int32(Required)); s.strField(4, "id") })
	w.i64Field(3, 2)
	w.listHeader(4, ctStruct, 0)
	w.strField(6, "phase1-test")
	w.stop()
	return w.b
}

func TestDecodeFileMetaData(t *testing.T) {
	m, err := DecodeFileMetaData(minimalMetadata())
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != 1 || m.NumRows != 2 || len(m.Schema) != 2 || m.Schema[1].Name != "id" || m.Schema[1].Type == nil || *m.Schema[1].Type != TypeInt64 {
		t.Fatalf("metadata=%+v", m)
	}
}

func TestBuildSchemaTreeListLevels(t *testing.T) {
	list := ConvertedList
	opt := Optional
	rep := Repeated
	rootChildren := int32(1)
	outerChildren := int32(1)
	repeatedChildren := int32(1)
	int64Type := TypeInt64
	flat := []SchemaElement{
		{Name: "schema", NumChildren: &rootChildren},
		{Name: "items", Repetition: &opt, NumChildren: &outerChildren, Converted: &list},
		{Name: "list", Repetition: &rep, NumChildren: &repeatedChildren},
		{Name: "element", Repetition: &opt, Type: &int64Type},
	}
	tree, err := BuildSchemaTree(flat)
	if err != nil {
		t.Fatal(err)
	}
	leaf := tree.Find("items", "list", "element")
	if leaf == nil {
		t.Fatal("leaf not found")
	}
	if leaf.MaxDef != 3 || leaf.MaxRep != 1 {
		t.Fatalf("levels def=%d rep=%d", leaf.MaxDef, leaf.MaxRep)
	}
	items := tree.Root.Children[0]
	if items.Logical.ID != coretypes.List || items.Logical.Child == nil || items.Logical.Child.ID != coretypes.BigInt {
		t.Fatalf("items logical=%+v", items.Logical)
	}
}

func TestBuildSchemaTreeDecimalAndTimestamp(t *testing.T) {
	req := Required
	rootChildren := int32(2)
	i64 := TypeInt64
	fixed := TypeFixedLenByteArray
	dec := ConvertedDecimal
	ts := ConvertedTimestampMicros
	p := int32(20)
	s := int32(4)
	l := int32(16)
	flat := []SchemaElement{{Name: "schema", NumChildren: &rootChildren}, {Name: "amount", Repetition: &req, Type: &fixed, TypeLength: &l, Converted: &dec, Precision: &p, Scale: &s}, {Name: "ts", Repetition: &req, Type: &i64, Converted: &ts}}
	tree, err := BuildSchemaTree(flat)
	if err != nil {
		t.Fatal(err)
	}
	if tree.Root.Children[0].Logical.ID != coretypes.Decimal || tree.Root.Children[0].Logical.Decimal.Width != 20 || tree.Root.Children[1].Logical.ID != coretypes.TimestampType {
		t.Fatalf("logical=%+v %+v", tree.Root.Children[0].Logical, tree.Root.Children[1].Logical)
	}
}

type recordingDriver struct {
	data     []byte
	ranges   []vfs.RangeRequest
	mutateAt int
}

func (d *recordingDriver) Scheme() string { return "mem" }
func (d *recordingDriver) StaticCapabilities() vfs.CapabilitySet {
	return vfs.Capabilities(vfs.RandomRead, vfs.ConditionalRead, vfs.StableETag)
}
func (d *recordingDriver) Probe(context.Context) (vfs.ProbeResult, error) {
	return vfs.ProbeResult{Supported: d.StaticCapabilities(), Endpoint: "mem://x", CredentialScope: "x", SecurityProfile: "test"}, nil
}
func (d *recordingDriver) Stat(context.Context, vfs.URI) (vfs.ObjectInfo, error) {
	return vfs.ObjectInfo{}, errors.New("unused")
}
func (d *recordingDriver) List(context.Context, vfs.URI, vfs.ListOptions) (vfs.ListPage, error) {
	return vfs.ListPage{}, errors.New("unused")
}
func (d *recordingDriver) ReadRange(_ context.Context, r vfs.RangeRequest, scope *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	d.ranges = append(d.ranges, r)
	if d.mutateAt > 0 && len(d.ranges) == d.mutateAt {
		return vfs.RangeResult{}, vfs.ErrObjectChanged
	}
	if r.Offset < 0 || r.Offset+r.Length > int64(len(d.data)) {
		return vfs.RangeResult{}, fmt.Errorf("bad range")
	}
	b, err := bm.Allocate(scope, r.Tag, int(r.Length))
	if err != nil {
		return vfs.RangeResult{}, err
	}
	copy(b.Bytes(), d.data[r.Offset:r.Offset+r.Length])
	return vfs.RangeResult{Buffer: b, Identity: vfs.ObjectIdentity{ETag: "e", Size: int64(len(d.data))}}, nil
}

func parquetFile(meta []byte) []byte {
	out := append([]byte("PAR1"), meta...)
	var tail [8]byte
	binary.LittleEndian.PutUint32(tail[:4], uint32(len(meta)))
	copy(tail[4:], "PAR1")
	return append(out, tail[:]...)
}

func TestDiscoverFooterUsesMandatoryTwoExactRanges(t *testing.T) {
	data := parquetFile(minimalMetadata())
	d := &recordingDriver{data: data}
	u, _ := vfs.ParseURI("mem://x/file.parquet")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	f, err := DiscoverFooter(context.Background(), d, FooterSource{URI: u, Size: int64(len(data)), Identity: vfs.ObjectIdentity{ETag: "e", Size: int64(len(data))}}, scope, bm, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.ranges) != 2 {
		t.Fatalf("ranges=%v", d.ranges)
	}
	if d.ranges[0].Offset != int64(len(data)-8) || d.ranges[0].Length != 8 {
		t.Fatalf("trailer=%+v", d.ranges[0])
	}
	if d.ranges[1].Offset != 4 || d.ranges[1].Length != int64(len(minimalMetadata())) {
		t.Fatalf("metadata=%+v", d.ranges[1])
	}
	if f.Metadata.NumRows != 2 || f.Schema.Find("id") == nil {
		t.Fatalf("footer=%+v", f)
	}
	if f.Metrics.Requests != 2 || f.Metrics.PlannedBytes != int64(8+len(minimalMetadata())) {
		t.Fatalf("metrics=%+v", f.Metrics)
	}
	if scope.Used() != 0 {
		t.Fatalf("leaked=%d", scope.Used())
	}
}

func TestDiscoverFooterInvalidMagicReleases(t *testing.T) {
	data := parquetFile(minimalMetadata())
	copy(data[len(data)-4:], "NOPE")
	d := &recordingDriver{data: data}
	u, _ := vfs.ParseURI("mem://x/f")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	_, err := DiscoverFooter(context.Background(), d, FooterSource{URI: u, Size: int64(len(data)), Identity: vfs.ObjectIdentity{ETag: "e", Size: int64(len(data))}}, scope, bm, 0)
	if !errors.Is(err, ErrInvalidMagic) {
		t.Fatalf("got %v", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("leaked=%d", scope.Used())
	}
}

func TestDiscoverFooterObjectChangedSecondRead(t *testing.T) {
	data := parquetFile(minimalMetadata())
	d := &recordingDriver{data: data, mutateAt: 2}
	u, _ := vfs.ParseURI("mem://x/f")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	_, err := DiscoverFooter(context.Background(), d, FooterSource{URI: u, Size: int64(len(data)), Identity: vfs.ObjectIdentity{ETag: "e", Size: int64(len(data))}}, scope, bm, 0)
	if !errors.Is(err, vfs.ErrObjectChanged) {
		t.Fatalf("got %v", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("leaked=%d", scope.Used())
	}
}
