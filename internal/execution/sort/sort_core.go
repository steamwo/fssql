package sort

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
	"github.com/steamwo/fssql/internal/semantics"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

type Key struct {
	Column     int
	Desc       bool
	NullsFirst bool
}

type Config struct {
	InputTypes      []types.LogicalType
	Keys            []Key
	MemoryBudget    int64
	SpillBlockBytes int
	Scope           *memory.Scope
	Buffers         *memory.BufferManager
	Temp            *storagetemp.QueryScope
	Compression     storagetemp.Compression
}

type Stats struct {
	RunsCreated      int
	MergePasses      int
	SpillPages       int
	PeakBuilderBytes int64
}

type run struct {
	pages *storagetemp.PageList
	rows  uint64
}

type runWriter struct {
	store        *storagetemp.SpillablePageStore
	scope        *memory.Scope
	bm           *memory.BufferManager
	pages        *storagetemp.PageList
	buf          *memory.Buffer
	used, target int
	rows         uint64
}

func newRunWriter(store *storagetemp.SpillablePageStore, scope *memory.Scope, bm *memory.BufferManager, target int) (*runWriter, error) {
	if target < 512 { target = 512 }
	pl, err := storagetemp.NewPageList(scope, bm); if err != nil { return nil, err }
	b, err := bm.Allocate(scope, memory.TagSpillIO, target); if err != nil { pl.Release(); return nil, err }
	return &runWriter{store: store, scope: scope, bm: bm, pages: pl, buf: b, target: target}, nil
}
func (w *runWriter) flush(ctx context.Context) error {
	if w.used == 0 { return nil }
	id, err := w.store.WritePage(ctx, w.buf.Bytes()[:w.used]); if err != nil { return err }
	if err := w.pages.Append(id); err != nil { return err }
	w.used = 0
	if w.buf.Len() != w.target { if err := w.buf.Resize(w.target, nil); err != nil { return err } }
	return nil
}
func (w *runWriter) append(ctx context.Context, row []byte) error {
	if err := ctx.Err(); err != nil { return err }
	need := 4 + len(row)
	if w.used > 0 && w.used+need > w.buf.Len() { if err := w.flush(ctx); err != nil { return err } }
	if need > w.buf.Len() { if err := w.buf.Resize(need, nil); err != nil { return err } }
	binary.LittleEndian.PutUint32(w.buf.Bytes()[w.used:w.used+4], uint32(len(row))); copy(w.buf.Bytes()[w.used+4:w.used+need], row); w.used += need; w.rows++; return nil
}
func (w *runWriter) close(ctx context.Context) (*run, error) {
	if err := w.flush(ctx); err != nil { return nil, err }
	if w.buf != nil { w.buf.Release(); w.buf = nil }
	r := &run{pages: w.pages, rows: w.rows}; w.pages = nil; return r, nil
}
func (w *runWriter) release() { if w.buf != nil { w.buf.Release(); w.buf=nil }; if w.pages != nil { w.pages.Release(); w.pages=nil } }

type ExternalSort struct {
	cfg Config
	layout exechash.RowLayout
	builder *exechash.TupleDataCollection
	store *storagetemp.SpillablePageStore
	runs []*run
	runMeta *memory.Reservation
	finalized bool
	stats Stats
	preparedKeys []preparedSortKey
}

func New(cfg Config) (*ExternalSort, error) {
	if cfg.Scope == nil || cfg.Buffers == nil || cfg.Temp == nil { return nil, errors.New("external sort requires scope, buffers and temp") }
	if len(cfg.Keys) == 0 { return nil, errors.New("sort requires at least one key") }
	if cfg.MemoryBudget <= 0 { return nil, errors.New("sort memory budget must be positive") }
	if cfg.SpillBlockBytes <= 0 { cfg.SpillBlockBytes = 64 << 10 }
	for _, k := range cfg.Keys { if k.Column < 0 || k.Column >= len(cfg.InputTypes) { return nil, fmt.Errorf("sort key %d out of range", k.Column) } }
	layout, err := exechash.NewRelocatableRowLayout(cfg.InputTypes); if err != nil { return nil, err }
	store, err := storagetemp.NewSpillablePageStore(cfg.Temp, cfg.Scope, cfg.Buffers, storagetemp.PageStoreConfig{Compression: cfg.Compression, MinCompressBytes: 1024, SegmentMaxBytes: 64 << 20}); if err != nil { return nil, err }
	meta, err := cfg.Scope.Reserve(memory.TagMetadata, 0); if err != nil { store.Close(); return nil, err }
	s := &ExternalSort{cfg: cfg, layout: layout, store: store, runMeta: meta}
	for _, key := range cfg.Keys { s.preparedKeys = append(s.preparedKeys, preparedSortKey{key:key, column:layout.Columns[key.Column], fixedWidth:layout.FixedWidth}) }
	if err := s.resetBuilder(); err != nil { meta.Release(); store.Close(); return nil, err }
	return s, nil
}
func (s *ExternalSort) resetBuilder() error { b,err:=exechash.NewTupleDataCollectionForLayout(s.cfg.Scope,s.cfg.Buffers,s.layout,8);if err!=nil{return err};s.builder=b;return nil }
func (s *ExternalSort) builderBytes() int64 { if s.builder==nil{return 0};return int64(s.builder.AllocatedBytes()) }
func (s *ExternalSort) updatePeak(){if n:=s.builderBytes();n>s.stats.PeakBuilderBytes{s.stats.PeakBuilderBytes=n}}
func (s *ExternalSort) appendRun(r *run) error { if r==nil||r.rows==0{if r!=nil&&r.pages!=nil{r.pages.Release()};return nil};if err:=s.runMeta.Resize(int64((len(s.runs)+1)*64));err!=nil{return err};s.runs=append(s.runs,r);s.stats.RunsCreated++;s.stats.SpillPages+=r.pages.Count();return nil }
func (s *ExternalSort) Consume(ctx context.Context,c *vector.DataChunk) error { if s.finalized{return errors.New("sort finalized")};if c==nil{return errors.New("nil chunk")};if len(c.Columns)!=len(s.cfg.InputTypes){return errors.New("sort input column count mismatch")};for i:=0;i<c.Cardinality;i++{if err:=ctx.Err();err!=nil{return err};if err:=s.builder.AppendChunkRow(c,i);err!=nil{return err}};s.updatePeak();if s.builderBytes()>s.cfg.MemoryBudget{return s.spillBuilder(ctx)};return nil }

func compareRowsWith(layout exechash.RowLayout,keys []Key,a,b []byte)(int,error){for _,k:=range keys{ad,av,err:=layout.ColumnData(a,k.Column);if err!=nil{return 0,err};bd,bv,err:=layout.ColumnData(b,k.Column);if err!=nil{return 0,err};if !av||!bv{if !av&&!bv{continue};if !av{if k.NullsFirst{return -1,nil};return 1,nil};if k.NullsFirst{return 1,nil};return -1,nil};cmp,err:=compareEncoded(layout.Columns[k.Column].Type,ad,bd);if err!=nil{return 0,err};if cmp!=0{if k.Desc{cmp=-cmp};return cmp,nil}};return 0,nil}
func (s *ExternalSort) compareRows(a,b []byte)(int,error){return comparePreparedRows(s.preparedKeys,a,b)}
func compareEncoded(t types.LogicalType,a,b []byte)(int,error){
	if t.IsUnsignedInteger(){
		switch t.ID{
		case types.TinyInt:return semantics.CompareUint64(uint64(a[0]),uint64(b[0])),nil
		case types.SmallInt:return semantics.CompareUint64(uint64(binary.LittleEndian.Uint16(a)),uint64(binary.LittleEndian.Uint16(b))),nil
		case types.Integer:return semantics.CompareUint64(uint64(binary.LittleEndian.Uint32(a)),uint64(binary.LittleEndian.Uint32(b))),nil
		case types.BigInt:return semantics.CompareUint64(binary.LittleEndian.Uint64(a),binary.LittleEndian.Uint64(b)),nil
		}
	}
	switch t.ID{case types.Boolean:av,bv:=a[0]!=0,b[0]!=0;if av==bv{return 0,nil};if !av{return -1,nil};return 1,nil;case types.TinyInt:return semantics.CompareInt64(int64(int8(a[0])),int64(int8(b[0]))),nil;case types.SmallInt:return semantics.CompareInt64(int64(int16(binary.LittleEndian.Uint16(a))),int64(int16(binary.LittleEndian.Uint16(b)))),nil;case types.Integer,types.DateType:return semantics.CompareInt64(int64(int32(binary.LittleEndian.Uint32(a))),int64(int32(binary.LittleEndian.Uint32(b)))),nil;case types.BigInt,types.TimestampType:return semantics.CompareInt64(int64(binary.LittleEndian.Uint64(a)),int64(binary.LittleEndian.Uint64(b))),nil;case types.HugeInt:return types.CompareInt128(types.Int128{Lo:binary.LittleEndian.Uint64(a[:8]),Hi:int64(binary.LittleEndian.Uint64(a[8:16]))},types.Int128{Lo:binary.LittleEndian.Uint64(b[:8]),Hi:int64(binary.LittleEndian.Uint64(b[8:16]))}),nil;case types.Float:return semantics.CompareFloat32(math.Float32frombits(binary.LittleEndian.Uint32(a)),math.Float32frombits(binary.LittleEndian.Uint32(b))),nil;case types.Double:return semantics.CompareFloat64(math.Float64frombits(binary.LittleEndian.Uint64(a)),math.Float64frombits(binary.LittleEndian.Uint64(b))),nil;case types.Decimal:if t.Decimal.Width<=18{return semantics.CompareInt64(int64(binary.LittleEndian.Uint64(a)),int64(binary.LittleEndian.Uint64(b))),nil};return types.CompareInt128(types.Int128{Lo:binary.LittleEndian.Uint64(a[:8]),Hi:int64(binary.LittleEndian.Uint64(a[8:16]))},types.Int128{Lo:binary.LittleEndian.Uint64(b[:8]),Hi:int64(binary.LittleEndian.Uint64(b[8:16]))}),nil;case types.Varchar:return bytes.Compare(a,b),nil;default:return 0,fmt.Errorf("sort compare unsupported type %v",t.ID)}}

type managedIndices struct {
	buf  *memory.Buffer
	data []byte
	n    int
}

func newManagedIndices(scope *memory.Scope, bm *memory.BufferManager, n int) (*managedIndices, error) {
	b, err := bm.Allocate(scope, memory.TagSort, n*4)
	if err != nil {
		return nil, err
	}
	m := &managedIndices{buf: b, data: b.Bytes(), n: n}
	for i := 0; i < n; i++ {
		m.set(i, i)
	}
	return m, nil
}
func (m *managedIndices) get(i int) int { return int(binary.LittleEndian.Uint32(m.data[i*4 : i*4+4])) }
func (m *managedIndices) set(i, v int)  { binary.LittleEndian.PutUint32(m.data[i*4:i*4+4], uint32(v)) }
func (m *managedIndices) swap(i, j int) { a, b := m.get(i), m.get(j); m.set(i, b); m.set(j, a) }
func (m *managedIndices) release() {
	if m.buf != nil {
		m.buf.Release()
		m.buf = nil
		m.data = nil
	}
}
func (s *ExternalSort) sortedIndices() (*managedIndices, error) {
	n := s.builder.Count()
	idx, err := newManagedIndices(s.cfg.Scope, s.cfg.Buffers, n)
	if err != nil {
		return nil, err
	}
	if n < 2 {
		return idx, nil
	}
	row := s.builder.BorrowRowReader()
	compare := func(a, b int) (int, error) { return s.compareRows(row(a), row(b)) }
	prefixes, err := s.newStringPrefixes(row, n)
	if err != nil {
		idx.release()
		return nil, err
	}
	if prefixes != nil {
		defer prefixes.Release()
		compare = s.prefixComparator(prefixes.Bytes(), row)
	}
	err = sortManagedIndices(idx, compare)
	if err != nil {
		idx.release()
		return nil, err
	}
	return idx, nil
}
func(s *ExternalSort)spillBuilder(ctx context.Context)error{if s.builder==nil||s.builder.Count()==0{return nil};idx,err:=s.sortedIndices();if err!=nil{return err};w,err:=newRunWriter(s.store,s.cfg.Scope,s.cfg.Buffers,s.cfg.SpillBlockBytes);if err!=nil{idx.release();return err};for i:=0;i<idx.n;i++{if err:=ctx.Err();err!=nil{w.release();idx.release();return err};if err:=w.append(ctx,s.builder.Row(idx.get(i)));err!=nil{w.release();idx.release();return err}};idx.release();r,err:=w.close(ctx);if err!=nil{w.release();return err};if err:=s.appendRun(r);err!=nil{if r.pages!=nil{r.pages.Release()};return err};s.builder.Release();s.builder=nil;return s.resetBuilder()}
