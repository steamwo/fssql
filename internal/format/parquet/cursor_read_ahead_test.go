package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func headerLookaheadFixture() []byte {
	var data []byte
	for page := 0; page < 3; page++ {
		body := make([]byte, 32*4)
		for row := 0; row < 32; row++ {
			binary.LittleEndian.PutUint32(body[row*4:], uint32(page*32+row))
		}
		data = append(data, cursorDataPageHeader(32, len(body), EncodingPlain)...)
		data = append(data, body...)
	}
	return data
}

func TestColumnCursorReusesBoundedNextHeaderProbe(t *testing.T) {
	data := headerLookaheadFixture()
	driver := &recordingDriver{data: data}
	uri, _ := vfs.ParseURI("mem://x/header-lookahead")
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	cursor, err := NewColumnCursor(driver, uri, vfs.ObjectIdentity{ETag: "e", Size: int64(len(data))}, &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 96, TotalCompressedSize: int64(len(data))}, requiredInt32Leaf(), scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	for base := 0; base < 96; {
		out, err := cursor.ReadScalar(context.Background(), 47)
		if err != nil {
			t.Fatal(err)
		}
		for row := 0; row < out.Count(); row++ {
			if value, valid := out.Int32(row); !valid || value != int32(base+row) {
				t.Fatalf("row=%d value=%d valid=%v", base+row, value, valid)
			}
		}
		base += out.Count()
		out.Release()
	}
	cursor.Close()
	if cursor.Metrics().Requests != 4 || cursor.Metrics().PagesRead != 3 {
		t.Fatalf("expected first header plus three bodies: %+v", cursor.Metrics())
	}
	for _, request := range driver.ranges {
		if request.Offset < 0 || request.Length > int64(len(data))-request.Offset || request.Length > 128+pageHeaderPiggyback || request.Expected == nil || request.Expected.ETag != "e" {
			t.Fatalf("unsafe lookahead range: %+v", request)
		}
	}
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}

func TestHeaderLookaheadFailureAndBudgetFallback(t *testing.T) {
	uri, _ := vfs.ParseURI("mem://x/header-lookahead")
	for _, limit := range []int64{64, 200} {
		m := memory.NewManager(limit)
		scope := m.NewScope()
		bm := memory.NewBufferManager(nil)
		driver := &recordingDriver{data: make([]byte, 512)}
		cursor := &ColumnCursor{driver: driver, uri: uri, end: 512, scope: scope, bm: bm}
		// 64-byte page fits; either the optional probe allocation or the
		// expanded request fails, and exact-body reading must still succeed.
		body, err := cursor.readPageBody(context.Background(), 0, 64)
		if err != nil || body.Len() != 64 || cursor.headerTail != nil {
			t.Fatalf("limit=%d body=%v err=%v", limit, body, err)
		}
		body.Release()
		cursor.Close()
		if m.Used() != 0 {
			t.Fatalf("limit=%d managed leak=%d", limit, m.Used())
		}
	}
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	data := headerLookaheadFixture()
	driver := &recordingDriver{data: data, mutateAt: 3}
	cursor, err := NewColumnCursor(driver, uri, vfs.ObjectIdentity{ETag: "e", Size: int64(len(data))}, &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 96, TotalCompressedSize: int64(len(data))}, requiredInt32Leaf(), scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if out, err := cursor.ReadScalar(context.Background(), 96); !errors.Is(err, vfs.ErrObjectChanged) || out != nil {
		t.Fatalf("object change out=%v err=%v", out, err)
	}
	cursor.Close()
	if len(driver.ranges) != 3 || m.Used() != 0 {
		t.Fatalf("I/O error retried or leaked: reads=%d managed=%d", len(driver.ranges), m.Used())
	}
}
