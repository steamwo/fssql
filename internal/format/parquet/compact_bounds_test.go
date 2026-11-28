package parquet

import (
	"bytes"
	"testing"
)

func TestCompactListRejectsTruncatedCountsBeforeAllocation(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  byte
	}{
		{"schema", ctStruct},
		{"integer", ctI32},
		{"binary", ctBinary},
		{"boolean", ctBoolTrue},
		{"double", ctDouble},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, count := range []int{1, 14, 15, 65536, maxCompactCollection} {
				w := &compactWriter{}
				w.listHeader(1, tc.typ, count)
				r := newCompactReader(w.b[1:])
				if _, _, err := r.listHeader(); err == nil {
					t.Fatalf("accepted count=%d with no collection payload", count)
				}
			}
		})
	}
}

func TestCompactListAllowsMinimumEncodedValues(t *testing.T) {
	// Empty structs still have a STOP byte; empty strings have a length byte.
	for _, typ := range []byte{ctStruct, ctBinary, ctI32, ctBoolTrue} {
		for _, count := range []int{0, 1, 14, 15, 128} {
			w := &compactWriter{}
			w.listHeader(1, typ, count)
			w.b = append(w.b, bytes.Repeat([]byte{0}, count)...)
			r := newCompactReader(w.b[1:])
			gotType, gotCount, err := r.listHeader()
			if err != nil || gotType != typ || gotCount != count {
				t.Fatalf("type=%d count=%d: got type=%d count=%d error=%v", typ, count, gotType, gotCount, err)
			}
		}
	}
}
