package types

import "testing"

func TestDecimalValidationAndPhysical(t *testing.T) {
	d, err := NewDecimal(18, 4)
	if err != nil || d.Physical() != PhysicalInt64 {
		t.Fatalf("DECIMAL(18,4): %#v %v", d, err)
	}
	d, err = NewDecimal(38, 10)
	if err != nil || d.Physical() != PhysicalInt128 {
		t.Fatalf("DECIMAL(38,10): %#v %v", d, err)
	}
	for _, tc := range [][2]uint8{{0, 0}, {39, 0}, {4, 5}} {
		if _, err := NewDecimal(tc[0], tc[1]); err == nil {
			t.Fatalf("expected invalid decimal %v", tc)
		}
	}
}

func TestLogicalPhysicalSeparation(t *testing.T) {
	if (LogicalType{ID: DateType}).Physical() != PhysicalInt32 {
		t.Fatal("date physical")
	}
	if (LogicalType{ID: TimestampType}).Physical() != PhysicalInt64 {
		t.Fatal("timestamp physical")
	}
	if (LogicalType{ID: Varchar}).Physical() != PhysicalString {
		t.Fatal("varchar physical")
	}
}

func TestListAndStructAreRealTypes(t *testing.T) {
	list := NewList(LogicalType{ID: BigInt})
	if list.Child == nil || list.Physical() != PhysicalList {
		t.Fatal("list")
	}
	st := NewStruct(StructField{Name: "x", Type: LogicalType{ID: BigInt}})
	if len(st.Fields) != 1 || st.Physical() != PhysicalStruct {
		t.Fatal("struct")
	}
}
