package function

import "testing"

func TestBuiltinCatalogLookupID(t *testing.T) {
	catalog := NewBuiltinCatalog()
	for _, id := range []ID{CountID, SumID, AvgID, MinID, MaxID, ReadParquetID, ReadCSVID, ReadJSONID, ReadNDJSONID, GlobID} {
		definition, ok := catalog.LookupID(id)
		if !ok || definition.ID != id || definition.Name == "" {
			t.Fatalf("id=%d definition=%+v ok=%v", id, definition, ok)
		}
	}
	if _, ok := catalog.LookupID(InvalidID); ok {
		t.Fatal("InvalidID must not resolve")
	}
}
