package gosql

// Public logical type constructors for the Phase 2 fidelity types. They are
// values rather than aliases to internal IDs, so callers do not depend on the
// engine's physical representation.
func UTinyIntType() Type  { return Type{ID: "UTINYINT"} }
func USmallIntType() Type { return Type{ID: "USMALLINT"} }
func UIntegerType() Type  { return Type{ID: "UINTEGER"} }
func UBigIntType() Type   { return Type{ID: "UBIGINT"} }
func BlobType() Type      { return Type{ID: "BLOB"} }
func TimestampNSType() Type { return Type{ID: "TIMESTAMP_NS"} }
func TimestampTZType() Type { return Type{ID: "TIMESTAMPTZ"} }
