package tablefunc

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/types"
)

// ParseColumns parses a comma-separated schema for a text reader.
func ParseColumns(spec string) ([]types.StructField, error) {
	parts, err := splitColumnDefinitions(spec)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("columns schema is empty")
	}
	out := make([]types.StructField, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		text := strings.TrimSpace(part)
		split := strings.IndexAny(text, " \t\r\n")
		name := ""
		quotedName := strings.HasPrefix(text, "\"")
		if quotedName {
			var b strings.Builder
			split = 0
			for i := 1; i < len(text); i++ {
				if text[i] == '"' {
					if i+1 < len(text) && text[i+1] == '"' {
						b.WriteByte('"')
						i++
						continue
					}
					split = i + 1
					break
				}
				b.WriteByte(text[i])
			}
			name = b.String()
		}
		if split <= 0 {
			return nil, fmt.Errorf("columns entry %q requires name and type", text)
		}
		if !quotedName {
			name = strings.TrimSpace(text[:split])
		}
		typeText := strings.TrimSpace(text[split:])
		if name == "" || typeText == "" || (quotedName && (split >= len(text) || !strings.ContainsRune(" \t\r\n", rune(text[split])))) {
			return nil, fmt.Errorf("invalid columns entry %q", text)
		}
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("duplicate columns name %q", name)
		}
		seen[key] = struct{}{}
		typ, err := ParseColumnType(typeText)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", name, err)
		}
		out = append(out, types.StructField{Name: name, Type: typ})
	}
	return out, nil
}

func splitColumnDefinitions(spec string) ([]string, error) {
	var out []string
	start, depth := 0, 0
	quoted := false
	for i, r := range spec {
		if r == '"' {
			quoted = !quoted
			continue
		}
		if quoted {
			continue
		}
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("invalid columns schema")
			}
		case ',':
			if depth == 0 {
				out = append(out, spec[start:i])
				start = i + 1
			}
		}
	}
	if depth != 0 || quoted {
		return nil, fmt.Errorf("invalid columns schema")
	}
	out = append(out, spec[start:])
	return out, nil
}

// ParseColumnType parses one scalar type in a text reader schema.
func ParseColumnType(input string) (types.LogicalType, error) {
	text := strings.ToUpper(strings.TrimSpace(input))
	switch text {
	case "BOOLEAN":
		return types.LogicalType{ID: types.Boolean}, nil
	case "TINYINT":
		return types.LogicalType{ID: types.TinyInt}, nil
	case "SMALLINT":
		return types.LogicalType{ID: types.SmallInt}, nil
	case "INTEGER", "INT":
		return types.LogicalType{ID: types.Integer}, nil
	case "BIGINT":
		return types.LogicalType{ID: types.BigInt}, nil
	case "HUGEINT":
		return types.LogicalType{ID: types.HugeInt}, nil
	case "UTINYINT":
		return types.UTinyIntType(), nil
	case "USMALLINT":
		return types.USmallIntType(), nil
	case "UINTEGER":
		return types.UIntegerType(), nil
	case "UBIGINT":
		return types.UBigIntType(), nil
	case "FLOAT":
		return types.LogicalType{ID: types.Float}, nil
	case "DOUBLE":
		return types.LogicalType{ID: types.Double}, nil
	case "VARCHAR", "STRING":
		return types.LogicalType{ID: types.Varchar}, nil
	case "BLOB":
		return types.BlobType(), nil
	case "DATE":
		return types.LogicalType{ID: types.DateType}, nil
	case "TIMESTAMP":
		return types.LogicalType{ID: types.TimestampType}, nil
	case "TIMESTAMP_NS":
		return types.TimestampNSType(), nil
	case "TIMESTAMPTZ":
		return types.TimestampTZType(), nil
	}
	if strings.HasPrefix(text, "DECIMAL(") && strings.HasSuffix(text, ")") {
		body := strings.TrimSpace(text[len("DECIMAL(") : len(text)-1])
		pieces := strings.Split(body, ",")
		if len(pieces) < 1 || len(pieces) > 2 {
			return types.LogicalType{}, fmt.Errorf("invalid %s", text)
		}
		w, err := strconv.Atoi(strings.TrimSpace(pieces[0]))
		if err != nil || w < 1 || w > 38 {
			return types.LogicalType{}, fmt.Errorf("invalid DECIMAL precision")
		}
		scale := 0
		if len(pieces) == 2 {
			scale, err = strconv.Atoi(strings.TrimSpace(pieces[1]))
			if err != nil || scale < 0 || scale > w {
				return types.LogicalType{}, fmt.Errorf("invalid DECIMAL scale")
			}
		}
		return types.NewDecimal(uint8(w), uint8(scale))
	}
	return types.LogicalType{}, fmt.Errorf("unsupported type %q", input)
}
