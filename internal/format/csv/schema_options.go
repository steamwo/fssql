package csv

import (
	"fmt"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/types"
"github.com/steamwo/fssql/internal/semantics"
	"github.com/steamwo/fssql/internal/tablefunc"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const (
	noQuotedNullPrefix  = "\x00fssql:no-quoted-null:\x00"
	yesQuotedNullPrefix = "\x00fssql:quoted-null:\x00"
)

func ParseTableFunctionSource(token string) (string, SourceBindOptions, error) {
	pattern, raw, encoded, err := tablefunc.Decode(token)
	if err != nil {
		return "", SourceBindOptions{}, fmt.Errorf("csv: decode read_csv options: %w", err)
	}
	if !encoded {
		return pattern, SourceBindOptions{}, nil
	}
	opts := SourceBindOptions{}
	autoDetect := true
	for key, value := range raw {
		switch key {
		case "compression":
			mode,err:=vfs.ParseFileCompression(value);if err!=nil{return "",SourceBindOptions{},err};if mode==""{return "",SourceBindOptions{},fmt.Errorf("csv: compression must not be empty")};opts.FileSet.View.FileCompression=mode
		case "header":
			flag, err := parseCSVBoolOption(key, value)
			if err != nil {
				return "", SourceBindOptions{}, err
			}
			opts.Inference.Header = flag
		case "auto_detect":
			flag, err := parseCSVBoolOption(key, value)
			if err != nil {
				return "", SourceBindOptions{}, err
			}
			autoDetect = flag
		case "allow_quoted_nulls":
			flag, err := parseCSVBoolOption(key, value)
			if err != nil {
				return "", SourceBindOptions{}, err
			}
			opts.AllowQuotedNulls = flag
			opts.QuotedNullsSpecified = true
			if flag {
				opts.Inference.NullLiteral = []byte(yesQuotedNullPrefix)
			} else {
				opts.Inference.NullLiteral = []byte(noQuotedNullPrefix)
			}
		case "columns":
			fields, err := parseCSVColumns(value)
			if err != nil {
				return "", SourceBindOptions{}, err
			}
			opts.ExplicitSchema = fields
		case "dateformat","date_format":opts.Inference.DateFormat=value
		case "timestampformat","timestamp_format":opts.Inference.TimestampFormat=value
		case "auto_type_candidates":
			var names []string;if err:=json.Unmarshal([]byte(value),&names);err!=nil{return "",SourceBindOptions{},fmt.Errorf("auto_type_candidates requires a list of type names")};opts.Inference.Candidates=make([]types.LogicalType,0,len(names));for _,name:=range names{if strings.EqualFold(name,"NULL"){continue};if strings.EqualFold(name,"DECIMAL"){name="DECIMAL(18,3)"};typ,err:=parseCSVType(name);if err!=nil{return "",SourceBindOptions{},err};if typ.IsBlob()||typ.ID==types.List||typ.ID==types.Struct{return "",SourceBindOptions{},fmt.Errorf("unsupported CSV candidate %s",name)};opts.Inference.Candidates=append(opts.Inference.Candidates,typ)}
		default:
			return "", SourceBindOptions{}, fmt.Errorf("csv: unsupported read_csv option %q", key)
		}
	}
 for _,format:=range []string{opts.Inference.DateFormat,opts.Inference.TimestampFormat}{if format!=""{if _,err:=semantics.TimeFormatLayout(format);err!=nil{return "",SourceBindOptions{},err}}}
	opts.AutoDetect = &autoDetect
	if !autoDetect && len(opts.ExplicitSchema) == 0 {
		return "", SourceBindOptions{}, fmt.Errorf("csv: auto_detect=false requires columns")
	}
	return pattern, opts, nil
}

func effectiveCSVNullLiteral(encoded []byte) []byte {
	switch string(encoded) {
	case noQuotedNullPrefix, yesQuotedNullPrefix:
		return []byte{}
	default:
		return encoded
	}
}

func csvQuotedNullAllowed(encoded []byte) bool { return string(encoded) != noQuotedNullPrefix }

func parseCSVBoolOption(name, value string) (bool, error) {
	flag, err := strconv.ParseBool(strings.ToLower(strings.TrimSpace(value)))
	if err != nil {
		return false, fmt.Errorf("csv: option %s requires BOOLEAN", name)
	}
	return flag, nil
}

func parseCSVColumns(spec string) ([]types.StructField, error) {
	parts, err := splitCSVSchemaFields(spec)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("csv: columns schema is empty")
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
			return nil, fmt.Errorf("csv: columns entry %q requires name and type", text)
		}
		if !quotedName {
			name = strings.TrimSpace(text[:split])
		}
		typeText := strings.TrimSpace(text[split:])
		if name == "" || typeText == "" || (quotedName && (split >= len(text) || !strings.ContainsRune(" \t\r\n", rune(text[split])))) {
			return nil, fmt.Errorf("csv: invalid columns entry %q", text)
		}
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("csv: duplicate columns name %q", name)
		}
		seen[key] = struct{}{}
		typ, err := parseCSVType(typeText)
		if err != nil {
			return nil, fmt.Errorf("csv: column %s: %w", name, err)
		}
		out = append(out, types.StructField{Name: name, Type: typ})
	}
	return out, nil
}

func splitCSVSchemaFields(spec string) ([]string, error) {
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
				return nil, fmt.Errorf("csv: invalid columns schema")
			}
		case ',':
			if depth == 0 {
				out = append(out, spec[start:i])
				start = i + 1
			}
		}
	}
	if depth != 0 || quoted {
		return nil, fmt.Errorf("csv: invalid columns schema")
	}
	out = append(out, spec[start:])
	return out, nil
}

func parseCSVType(input string) (types.LogicalType, error) {
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
