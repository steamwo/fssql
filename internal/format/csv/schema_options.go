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
			fields, err := tablefunc.ParseColumns(value)
			if err != nil {
				return "", SourceBindOptions{}, fmt.Errorf("csv: %w", err)
			}
			opts.ExplicitSchema = fields
		case "dateformat","date_format":opts.Inference.DateFormat=value
		case "timestampformat","timestamp_format":opts.Inference.TimestampFormat=value
		case "auto_type_candidates":
			var names []string;if err:=json.Unmarshal([]byte(value),&names);err!=nil{return "",SourceBindOptions{},fmt.Errorf("auto_type_candidates requires a list of type names")};opts.Inference.Candidates=make([]types.LogicalType,0,len(names));for _,name:=range names{if strings.EqualFold(name,"NULL"){continue};if strings.EqualFold(name,"DECIMAL"){name="DECIMAL(18,3)"};typ,err:=tablefunc.ParseColumnType(name);if err!=nil{return "",SourceBindOptions{},err};if typ.IsBlob()||typ.ID==types.List||typ.ID==types.Struct{return "",SourceBindOptions{},fmt.Errorf("unsupported CSV candidate %s",name)};opts.Inference.Candidates=append(opts.Inference.Candidates,typ)}
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
