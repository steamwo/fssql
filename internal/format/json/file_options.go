package json

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/storage/fileset"
	"github.com/steamwo/fssql/internal/storage/vfs"
	"github.com/steamwo/fssql/internal/tablefunc"
)

func fileOptions(pattern string, opts fileset.PlanOptions, registry *vfs.Registry) (string, fileset.PlanOptions, []types.StructField, error) {
	decoded, raw, encoded, err := tablefunc.Decode(pattern)
	if err != nil {
		return "", opts, nil, fmt.Errorf("json: decode source options: %w", err)
	}
	var columns []types.StructField
	if encoded {
		pattern = decoded
		for key, value := range raw {
			switch key {
			case "compression":
				mode, err := vfs.ParseFileCompression(value)
				if err != nil {
					return "", opts, nil, err
				}
				if mode == "" {
					return "", opts, nil, fmt.Errorf("json: compression must not be empty")
				}
				opts.View.FileCompression = mode
			case "columns":
				columns, err = tablefunc.ParseColumns(value)
				if err != nil {
					return "", opts, nil, fmt.Errorf("json: %w", err)
				}
				for _, column := range columns {
					t := column.Type
					switch t.ID {
					case types.Boolean, types.Integer, types.BigInt, types.Float, types.Double, types.Varchar:
						if !t.Unsigned && !t.Binary {
							continue
						}
					}
					return "", opts, nil, fmt.Errorf("json: column %q uses an unsupported explicit type", column.Name)
				}
			default:
				return "", opts, nil, fmt.Errorf("json: unsupported reader option %q", key)
			}
		}
	}
	if opts.View.FileCompression == "" && registry.HasObjectViews() {
		opts.View.FileCompression = "auto"
	}
	return pattern, opts, columns, nil
}
