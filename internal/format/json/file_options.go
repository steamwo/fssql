package json

import (
	"fmt"

	"github.com/steamwo/fssql/internal/storage/fileset"
	"github.com/steamwo/fssql/internal/storage/vfs"
	"github.com/steamwo/fssql/internal/tablefunc"
)

func fileViewOptions(pattern string, opts fileset.PlanOptions, registry *vfs.Registry) (string, fileset.PlanOptions, error) {
	decoded, raw, encoded, err := tablefunc.Decode(pattern)
	if err != nil {
		return "", opts, fmt.Errorf("json: decode source options: %w", err)
	}
	if encoded {
		pattern = decoded
		for key, value := range raw {
			if key != "compression" {
				return "", opts, fmt.Errorf("json: unsupported reader option %q", key)
			}
			mode, err := vfs.ParseFileCompression(value)
			if err != nil {
				return "", opts, err
			}
			if mode == "" {
				return "", opts, fmt.Errorf("json: compression must not be empty")
			}
			opts.View.FileCompression = mode
		}
	}
	if opts.View.FileCompression == "" && registry.HasObjectViews() {
		opts.View.FileCompression = "auto"
	}
	return pattern, opts, nil
}
