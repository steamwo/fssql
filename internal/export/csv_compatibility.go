package export

import (
	"context"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func (w *CSVWriter) writeScalarField(ctx context.Context, vec *vector.Vector, typ types.LogicalType, row int) error {
	if w.opts.DuckDBCompatible && typ.IsTimestampTZ() {
		raw, _ := vec.Int64(row)
		plain := typ
		plain.Timestamp.AdjustedToUTC = false
		text := appendTimestampText(w.out.scalarScratch[:0], raw, plain)
		return w.writeTextField(ctx, append(text, "+00"...))
	}
	if !w.opts.ForceQuote {
		return writeCSVScalar(ctx, w.out, w.out.scalarScratch[:0], vec, typ, row)
	}
	if err := w.out.WriteContextByte(ctx, w.opts.Quote); err != nil {
		return err
	}
	quoted := csvQuotedOutput{out: w.out, quote: w.opts.Quote}
	if err := writeCSVScalar(ctx, &quoted, w.out.scalarScratch[:0], vec, typ, row); err != nil {
		return err
	}
	return w.out.WriteContextByte(ctx, w.opts.Quote)
}

// walkBlobText streams DuckDB's printable ASCII / \xHH BLOB representation.
func walkBlobText(data []byte, emit func(byte) error) error {
	const hex = "0123456789ABCDEF"
	for _, b := range data {
		if b >= 32 && b <= 126 && b != '\\' && b != '\'' && b != '"' {
			if err := emit(b); err != nil {
				return err
			}
		} else {
			for _, c := range [4]byte{'\\', 'x', hex[b>>4], hex[b&15]} {
				if err := emit(c); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (w *CSVWriter) writeDuckDBBlob(ctx context.Context, vec *vector.Vector, row int) error {
	data, _ := vec.Bytes(row)
	quote := w.opts.ForceQuote
	pos, matchesNull := 0, true
	_ = walkBlobText(data, func(b byte) error {
		quote = quote || b == w.opts.Delimiter || b == w.opts.Quote || b == '\r' || b == '\n'
		if pos >= len(w.opts.NullString) || w.opts.NullString[pos] != b {
			matchesNull = false
		}
		pos++
		return nil
	})
	quote = quote || (matchesNull && pos == len(w.opts.NullString))
	var out valueOutput = w.out
	if quote {
		if err := w.out.WriteContextByte(ctx, w.opts.Quote); err != nil {
			return err
		}
		out = &csvQuotedOutput{out: w.out, quote: w.opts.Quote}
	}
	if err := walkBlobText(data, func(b byte) error { return out.WriteContextByte(ctx, b) }); err != nil {
		return err
	}
	if quote {
		return w.out.WriteContextByte(ctx, w.opts.Quote)
	}
	return nil
}
