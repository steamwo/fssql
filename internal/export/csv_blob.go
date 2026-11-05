package export

import "context"

// writeBlobField quotes the hexadecimal representation without buffering it.
func (w *CSVWriter) writeBlobField(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return w.writeStringField(ctx, "", w.opts.NullString == "")
	}
	const digits = "0123456789ABCDEF"
	quote := w.opts.ForceQuote
	matchesNull := len(w.opts.NullString)/2 == len(data) && len(w.opts.NullString)%2 == 0
	for i, b := range data {
		hi, lo := digits[b>>4], digits[b&15]
		if hi == w.opts.Delimiter || lo == w.opts.Delimiter || hi == w.opts.Quote || lo == w.opts.Quote {
			quote = true
		}
		if matchesNull && (w.opts.NullString[2*i] != hi || w.opts.NullString[2*i+1] != lo) {
			matchesNull = false
		}
	}
	quote = quote || matchesNull
	var output valueOutput = w.out
	if quote {
		if err := w.out.WriteContextByte(ctx, w.opts.Quote); err != nil {
			return err
		}
		output = &csvQuotedOutput{out: w.out, quote: w.opts.Quote}
	}
	for _, b := range data {
		if err := output.WriteContextByte(ctx, digits[b>>4]); err != nil {
			return err
		}
		if err := output.WriteContextByte(ctx, digits[b&15]); err != nil {
			return err
		}
	}
	if quote {
		return w.out.WriteContextByte(ctx, w.opts.Quote)
	}
	return nil
}
