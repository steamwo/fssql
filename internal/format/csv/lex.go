package csv

import "fmt"

// LexState is the CSV lexical state shared by row parsing and boundary
// recognition. It deliberately contains no row/value materialization state.
type LexState uint8

const (
	StateFieldStart LexState = iota
	StateUnquoted
	StateQuoted
	StateAfterQuote
)

// LexEvent is a bit mask emitted for one consumed byte.
type LexEvent uint8

const (
	EventNone LexEvent = 0
	EventData LexEvent = 1 << iota
	EventFieldEnd
	EventRecordEnd
)

func (e LexEvent) Has(flag LexEvent) bool { return e&flag != 0 }

// LexConfig contains the lexical characters that affect CSV boundaries.
type LexConfig struct {
	Delimiter byte
	Quote     byte
}

func DefaultLexConfig() LexConfig { return LexConfig{Delimiter: ',', Quote: '"'} }

func (c LexConfig) Validate() error {
	if c.Delimiter == 0 {
		return fmt.Errorf("csv: delimiter must not be NUL")
	}
	if c.Quote == 0 {
		return fmt.Errorf("csv: quote must not be NUL")
	}
	if c.Delimiter == c.Quote {
		return fmt.Errorf("csv: delimiter and quote must differ")
	}
	if c.Delimiter == '\n' || c.Delimiter == '\r' {
		return fmt.Errorf("csv: delimiter must not be a record separator")
	}
	if c.Quote == '\n' || c.Quote == '\r' {
		return fmt.Errorf("csv: quote must not be a record separator")
	}
	return nil
}

// Machine implements the quote-aware CSV lexical rules. A single Machine can
// be fed incrementally across arbitrary input chunks; quoted newlines never
// emit EventRecordEnd. CRLF is normalized to one record boundary without
// requiring look-behind in callers.
type Machine struct {
	cfg    LexConfig
	state  LexState
	skipLF bool
}

func NewMachine(cfg LexConfig) (Machine, error) {
	if err := cfg.Validate(); err != nil {
		return Machine{}, err
	}
	return Machine{cfg: cfg, state: StateFieldStart}, nil
}

func (m *Machine) State() LexState { return m.state }

// Step consumes exactly one byte and returns lexical boundary/data events.
// Syntax errors are reported at the byte that makes the input invalid.
func (m *Machine) Step(b byte) (LexEvent, error) {
	if m.skipLF {
		m.skipLF = false
		if b == '\n' {
			return EventNone, nil
		}
	}

	switch m.state {
	case StateFieldStart:
		switch b {
		case m.cfg.Quote:
			m.state = StateQuoted
			return EventNone, nil
		case m.cfg.Delimiter:
			return EventFieldEnd, nil
		case '\n':
			return EventFieldEnd | EventRecordEnd, nil
		case '\r':
			m.skipLF = true
			return EventFieldEnd | EventRecordEnd, nil
		default:
			m.state = StateUnquoted
			return EventData, nil
		}

	case StateUnquoted:
		switch b {
		case m.cfg.Quote:
			return EventNone, fmt.Errorf("csv: quote inside unquoted field")
		case m.cfg.Delimiter:
			m.state = StateFieldStart
			return EventFieldEnd, nil
		case '\n':
			m.state = StateFieldStart
			return EventFieldEnd | EventRecordEnd, nil
		case '\r':
			m.state = StateFieldStart
			m.skipLF = true
			return EventFieldEnd | EventRecordEnd, nil
		default:
			return EventData, nil
		}

	case StateQuoted:
		if b == m.cfg.Quote {
			m.state = StateAfterQuote
			return EventNone, nil
		}
		return EventData, nil

	case StateAfterQuote:
		switch b {
		case m.cfg.Quote:
			m.state = StateQuoted
			return EventData, nil
		case m.cfg.Delimiter:
			m.state = StateFieldStart
			return EventFieldEnd, nil
		case '\n':
			m.state = StateFieldStart
			return EventFieldEnd | EventRecordEnd, nil
		case '\r':
			m.state = StateFieldStart
			m.skipLF = true
			return EventFieldEnd | EventRecordEnd, nil
		default:
			return EventNone, fmt.Errorf("csv: unexpected byte %q after closing quote", b)
		}
	default:
		return EventNone, fmt.Errorf("csv: invalid lexical state %d", m.state)
	}
}

// Finish validates end-of-input. It intentionally does not manufacture a
// record boundary; parsers decide how to finalize a last record without a line
// terminator, while boundary recognizers only report boundaries actually seen.
func (m *Machine) Finish() error {
	if m.state == StateQuoted {
		return fmt.Errorf("csv: unterminated quoted field")
	}
	return nil
}
