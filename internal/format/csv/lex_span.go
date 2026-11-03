package csv

import (
	"bytes"
	"encoding/binary"
)

// advanceData consumes a span whose every byte would emit only EventData.
// Structural bytes still go through Step, so parsing, counting and boundary
// recognition use the same quote/CRLF rules. No field or record is skipped.
func (m *Machine) advanceData(input []byte) int {
	if m.skipLF || len(input) == 0 {
		return 0
	}
	if m.state == StateQuoted {
		if end := bytes.IndexByte(input, m.cfg.Quote); end >= 0 {
			return end
		}
		return len(input)
	}
	if m.state != StateFieldStart && m.state != StateUnquoted {
		return 0
	}
	const ones = uint64(0x0101010101010101)
	delimiter, quote := uint64(m.cfg.Delimiter)*ones, uint64(m.cfg.Quote)*ones
	i := 0
	for len(input)-i >= 8 {
		word := binary.LittleEndian.Uint64(input[i:])
		if csvHasZero(word^delimiter) || csvHasZero(word^quote) || csvHasZero(word^('\n'*ones)) || csvHasZero(word^('\r'*ones)) {
			break
		}
		i += 8
	}
	for i < len(input) {
		b := input[i]
		if b == m.cfg.Delimiter || b == m.cfg.Quote || b == '\n' || b == '\r' {
			break
		}
		i++
	}
	if i > 0 {
		m.state = StateUnquoted
	}
	return i
}

func csvHasZero(word uint64) bool {
	return (word-0x0101010101010101)&^word&0x8080808080808080 != 0
}
