package json

import (
	"fmt"
	"strconv"
)

func extractObject(raw []byte, index map[string]int, width int) ([]cell, error) {
	t := NewTokenizer(raw)
	first, err := t.Next()
	if err != nil {
		return nil, err
	}
	if first.Kind != TokenObjectStart {
		return nil, fmt.Errorf("expected object")
	}
	out := make([]cell, width)
	tok, err := t.Next()
	if err != nil {
		return nil, err
	}
	if tok.Kind == TokenObjectEnd {
		tail, _ := t.Next()
		if tail.Kind != TokenEOF {
			return nil, fmt.Errorf("trailing content")
		}
		return out, nil
	}
	for {
		if tok.Kind != TokenString {
			return nil, fmt.Errorf("expected object key at %d", tok.Start)
		}
		key, err := decodeKey(raw[tok.Start:tok.End])
		if err != nil {
			return nil, err
		}
		colon, err := t.Next()
		if err != nil {
			return nil, err
		}
		if colon.Kind != TokenColon {
			return nil, fmt.Errorf("expected colon after key %q", key)
		}
		val, err := t.Next()
		if err != nil {
			return nil, err
		}
		start, end, err := consumeValue(raw, t, val)
		if err != nil {
			return nil, err
		}
		if i, ok := index[key]; ok {
			out[i] = cell{raw: raw[start:end], present: true}
		}
		delim, err := t.Next()
		if err != nil {
			return nil, err
		}
		if delim.Kind == TokenObjectEnd {
			break
		}
		if delim.Kind != TokenComma {
			return nil, fmt.Errorf("expected comma or object end at %d", delim.Start)
		}
		tok, err = t.Next()
		if err != nil {
			return nil, err
		}
	}
	tail, err := t.Next()
	if err != nil {
		return nil, err
	}
	if tail.Kind != TokenEOF {
		return nil, fmt.Errorf("trailing content at %d", tail.Start)
	}
	return out, nil
}

func extractArray(raw []byte) ([]cell, error) {
	t := NewTokenizer(raw)
	first, err := t.Next()
	if err != nil {
		return nil, err
	}
	if first.Kind != TokenArrayStart {
		return nil, fmt.Errorf("expected array")
	}
	tok, err := t.Next()
	if err != nil {
		return nil, err
	}
	if tok.Kind == TokenArrayEnd {
		return nil, nil
	}
	var out []cell
	for {
		start, end, err := consumeValue(raw, t, tok)
		if err != nil {
			return nil, err
		}
		out = append(out, cell{raw: raw[start:end], present: true})
		delim, err := t.Next()
		if err != nil {
			return nil, err
		}
		if delim.Kind == TokenArrayEnd {
			break
		}
		if delim.Kind != TokenComma {
			return nil, fmt.Errorf("expected comma or array end at %d", delim.Start)
		}
		tok, err = t.Next()
		if err != nil {
			return nil, err
		}
	}
	tail, err := t.Next()
	if err != nil {
		return nil, err
	}
	if tail.Kind != TokenEOF {
		return nil, fmt.Errorf("trailing array content")
	}
	return out, nil
}

func consumeValue(data []byte, t *Tokenizer, first Token) (int, int, error) {
	start := first.Start
	switch first.Kind {
	case TokenString, TokenNumber, TokenTrue, TokenFalse, TokenNull:
		return start, first.End, nil
	case TokenObjectStart, TokenArrayStart:
	default:
		return 0, 0, fmt.Errorf("expected value at %d", first.Start)
	}
	depth := 1
	end := first.End
	for depth > 0 {
		tok, err := t.Next()
		if err != nil {
			return 0, 0, err
		}
		if tok.Kind == TokenEOF {
			return 0, 0, fmt.Errorf("unterminated structured value at %d", start)
		}
		switch tok.Kind {
		case TokenObjectStart, TokenArrayStart:
			depth++
		case TokenObjectEnd, TokenArrayEnd:
			depth--
		}
		end = tok.End
	}
	return start, end, nil
}

func decodeKey(raw []byte) (string, error) {
	s, err := strconv.Unquote(string(raw))
	if err != nil {
		return "", fmt.Errorf("invalid object key: %w", err)
	}
	return s, nil
}
