package json

import (
	"bytes"
	"fmt"
)

type TokenKind uint8

const (
	TokenInvalid TokenKind = iota
	TokenObjectStart
	TokenObjectEnd
	TokenArrayStart
	TokenArrayEnd
	TokenColon
	TokenComma
	TokenString
	TokenNumber
	TokenTrue
	TokenFalse
	TokenNull
	TokenEOF
)

type Token struct {
	Kind       TokenKind
	Start, End int
}

type Tokenizer struct {
	data []byte
	pos  int
}

func NewTokenizer(data []byte) *Tokenizer { return &Tokenizer{data: data} }
func (t *Tokenizer) Position() int         { return t.pos }

func (t *Tokenizer) Next() (Token, error) {
	for t.pos < len(t.data) {
		switch t.data[t.pos] {
		case ' ', '\t', '\r', '\n':
			t.pos++
		default:
			goto nonspace
		}
	}
nonspace:
	if t.pos >= len(t.data) {
		return Token{Kind: TokenEOF, Start: t.pos, End: t.pos}, nil
	}
	start := t.pos
	b := t.data[t.pos]
	t.pos++
	switch b {
	case '{':
		return Token{TokenObjectStart, start, t.pos}, nil
	case '}':
		return Token{TokenObjectEnd, start, t.pos}, nil
	case '[':
		return Token{TokenArrayStart, start, t.pos}, nil
	case ']':
		return Token{TokenArrayEnd, start, t.pos}, nil
	case ':':
		return Token{TokenColon, start, t.pos}, nil
	case ',':
		return Token{TokenComma, start, t.pos}, nil
	case '"':
		esc := false
		for t.pos < len(t.data) {
			c := t.data[t.pos]
			t.pos++
			if esc {
				esc = false
				continue
			}
			if c == '\\' {
				esc = true
				continue
			}
			if c == '"' {
				return Token{TokenString, start, t.pos}, nil
			}
			if c < 0x20 {
				return Token{}, fmt.Errorf("json: control character in string at %d", t.pos-1)
			}
		}
		return Token{}, fmt.Errorf("json: unterminated string at %d", start)
	case 't':
		if t.consumeKeyword("rue") {
			return Token{TokenTrue, start, t.pos}, nil
		}
	case 'f':
		if t.consumeKeyword("alse") {
			return Token{TokenFalse, start, t.pos}, nil
		}
	case 'n':
		if t.consumeKeyword("ull") {
			return Token{TokenNull, start, t.pos}, nil
		}
	default:
		if b == '-' || (b >= '0' && b <= '9') {
			for t.pos < len(t.data) {
				c := t.data[t.pos]
				if (c >= '0' && c <= '9') || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-' {
					t.pos++
					continue
				}
				break
			}
			return Token{TokenNumber, start, t.pos}, nil
		}
	}
	return Token{}, fmt.Errorf("json: invalid token at %d", start)
}

func (t *Tokenizer) consumeKeyword(rest string) bool {
	if t.pos+len(rest) > len(t.data) || !bytes.Equal(t.data[t.pos:t.pos+len(rest)], []byte(rest)) {
		return false
	}
	t.pos += len(rest)
	return true
}

func TokenBytes(data []byte, tok Token) []byte { return data[tok.Start:tok.End] }
