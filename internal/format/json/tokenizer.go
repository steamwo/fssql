package json

import (
	"fmt"
	"unicode/utf16"
	"unicode/utf8"
)

type TokenKind uint8

const (
	TokenInvalid TokenKind = iota
	TokenLBrace
	TokenRBrace
	TokenLBracket
	TokenRBracket
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
	Raw        []byte
	Start, End int
}
type Tokenizer struct {
	input []byte
	pos   int
}

func NewTokenizer(input []byte) *Tokenizer { return &Tokenizer{input: input} }
func (t *Tokenizer) Position() int         { return t.pos }
func (t *Tokenizer) Next() (Token, error) {
	for t.pos < len(t.input) {
		switch t.input[t.pos] {
		case ' ', '\t', '\r', '\n':
			t.pos++
		default:
			goto token
		}
	}
	return Token{Kind: TokenEOF, Start: t.pos, End: t.pos}, nil
token:
	start := t.pos
	b := t.input[t.pos]
	t.pos++
	switch b {
	case '{':
		return Token{Kind: TokenLBrace, Start: start, End: t.pos}, nil
	case '}':
		return Token{Kind: TokenRBrace, Start: start, End: t.pos}, nil
	case '[':
		return Token{Kind: TokenLBracket, Start: start, End: t.pos}, nil
	case ']':
		return Token{Kind: TokenRBracket, Start: start, End: t.pos}, nil
	case ':':
		return Token{Kind: TokenColon, Start: start, End: t.pos}, nil
	case ',':
		return Token{Kind: TokenComma, Start: start, End: t.pos}, nil
	case '"':
		rawStart := t.pos
		for t.pos < len(t.input) {
			c := t.input[t.pos]
			if c < 0x20 {
				return Token{}, fmt.Errorf("json: control byte in string at %d", t.pos)
			}
			if c == '"' {
				raw := t.input[rawStart:t.pos]
				t.pos++
				return Token{Kind: TokenString, Raw: raw, Start: start, End: t.pos}, nil
			}
			if c == '\\' {
				t.pos++
				if t.pos >= len(t.input) {
					return Token{}, fmt.Errorf("json: incomplete escape at %d", t.pos)
				}
				switch t.input[t.pos] {
				case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				case 'u':
					if t.pos+4 >= len(t.input) {
						return Token{}, fmt.Errorf("json: incomplete unicode escape at %d", t.pos)
					}
					for i := 1; i <= 4; i++ {
						if !isHex(t.input[t.pos+i]) {
							return Token{}, fmt.Errorf("json: invalid unicode escape at %d", t.pos)
						}
					}
					t.pos += 4
				default:
					return Token{}, fmt.Errorf("json: invalid escape at %d", t.pos)
				}
			}
			t.pos++
		}
		return Token{}, fmt.Errorf("json: unterminated string at %d", start)
	case 't':
		if t.consumeLiteral("rue") {
			return Token{Kind: TokenTrue, Raw: t.input[start:t.pos], Start: start, End: t.pos}, nil
		}
	case 'f':
		if t.consumeLiteral("alse") {
			return Token{Kind: TokenFalse, Raw: t.input[start:t.pos], Start: start, End: t.pos}, nil
		}
	case 'n':
		if t.consumeLiteral("ull") {
			return Token{Kind: TokenNull, Raw: t.input[start:t.pos], Start: start, End: t.pos}, nil
		}
	default:
		if b == '-' || (b >= '0' && b <= '9') {
			t.pos = start
			end, err := scanNumber(t.input, start)
			if err != nil {
				return Token{}, err
			}
			t.pos = end
			return Token{Kind: TokenNumber, Raw: t.input[start:end], Start: start, End: end}, nil
		}
	}
	return Token{}, fmt.Errorf("json: unexpected byte %q at %d", b, start)
}
func (t *Tokenizer) consumeLiteral(s string) bool {
	if len(t.input)-t.pos < len(s) {
		return false
	}
	for i := range s {
		if t.input[t.pos+i] != s[i] {
			return false
		}
	}
	t.pos += len(s)
	return true
}
func scanNumber(in []byte, pos int) (int, error) {
	start := pos
	if pos < len(in) && in[pos] == '-' {
		pos++
	}
	if pos >= len(in) {
		return 0, fmt.Errorf("json: incomplete number")
	}
	if in[pos] == '0' {
		pos++
	} else if in[pos] >= '1' && in[pos] <= '9' {
		for pos < len(in) && in[pos] >= '0' && in[pos] <= '9' {
			pos++
		}
	} else {
		return 0, fmt.Errorf("json: invalid number at %d", start)
	}
	if pos < len(in) && in[pos] == '.' {
		pos++
		s := pos
		for pos < len(in) && in[pos] >= '0' && in[pos] <= '9' {
			pos++
		}
		if pos == s {
			return 0, fmt.Errorf("json: invalid fraction at %d", start)
		}
	}
	if pos < len(in) && (in[pos] == 'e' || in[pos] == 'E') {
		pos++
		if pos < len(in) && (in[pos] == '+' || in[pos] == '-') {
			pos++
		}
		s := pos
		for pos < len(in) && in[pos] >= '0' && in[pos] <= '9' {
			pos++
		}
		if pos == s {
			return 0, fmt.Errorf("json: invalid exponent at %d", start)
		}
	}
	return pos, nil
}
func isHex(b byte) bool { return b >= '0' && b <= '9' || b >= 'a' && b <= 'f' || b >= 'A' && b <= 'F' }
func hexValue(b byte) rune {
	if b >= '0' && b <= '9' {
		return rune(b - '0')
	}
	if b >= 'a' && b <= 'f' {
		return rune(b-'a') + 10
	}
	return rune(b-'A') + 10
}
func UnescapeString(raw, dst []byte) ([]byte, error) {
	out := dst[:0]
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			out = append(out, raw[i])
			continue
		}
		i++
		if i >= len(raw) {
			return nil, fmt.Errorf("json: incomplete escape")
		}
		switch raw[i] {
		case '"', '\\', '/':
			out = append(out, raw[i])
		case 'b':
			out = append(out, '\b')
		case 'f':
			out = append(out, '\f')
		case 'n':
			out = append(out, '\n')
		case 'r':
			out = append(out, '\r')
		case 't':
			out = append(out, '\t')
		case 'u':
			if i+4 >= len(raw) {
				return nil, fmt.Errorf("json: incomplete unicode escape")
			}
			r := rune(0)
			for j := 1; j <= 4; j++ {
				if !isHex(raw[i+j]) {
					return nil, fmt.Errorf("json: invalid unicode escape")
				}
				r = r*16 + hexValue(raw[i+j])
			}
			i += 4
			if utf16.IsSurrogate(r) {
				if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
					return nil, fmt.Errorf("json: unpaired surrogate")
				}
				r2 := rune(0)
				for j := 3; j <= 6; j++ {
					if !isHex(raw[i+j]) {
						return nil, fmt.Errorf("json: invalid surrogate")
					}
					r2 = r2*16 + hexValue(raw[i+j])
				}
				decoded := utf16.DecodeRune(r, r2)
				if decoded == utf8.RuneError {
					return nil, fmt.Errorf("json: invalid surrogate pair")
				}
				r = decoded
				i += 6
			}
			var tmp [utf8.UTFMax]byte
			n := utf8.EncodeRune(tmp[:], r)
			out = append(out, tmp[:n]...)
		default:
			return nil, fmt.Errorf("json: invalid escape")
		}
	}
	return out, nil
}

const maxJSONDepth = 128

func SkipValue(t *Tokenizer, first Token) error { return skipValue(t, first, 0) }
func skipValue(t *Tokenizer, first Token, depth int) error {
	if depth > maxJSONDepth {
		return fmt.Errorf("json: nesting exceeds %d", maxJSONDepth)
	}
	switch first.Kind {
	case TokenString, TokenNumber, TokenTrue, TokenFalse, TokenNull:
		return nil
	case TokenLBracket:
		next, err := t.Next()
		if err != nil {
			return err
		}
		if next.Kind == TokenRBracket {
			return nil
		}
		for {
			if err := skipValue(t, next, depth+1); err != nil {
				return err
			}
			sep, err := t.Next()
			if err != nil {
				return err
			}
			if sep.Kind == TokenRBracket {
				return nil
			}
			if sep.Kind != TokenComma {
				return fmt.Errorf("json: expected comma or ]")
			}
			next, err = t.Next()
			if err != nil {
				return err
			}
		}
	case TokenLBrace:
		next, err := t.Next()
		if err != nil {
			return err
		}
		if next.Kind == TokenRBrace {
			return nil
		}
		for {
			if next.Kind != TokenString {
				return fmt.Errorf("json: expected object key")
			}
			colon, err := t.Next()
			if err != nil {
				return err
			}
			if colon.Kind != TokenColon {
				return fmt.Errorf("json: expected colon")
			}
			value, err := t.Next()
			if err != nil {
				return err
			}
			if err := skipValue(t, value, depth+1); err != nil {
				return err
			}
			sep, err := t.Next()
			if err != nil {
				return err
			}
			if sep.Kind == TokenRBrace {
				return nil
			}
			if sep.Kind != TokenComma {
				return fmt.Errorf("json: expected comma or }")
			}
			next, err = t.Next()
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("json: token %d cannot start value", first.Kind)
	}
}
