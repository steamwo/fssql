package lexer

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/steamwo/fssql/internal/sql/ast"
)

var keywords = map[string]Kind{"SELECT": KeywordSelect, "FROM": KeywordFrom, "WHERE": KeywordWhere, "GROUP": KeywordGroup, "BY": KeywordBy, "HAVING": KeywordHaving, "ORDER": KeywordOrder, "LIMIT": KeywordLimit, "OFFSET": KeywordOffset, "DISTINCT": KeywordDistinct, "AS": KeywordAs, "WITH": KeywordWith, "JOIN": KeywordJoin, "INNER": KeywordInner, "LEFT": KeywordLeft, "RIGHT": KeywordRight, "FULL": KeywordFull, "CROSS": KeywordCross, "SEMI": KeywordSemi, "ANTI": KeywordAnti, "OUTER": KeywordOuter, "ON": KeywordOn, "USING": KeywordUsing, "ASC": KeywordAsc, "DESC": KeywordDesc, "NULLS": KeywordNulls, "FIRST": KeywordFirst, "LAST": KeywordLast, "AND": KeywordAnd, "OR": KeywordOr, "NOT": KeywordNot, "IS": KeywordIs, "NULL": KeywordNull, "TRUE": KeywordTrue, "FALSE": KeywordFalse}

// Lexer 是 SQL 控制面词法器，保存当前字节偏移与行列信息。
type Lexer struct {
	src    string // src 是不可变 SQL 源文本。
	offset int    // offset 是当前 UTF-8 字节偏移。
	line   int    // line 是当前 1-based 行号。
	column int    // column 是当前 1-based 字符列号。
}

// New 创建 SQL 词法器。
func New(src string) *Lexer { return &Lexer{src: src, line: 1, column: 1} }

// Next 扫描下一个 token，并保留精确源位置。
func (l *Lexer) Next() (Token, error) {
	if err := l.skipSpaceAndComments(); err != nil {
		return Token{}, err
	}
	start := l.position()
	if l.offset >= len(l.src) {
		return Token{Kind: EOF, Span: ast.Span{Start: start, End: start}}, nil
	}
	ch := l.src[l.offset]
	switch ch {
	case ',':
		return l.single(Comma), nil
	case '.':
		return l.single(Dot), nil
	case '(':
		return l.single(LParen), nil
	case ')':
		return l.single(RParen), nil
	case '*':
		return l.single(Star), nil
	case '+':
		return l.single(Plus), nil
	case '-':
		return l.single(Minus), nil
	case '/':
		return l.single(Slash), nil
	case '%':
		return l.single(Percent), nil
	case '=':
		return l.single(Eq), nil
	case ';':
		return l.single(Semicolon), nil
	case '<':
		l.advanceByte()
		if l.matchByte('=') {
			return l.tokenFrom(start, Lte), nil
		}
		if l.matchByte('>') {
			return l.tokenFrom(start, Neq), nil
		}
		return l.tokenFrom(start, Lt), nil
	case '>':
		l.advanceByte()
		if l.matchByte('=') {
			return l.tokenFrom(start, Gte), nil
		}
		return l.tokenFrom(start, Gt), nil
	case '!':
		l.advanceByte()
		if l.matchByte('=') {
			return l.tokenFrom(start, Neq), nil
		}
		return Token{}, fmt.Errorf("sql lexer: unexpected ! at %d:%d", start.Line, start.Column)
	case '\'':
		return l.scanString(start)
	case '"':
		return l.scanQuotedIdentifier(start)
	}
	if ch >= '0' && ch <= '9' {
		return l.scanNumber(start), nil
	}
	if isIdentStart(ch) || ch >= utf8.RuneSelf {
		return l.scanIdentifier(start)
	}
	return Token{}, fmt.Errorf("sql lexer: unexpected byte %q at %d:%d", ch, start.Line, start.Column)
}

// skipSpaceAndComments 跳过空白、-- 行注释和 /* */ 块注释。
func (l *Lexer) skipSpaceAndComments() error {
	for {
		for l.offset < len(l.src) {
			r, _ := utf8.DecodeRuneInString(l.src[l.offset:])
			if !unicode.IsSpace(r) {
				break
			}
			l.advanceRune()
		}
		if strings.HasPrefix(l.src[l.offset:], "--") {
			for l.offset < len(l.src) && l.src[l.offset] != '\n' {
				l.advanceByte()
			}
			continue
		}
		if strings.HasPrefix(l.src[l.offset:], "/*") {
			l.advanceByte()
			l.advanceByte()
			for l.offset < len(l.src) && !strings.HasPrefix(l.src[l.offset:], "*/") {
				l.advanceRune()
			}
			if l.offset >= len(l.src) {
				return fmt.Errorf("sql lexer: unterminated block comment")
			}
			l.advanceByte()
			l.advanceByte()
			continue
		}
		return nil
	}
}

// scanIdentifier 扫描普通标识符并匹配关键字。
func (l *Lexer) scanIdentifier(start ast.Position) (Token, error) {
	for l.offset < len(l.src) {
		r, size := utf8.DecodeRuneInString(l.src[l.offset:])
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
			break
		}
		l.advanceN(size, r)
	}
	raw := l.src[start.Offset:l.offset]
	upper := strings.ToUpper(raw)
	if k, ok := keywords[upper]; ok {
		return Token{Kind: k, Raw: raw, Text: upper, Span: ast.Span{Start: start, End: l.position()}}, nil
	}
	return Token{Kind: Identifier, Raw: raw, Text: strings.ToLower(raw), Span: ast.Span{Start: start, End: l.position()}}, nil
}

// scanQuotedIdentifier 扫描双引号标识符并处理重复引号转义。
func (l *Lexer) scanQuotedIdentifier(start ast.Position) (Token, error) {
	l.advanceByte()
	var b strings.Builder
	for l.offset < len(l.src) {
		if l.src[l.offset] == '"' {
			l.advanceByte()
			if l.offset < len(l.src) && l.src[l.offset] == '"' {
				b.WriteByte('"')
				l.advanceByte()
				continue
			}
			raw := l.src[start.Offset:l.offset]
			return Token{Kind: QuotedIdentifier, Raw: raw, Text: b.String(), Span: ast.Span{Start: start, End: l.position()}}, nil
		}
		r, size := utf8.DecodeRuneInString(l.src[l.offset:])
		b.WriteRune(r)
		l.advanceN(size, r)
	}
	return Token{}, fmt.Errorf("sql lexer: unterminated quoted identifier at %d:%d", start.Line, start.Column)
}

// scanString 扫描单引号字符串并处理两个单引号转义。
func (l *Lexer) scanString(start ast.Position) (Token, error) {
	l.advanceByte()
	var b strings.Builder
	for l.offset < len(l.src) {
		if l.src[l.offset] == '\'' {
			l.advanceByte()
			if l.offset < len(l.src) && l.src[l.offset] == '\'' {
				b.WriteByte('\'')
				l.advanceByte()
				continue
			}
			raw := l.src[start.Offset:l.offset]
			return Token{Kind: String, Raw: raw, Text: b.String(), Span: ast.Span{Start: start, End: l.position()}}, nil
		}
		r, size := utf8.DecodeRuneInString(l.src[l.offset:])
		b.WriteRune(r)
		l.advanceN(size, r)
	}
	return Token{}, fmt.Errorf("sql lexer: unterminated string at %d:%d", start.Line, start.Column)
}

// scanNumber 扫描十进制整数或小数；符号交给 Pratt 一元表达式处理。
func (l *Lexer) scanNumber(start ast.Position) Token {
	for l.offset < len(l.src) && l.src[l.offset] >= '0' && l.src[l.offset] <= '9' {
		l.advanceByte()
	}
	if l.offset < len(l.src) && l.src[l.offset] == '.' {
		l.advanceByte()
		for l.offset < len(l.src) && l.src[l.offset] >= '0' && l.src[l.offset] <= '9' {
			l.advanceByte()
		}
	}
	return l.tokenFrom(start, Number)
}

// single 消费一个单字节 token。
func (l *Lexer) single(kind Kind) Token {
	start := l.position()
	l.advanceByte()
	return l.tokenFrom(start, kind)
}

// tokenFrom 根据起始位置构造已扫描 token。
func (l *Lexer) tokenFrom(start ast.Position, kind Kind) Token {
	raw := l.src[start.Offset:l.offset]
	return Token{Kind: kind, Raw: raw, Text: raw, Span: ast.Span{Start: start, End: l.position()}}
}

// matchByte 在当前字节匹配成功时前进一步。
func (l *Lexer) matchByte(b byte) bool {
	if l.offset < len(l.src) && l.src[l.offset] == b {
		l.advanceByte()
		return true
	}
	return false
}

// position 返回当前源位置快照。
func (l *Lexer) position() ast.Position {
	return ast.Position{Offset: l.offset, Line: l.line, Column: l.column}
}

// advanceByte 前进一步 ASCII/单字节位置并维护行列。
func (l *Lexer) advanceByte() {
	if l.offset >= len(l.src) {
		return
	}
	if l.src[l.offset] == '\n' {
		l.line++
		l.column = 1
	} else {
		l.column++
	}
	l.offset++
}

// advanceRune 前进一步 UTF-8 rune。
func (l *Lexer) advanceRune() {
	r, size := utf8.DecodeRuneInString(l.src[l.offset:])
	l.advanceN(size, r)
}

// advanceN 按给定 rune 字节宽度更新游标。
func (l *Lexer) advanceN(size int, r rune) {
	l.offset += size
	if r == '\n' {
		l.line++
		l.column = 1
	} else {
		l.column++
	}
}

// isIdentStart 判断 ASCII 字节是否可作为普通标识符首字符。
func isIdentStart(b byte) bool { return b == '_' || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') }

// All 扫描完整 SQL，供 parser 一次性持有有限 token 序列。
func All(src string) ([]Token, error) {
	l := New(src)
	out := make([]Token, 0, 32)
	for {
		t, err := l.Next()
		if err != nil {
			return nil, err
		}
		out = append(out, t)
		if t.Kind == EOF {
			return out, nil
		}
	}
}
