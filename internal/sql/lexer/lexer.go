package lexer

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/steamwo/fssql/internal/sql/ast"
)

var keywords = map[string]Kind{"UNION": KeywordUnion, "SELECT": KeywordSelect, "FROM": KeywordFrom, "WHERE": KeywordWhere, "GROUP": KeywordGroup, "BY": KeywordBy, "HAVING": KeywordHaving, "ORDER": KeywordOrder, "LIMIT": KeywordLimit, "OFFSET": KeywordOffset, "DISTINCT": KeywordDistinct, "AS": KeywordAs, "WITH": KeywordWith, "JOIN": KeywordJoin, "INNER": KeywordInner, "LEFT": KeywordLeft, "RIGHT": KeywordRight, "FULL": KeywordFull, "CROSS": KeywordCross, "SEMI": KeywordSemi, "ANTI": KeywordAnti, "OUTER": KeywordOuter, "ON": KeywordOn, "USING": KeywordUsing, "ASC": KeywordAsc, "DESC": KeywordDesc, "NULLS": KeywordNulls, "FIRST": KeywordFirst, "LAST": KeywordLast, "AND": KeywordAnd, "OR": KeywordOr, "NOT": KeywordNot, "IS": KeywordIs, "NULL": KeywordNull, "TRUE": KeywordTrue, "FALSE": KeywordFalse}

type Lexer struct { src string; offset int; line int; column int }
func New(src string) *Lexer { return &Lexer{src: src, line: 1, column: 1} }
func (l *Lexer) Next() (Token,error) {
	if err:=l.skipSpaceAndComments();err!=nil{return Token{},err};start:=l.position();if l.offset>=len(l.src){return Token{Kind:EOF,Span:ast.Span{Start:start,End:start}},nil};ch:=l.src[l.offset]
	if ch=='?'||ch=='$'{return l.scanParameter(start)}
	switch ch {case '{':return l.single(LBrace),nil;case '}':return l.single(RBrace),nil;case ':':return l.single(Colon),nil;case ',':return l.single(Comma),nil;case '.':return l.single(Dot),nil;case '(':return l.single(LParen),nil;case ')':return l.single(RParen),nil;case '*':return l.single(Star),nil;case '+':return l.single(Plus),nil;case '-':return l.single(Minus),nil;case '/':return l.single(Slash),nil;case '%':return l.single(Percent),nil;case '=':return l.single(Eq),nil;case ';':return l.single(Semicolon),nil;case '|':l.advanceByte();if l.matchByte('|'){return l.tokenFrom(start,Concat),nil};return Token{},fmt.Errorf("sql lexer: unexpected | at %d:%d",start.Line,start.Column);case '<':l.advanceByte();if l.matchByte('='){return l.tokenFrom(start,Lte),nil};if l.matchByte('>'){return l.tokenFrom(start,Neq),nil};return l.tokenFrom(start,Lt),nil;case '>':l.advanceByte();if l.matchByte('='){return l.tokenFrom(start,Gte),nil};return l.tokenFrom(start,Gt),nil;case '!':l.advanceByte();if l.matchByte('='){return l.tokenFrom(start,Neq),nil};return Token{},fmt.Errorf("sql lexer: unexpected ! at %d:%d",start.Line,start.Column);case '\'':return l.scanString(start);case '"':return l.scanQuotedIdentifier(start)}
	if ch>='0'&&ch<='9'{return l.scanNumber(start),nil};if isIdentStart(ch)||ch>=utf8.RuneSelf{return l.scanIdentifier(start)};return Token{},fmt.Errorf("sql lexer: unexpected byte %q at %d:%d",ch,start.Line,start.Column)
}
func (l *Lexer) skipSpaceAndComments() error { for { for l.offset<len(l.src){r,_:=utf8.DecodeRuneInString(l.src[l.offset:]);if !unicode.IsSpace(r){break};l.advanceRune()};if strings.HasPrefix(l.src[l.offset:],"--"){for l.offset<len(l.src)&&l.src[l.offset]!='\n'{l.advanceByte()};continue};if strings.HasPrefix(l.src[l.offset:],"/*"){l.advanceByte();l.advanceByte();for l.offset<len(l.src)&&!strings.HasPrefix(l.src[l.offset:],"*/"){l.advanceRune()};if l.offset>=len(l.src){return fmt.Errorf("sql lexer: unterminated block comment")};l.advanceByte();l.advanceByte();continue};return nil } }
func (l *Lexer) scanIdentifier(start ast.Position)(Token,error){for l.offset<len(l.src){r,size:=utf8.DecodeRuneInString(l.src[l.offset:]);if !(unicode.IsLetter(r)||unicode.IsDigit(r)||r=='_'){break};l.advanceN(size,r)};raw:=l.src[start.Offset:l.offset];upper:=strings.ToUpper(raw);if k,ok:=keywords[upper];ok{return Token{Kind:k,Raw:raw,Text:upper,Span:ast.Span{Start:start,End:l.position()}},nil};return Token{Kind:Identifier,Raw:raw,Text:strings.ToLower(raw),Span:ast.Span{Start:start,End:l.position()}},nil}
func (l *Lexer) scanQuotedIdentifier(start ast.Position)(Token,error){l.advanceByte();var b strings.Builder;for l.offset<len(l.src){if l.src[l.offset]=='"'{l.advanceByte();if l.offset<len(l.src)&&l.src[l.offset]=='"'{b.WriteByte('"');l.advanceByte();continue};raw:=l.src[start.Offset:l.offset];return Token{Kind:QuotedIdentifier,Raw:raw,Text:b.String(),Span:ast.Span{Start:start,End:l.position()}},nil};r,size:=utf8.DecodeRuneInString(l.src[l.offset:]);b.WriteRune(r);l.advanceN(size,r)};return Token{},fmt.Errorf("sql lexer: unterminated quoted identifier at %d:%d",start.Line,start.Column)}
func (l *Lexer) scanString(start ast.Position)(Token,error){l.advanceByte();var b strings.Builder;for l.offset<len(l.src){if l.src[l.offset]=='\''{l.advanceByte();if l.offset<len(l.src)&&l.src[l.offset]=='\''{b.WriteByte('\'');l.advanceByte();continue};raw:=l.src[start.Offset:l.offset];return Token{Kind:String,Raw:raw,Text:b.String(),Span:ast.Span{Start:start,End:l.position()}},nil};r,size:=utf8.DecodeRuneInString(l.src[l.offset:]);b.WriteRune(r);l.advanceN(size,r)};return Token{},fmt.Errorf("sql lexer: unterminated string at %d:%d",start.Line,start.Column)}
func (l *Lexer) scanNumber(start ast.Position)Token{for l.offset<len(l.src)&&l.src[l.offset]>='0'&&l.src[l.offset]<='9'{l.advanceByte()};if l.offset<len(l.src)&&l.src[l.offset]=='.'{l.advanceByte();for l.offset<len(l.src)&&l.src[l.offset]>='0'&&l.src[l.offset]<='9'{l.advanceByte()}};return l.tokenFrom(start,Number)}
func (l *Lexer) single(kind Kind)Token{start:=l.position();l.advanceByte();return l.tokenFrom(start,kind)}
func (l *Lexer) tokenFrom(start ast.Position,kind Kind)Token{raw:=l.src[start.Offset:l.offset];return Token{Kind:kind,Raw:raw,Text:raw,Span:ast.Span{Start:start,End:l.position()}}}
func (l *Lexer) matchByte(b byte)bool{if l.offset<len(l.src)&&l.src[l.offset]==b{l.advanceByte();return true};return false}
func (l *Lexer) position()ast.Position{return ast.Position{Offset:l.offset,Line:l.line,Column:l.column}}
func (l *Lexer) advanceByte(){if l.offset>=len(l.src){return};if l.src[l.offset]=='\n'{l.line++;l.column=1}else{l.column++};l.offset++}
func (l *Lexer) advanceRune(){r,size:=utf8.DecodeRuneInString(l.src[l.offset:]);l.advanceN(size,r)}
func (l *Lexer) advanceN(size int,r rune){l.offset+=size;if r=='\n'{l.line++;l.column=1}else{l.column++}}
func isIdentStart(b byte)bool{return b=='_'||(b>='A'&&b<='Z')||(b>='a'&&b<='z')}
func All(src string)([]Token,error){l:=New(src);out:=make([]Token,0,32);for{t,err:=l.Next();if err!=nil{return nil,err};out=append(out,t);if t.Kind==EOF{return out,nil}}}
