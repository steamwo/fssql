package lexer

import "github.com/steamwo/fssql/internal/sql/ast"

// Kind 是 SQL 词法 token 类型。
type Kind uint16

const (
	EOF Kind = iota
	Identifier
	QuotedIdentifier
	String
	Number
	Comma
	Dot
	LParen
	RParen
	Star
	Plus
	Minus
	Slash
	Percent
	Eq
	Neq
	Lt
	Lte
	Gt
	Gte
	Semicolon
	KeywordSelect
	KeywordFrom
	KeywordWhere
	KeywordGroup
	KeywordBy
	KeywordHaving
	KeywordOrder
	KeywordLimit
	KeywordOffset
	KeywordDistinct
	KeywordAs
	KeywordWith
	KeywordJoin
	KeywordInner
	KeywordLeft
	KeywordRight
	KeywordFull
	KeywordCross
	KeywordSemi
	KeywordAnti
	KeywordOuter
	KeywordOn
	KeywordUsing
	KeywordAsc
	KeywordDesc
	KeywordNulls
	KeywordFirst
	KeywordLast
	KeywordAnd
	KeywordOr
	KeywordNot
	KeywordIs
	KeywordNull
	KeywordTrue
	KeywordFalse
)

// Token 保存 token 原文、规范化文本和源位置。
type Token struct {
	Kind Kind     // Kind 是 token 分类。
	Raw  string   // Raw 是源 SQL 中的原始文本。
	Text string   // Text 是去引号或规范化后的文本。
	Span ast.Span // Span 是 token 源位置。
}
