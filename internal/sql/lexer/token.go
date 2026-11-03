package lexer

import "github.com/steamwo/fssql/internal/sql/ast"

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
	Concat // appended to preserve existing token numeric identities
)

type Token struct {
	Kind Kind
	Raw string
	Text string
	Span ast.Span
}
