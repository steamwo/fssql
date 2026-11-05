package parser

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/lexer"
)

func (p *Parser) parseTableSample(input ast.TableExpr) (ast.TableExpr, error) {
	if !p.acceptWord("tablesample") {
		return input, nil
	}
	return p.parseSample(input)
}

func (p *Parser) parseUsingSample(input ast.TableExpr) (ast.TableExpr, error) {
	if p.peek().Kind != lexer.KeywordUsing || !p.peekNWord(1, "sample") {
		return input, nil
	}
	p.pos += 2
	sampled, err := p.parseSample(input)
	if err != nil {
		return nil, err
	}
	sampled.(*ast.SampleTableExpr).Using = true
	return sampled, nil
}

func (p *Parser) parseSample(input ast.TableExpr) (ast.TableExpr, error) {
	method := ""
	methodFirst := p.peekWord("reservoir") || p.peekWord("bernoulli") || p.peekWord("system")
	if methodFirst {
		method = strings.ToLower(p.peek().Text)
		p.pos++
		if err := p.expect(lexer.LParen, "("); err != nil {
			return nil, err
		}
	}
	amountToken := p.peek()
	if amountToken.Kind != lexer.Number {
		return nil, p.errorf("sample amount must be numeric")
	}
	p.pos++
	percent := p.acceptWord("percent") || p.accept(lexer.Percent)
	if !percent {
		p.acceptWord("rows")
	}
	var amount uint64
	if percent {
		value, err := strconv.ParseFloat(amountToken.Raw, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 100 {
			return nil, p.errorf("sample percentage must be in [0,100]")
		}
		amount = math.Float64bits(value)
	} else {
		value, err := strconv.ParseUint(amountToken.Raw, 10, 64)
		if err != nil {
			return nil, p.errorf("sample row count must be a non-negative integer")
		}
		amount = value
	}
	seeded, seed := false, uint64(0)
	if !methodFirst && p.accept(lexer.LParen) {
		if !p.peekWord("reservoir") && !p.peekWord("bernoulli") && !p.peekWord("system") {
			return nil, p.errorf("expected reservoir, bernoulli or system sampling method")
		}
		method = strings.ToLower(p.peek().Text)
		p.pos++
		methodFirst = true
	}
	if methodFirst {
		if p.accept(lexer.Comma) {
			var err error
			seed, err = p.parseSampleSeed()
			if err != nil {
				return nil, err
			}
			seeded = true
		}
		if err := p.expect(lexer.RParen, ")"); err != nil {
			return nil, err
		}
	}
	if p.acceptWord("repeatable") {
		if seeded {
			return nil, p.errorf("sample seed specified twice")
		}
		if err := p.expect(lexer.LParen, "("); err != nil {
			return nil, err
		}
		var err error
		seed, err = p.parseSampleSeed()
		if err != nil {
			return nil, err
		}
		seeded = true
		if err := p.expect(lexer.RParen, ")"); err != nil {
			return nil, err
		}
	}
	if method == "" {
		if percent {
			method = "system"
		} else {
			method = "reservoir"
		}
	}
	code := "r"
	if percent {
		switch method {
		case "reservoir":
			code = "p"
		case "bernoulli":
			code = "b"
		case "system":
			code = "s"
		}
	} else if method != "reservoir" {
		return nil, p.errorf("%s sampling requires a percentage", method)
	}
	seedCode := "n"
	if seeded {
		seedCode = "s"
	}
	return &ast.SampleTableExpr{Span: ast.Span{Start: input.NodeSpan().Start, End: p.previous().Span.End}, Input: input, Spec: fmt.Sprintf("sample:%s:%x:%s:%x", code, amount, seedCode, seed)}, nil
}

func (p *Parser) parseSampleSeed() (uint64, error) {
	t := p.peek()
	if t.Kind != lexer.Number {
		return 0, p.errorf("sample seed must be a non-negative integer")
	}
	p.pos++
	value, err := strconv.ParseUint(t.Raw, 10, 64)
	if err != nil {
		return 0, p.errorf("sample seed must be a non-negative integer")
	}
	return value, nil
}
