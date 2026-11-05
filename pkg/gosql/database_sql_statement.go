package gosql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/parser"
)

func (c *sqlConn) PrepareContext(ctx context.Context, sqlText string) (driver.Stmt, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	template, err := parser.Compile(sqlText)
	if err != nil {
		return nil, err
	}
	return &sqlStmt{conn: c, sql: sqlText, template: template}, nil
}

func (c *sqlConn) execTemplate(ctx context.Context, sqlText string, template *parser.Template, args []driver.NamedValue) (driver.Result, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	parsed, err := bindSQLParameters(template, args)
	if err != nil {
		return nil, err
	}
	q, prepared, _, err := c.db.prepareParsed(ctx, sqlText, parsed)
	if err != nil {
		return nil, err
	}
	runErr := prepared.Run(q, discardSink{})
	if err := errors.Join(runErr, q.Close()); err != nil {
		return nil, err
	}
	return sqlResult{}, nil
}

type sqlStmt struct {
	conn     *sqlConn
	sql      string
	template *parser.Template
	mu       sync.Mutex
	closed   bool
}

var (
	_ driver.Stmt             = (*sqlStmt)(nil)
	_ driver.StmtExecContext  = (*sqlStmt)(nil)
	_ driver.StmtQueryContext = (*sqlStmt)(nil)
)

func (s *sqlStmt) Close() error  { s.mu.Lock(); s.closed = true; s.mu.Unlock(); return nil }
func (s *sqlStmt) NumInput() int { return s.template.NumInput() }
func (s *sqlStmt) check() error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return errors.New("gosql: SQL statement is closed")
	}
	return s.conn.check()
}
func (s *sqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), namedSQLValues(args))
}
func (s *sqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), namedSQLValues(args))
}
func (s *sqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.conn.execTemplate(ctx, s.sql, s.template, args)
}
func (s *sqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.conn.queryTemplate(ctx, s.sql, s.template, args)
}
func namedSQLValues(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, value := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
	}
	return out
}

func bindSQLParameters(template *parser.Template, args []driver.NamedValue) (ast.Statement, error) {
	values := make([]ast.Expr, len(args))
	names := make([]string, len(args))
	for i, arg := range args {
		value, err := sqlParameterValue(arg.Value)
		if err != nil {
			return nil, fmt.Errorf("gosql: parameter %d: %w", i+1, err)
		}
		values[i], names[i] = value, arg.Name
	}
	return template.Bind(values, names)
}

func sqlParameterValue(value any) (ast.Expr, error) {
	literal := func(kind ast.LiteralKind, value string) ast.Expr { return &ast.LiteralExpr{Kind: kind, Value: value} }
	cast := func(name string, value ast.Expr) ast.Expr {
		return &ast.CallExpr{Function: ast.QualifiedName{Parts: []ast.Identifier{{Name: "__fssql_cast_" + name}}}, Args: []ast.Expr{value}}
	}
	switch v := value.(type) {
	case nil:
		return literal(ast.LiteralNull, "NULL"), nil
	case bool:
		return literal(ast.LiteralBool, strconv.FormatBool(v)), nil
	case int64:
		return literal(ast.LiteralInteger, strconv.FormatInt(v, 10)), nil
	case float64:
		return cast("double", literal(ast.LiteralString, strconv.FormatFloat(v, 'g', -1, 64))), nil
	case string:
		return literal(ast.LiteralString, v), nil
	case []byte:
		if v == nil {
			return literal(ast.LiteralNull, "NULL"), nil
		}
		return cast("blob", literal(ast.LiteralString, string(v))), nil
	case time.Time:
		utc := v.UTC()
		if !time.Unix(0, utc.UnixNano()).UTC().Equal(utc) {
			return nil, errors.New("timestamp parameter is outside TIMESTAMP_NS range")
		}
		return cast("timestamp_ns", literal(ast.LiteralString, utc.Format("2006-01-02 15:04:05.999999999"))), nil
	default:
		return nil, fmt.Errorf("unsupported value type %T", value)
	}
}
