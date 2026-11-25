package gosql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/session"
	"github.com/steamwo/fssql/internal/sql/parser"
)

func (db *DB) prepareWithArgs(ctx context.Context, sqlText string, args []any) (*query.Context, session.PreparedStatement, []Column, error) {
	if len(args) == 0 {
		return db.prepare(ctx, sqlText)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	template, err := parser.Compile(sqlText)
	if err != nil {
		return nil, nil, nil, err
	}
	values := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		entry := &values[i]
		entry.Ordinal = i + 1
		if named, ok := arg.(sql.NamedArg); ok {
			entry.Name, arg = named.Name, named.Value
		}
		entry.Value, err = driver.DefaultParameterConverter.ConvertValue(arg)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("gosql: parameter %d: %w", entry.Ordinal, err)
		}
	}
	parsed, err := bindSQLParameters(template, values)
	if err != nil {
		return nil, nil, nil, err
	}
	return db.prepareParsed(ctx, sqlText, parsed)
}
