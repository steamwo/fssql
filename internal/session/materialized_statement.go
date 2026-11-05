package session

import (
	"context"
	"errors"
	"fmt"
	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/sql/ast"
)

// MaterializeExecutor returns complete immutable storage plus rollback cleanup.
// The composition root installs it before exposing the engine to callers.
type MaterializeExecutor func(*query.Context, PreparedStatement) (catalog.ExternalTable, func() error, error)

func (e *StableStatementEngine) SetMaterializer(fn MaterializeExecutor) { e.materialize = fn }

func (e *StableStatementEngine) prepareCreateTableAs(ctx context.Context, q *query.Context, sql string, stmt *ast.CreateTableAsStmt) (PreparedStatement, error) {
	if e.materialize == nil {
		return nil, errors.New("session: CTAS materialization is not configured")
	}
	name, err := catalogObjectName(stmt.Name)
	if err != nil {
		return nil, err
	}
	prepared, err := e.selects.prepareSelect(ctx, q, sql, stmt.Query)
	if err != nil {
		return nil, err
	}
	return &preparedCreateTableAs{engine: e, name: name, input: prepared}, nil
}

type preparedCreateTableAs struct {
	engine *StableStatementEngine
	name   catalog.ObjectName
	input  PreparedStatement
}

func (*preparedCreateTableAs) Output() []planner.Column { return nil }
func (p *preparedCreateTableAs) Run(q *query.Context, sink pipeline.Sink) (err error) {
	tx, err := p.engine.selects.config.Catalog.Begin(q.Context())
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, exists, err := tx.Get(q.Context(), p.name); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("catalog object already exists: %s.%s", p.name.Schema, p.name.Name)
	}
	table, cleanup, err := p.engine.materialize(q, p.input)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed && cleanup != nil {
			err = errors.Join(err, cleanup())
		}
	}()
	table.Name = p.name
	if err = tx.CreateExternalTable(q.Context(), table); err != nil {
		return err
	}
	if err = tx.Commit(q.Context()); err != nil {
		return err
	}
	committed = true
	return sink.Finalize(q.Context())
}
