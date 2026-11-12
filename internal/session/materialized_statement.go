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
	if stmt.Temporary {name.Schema="temp"}else if name.Schema==""{name.Schema="main"}
	prepared, err := e.selects.prepareSelect(ctx, q, sql, stmt.Query)
	if err != nil {
		return nil, err
	}
	return &preparedCreateTableAs{engine: e, name: name, input: prepared,temporary:stmt.Temporary||name.Schema=="temp"}, nil
}

type preparedCreateTableAs struct {
	temporary bool
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
	input:=p.input
	if p.temporary{input=&scopedMaterializedInput{PreparedStatement:input,sessionID:q.SessionID}}
	table, cleanup, err := p.engine.materialize(q, input)
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
	if p.temporary {creator,ok:=tx.(interface{CreateTemporaryExternalTable(context.Context,catalog.ExternalTable)error});if !ok{return fmt.Errorf("session catalog does not support temporary tables")};err=creator.CreateTemporaryExternalTable(q.Context(),table)}else{err=tx.CreateExternalTable(q.Context(),table)}
	if err != nil {
		return err
	}
	if err = tx.Commit(q.Context()); err != nil {
		return err
	}
	committed = true
	if p.temporary&&cleanup!=nil {p.engine.addSessionCleanup(cleanup)}
	return sink.Finalize(q.Context())
}

type scopedMaterializedInput struct{PreparedStatement;sessionID string}
func(s *scopedMaterializedInput)MaterializedSession()string{return s.sessionID}
