package session

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// CopyExecutor is the statement-layer adapter into the frozen Export/VFS stack.
// It receives the already-bound query and the production prepared query so COPY
// never creates a second parser/binder/planner execution path.
type CopyExecutor interface {
	Run(*query.Context, *binder.BoundCopyTo, *PreparedSQLQuery, string) error
}

// CopyExecutorFunc is a composition-root helper for tests and embedders.
type CopyExecutorFunc func(*query.Context, *binder.BoundCopyTo, *PreparedSQLQuery, string) error

func (f CopyExecutorFunc) Run(q *query.Context, bound *binder.BoundCopyTo, prepared *PreparedSQLQuery, sql string) error {
	if f == nil {
		return fmt.Errorf("session: nil COPY executor")
	}
	return f(q, bound, prepared, sql)
}

type preparedCopyStatement struct {
	executor CopyExecutor
	bound    *binder.BoundCopyTo
	query    *PreparedSQLQuery
	sql      string
}

func (*preparedCopyStatement) Output() []planner.Column { return nil }

func (p *preparedCopyStatement) Run(q *query.Context, sink pipeline.Sink) error {
	if p == nil || p.executor == nil || p.bound == nil || p.query == nil || q == nil || sink == nil {
		return fmt.Errorf("session: COPY statement is uninitialized")
	}
	if err := p.executor.Run(q, p.bound, p.query, p.sql); err != nil {
		return err
	}
	return sink.Finalize(q.Context())
}

// ensureFrozenSourceSnapshot makes the empty source set explicit. Queries with
// real scans are frozen by bindPhysicalSourceSnapshot; no-source queries still
// need an immutable identity before Export can start.
func ensureFrozenSourceSnapshot(q *query.Context) error {
	if q == nil {
		return fmt.Errorf("session: COPY source snapshot requires query")
	}
	if _, ok := q.SourceSnapshot(); ok {
		return nil
	}
	sum := sha256.Sum256([]byte("fssql:stable-empty-source-set:v1"))
	return q.BindSourceSnapshot(query.SourceSnapshot{Fingerprint: hex.EncodeToString(sum[:])})
}
