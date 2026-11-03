package session

import (
	"errors"

	"github.com/steamwo/fssql/internal/query"
)

type preparedCopyAPIExecutor interface {
	RunPrepared(*query.Context, *PreparedSQLQuery, CopyRequest, string) (CopyExportResult, error)
}

// CopyToPrepared executes an already-bound SELECT through the same Export/VFS/commit path as SQL COPY.
func (e *StableStatementEngine) CopyToPrepared(q *query.Context, prepared PreparedStatement, request CopyRequest, queryText string) (CopyExportResult, error) {
	if e == nil || e.copy == nil {
		return CopyExportResult{}, errors.New("session: COPY executor is not configured")
	}
	selectPrepared, ok := prepared.(*PreparedSQLQuery)
	if !ok || selectPrepared == nil {
		return CopyExportResult{}, errors.New("session: CopyTo requires a SELECT query")
	}
	if err := ensureFrozenSourceSnapshot(q); err != nil {
		return CopyExportResult{}, err
	}
	runner, ok := e.copy.(preparedCopyAPIExecutor)
	if !ok {
		return CopyExportResult{}, errors.New("session: configured COPY executor does not implement typed CopyTo")
	}
	return runner.RunPrepared(q, selectPrepared, request, queryText)
}
