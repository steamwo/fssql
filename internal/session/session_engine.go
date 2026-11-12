package session

import (
	"errors"
	"github.com/steamwo/fssql/internal/catalog"
)

// ForkSession shares immutable engine dependencies and persistent metadata while
// giving each connection its own TEMP namespace and session settings.
func (e *StableStatementEngine) ForkSession() (*StableStatementEngine, error) {
	config := e.selects.config
	base := config.Catalog
	limit := 4096
	if overlay, ok := base.(*catalog.SessionCatalog); ok {
		base = overlay.Base
		limit = overlay.Limit
	}
	local, err := catalog.NewSessionCatalog(base, limit)
	if err != nil {
		return nil, err
	}
	config.Catalog = local
	selects, err := NewStableSQLEngine(config)
	if err != nil {
		return nil, err
	}
	return &StableStatementEngine{selects: selects, control: e.control, copy: e.copy, materialize: e.materialize, sessionID: nextSessionID()}, nil
}
func (e *StableStatementEngine) addSessionCleanup(fn func() error) {
	e.sessionMu.Lock()
	if e.sessionClosed {
		e.sessionMu.Unlock()
		_ = fn()
		return
	}
	e.sessionCleanups = append(e.sessionCleanups, fn)
	e.sessionMu.Unlock()
}
func (e *StableStatementEngine) CloseSession() error {
	e.sessionMu.Lock()
	if e.sessionClosed {
		e.sessionMu.Unlock()
		return nil
	}
	e.sessionClosed = true
	cleanups := e.sessionCleanups
	e.sessionCleanups = nil
	e.sessionMu.Unlock()
	var err error
	for i := len(cleanups) - 1; i >= 0; i-- {
		err = errors.Join(err, cleanups[i]())
	}
	return err
}
