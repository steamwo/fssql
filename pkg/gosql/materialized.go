package gosql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/session"
	"github.com/steamwo/fssql/internal/sql/binder"
)

const materializedFunctionID = function.ID(int(function.GlobID) + maxPublicTableFunctions + 1)
const materializedFunctionName = "fssql_materialized"

var materializedSequence atomic.Uint64

type materializedRegistry struct {
	mu       sync.Mutex
	items    map[string]*Materialized
	limit    int
	closed   bool
	building map[string]*query.Context
}

// Materialized owns a typed, bounded snapshot independent of the creating query.
// Close prevents new bindings; already bound queries retain their reader leases.
// All snapshot storage is released when the last bound query closes.
type Materialized struct {
	mu       sync.Mutex
	registry *materializedRegistry
	token    string
	state    *typedSourceState
	columns  []Column
	refs     int
	closed   bool
	cleaned  bool
}

// SQL returns a SELECT over this snapshot. It is valid only on the creating DB.
func (m *Materialized) SQL() string {
	return "SELECT * FROM " + materializedFunctionName + "('" + m.token + "')"
}

// Materialize consumes querySQL once, publishing a snapshot only after success.
// Close the returned object (or DB) to release retained memory and spill files.
func (db *DB) Materialize(ctx context.Context, querySQL string) (*Materialized, error) {
	q, prepared, columns, err := db.prepare(ctx, querySQL)
	if err != nil {
		return nil, err
	}
	defer q.Close()
	m, runErr := db.materializePrepared(ctx, q, prepared, columns)
	closeErr := q.Close()
	if runErr != nil || closeErr != nil {
		if m != nil {
			closeErr = errors.Join(closeErr, m.Close())
		}
		return nil, errors.Join(runErr, closeErr)
	}
	return m, nil
}

func (db *DB) materializePrepared(ctx context.Context, q *query.Context, prepared session.PreparedStatement, columns []Column) (*Materialized, error) {
	if len(columns) == 0 {
		return nil, errors.New("gosql: Materialize requires a query with columns")
	}
	r := db.materialized
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("gosql: DB is closed")
	}
	if len(r.items)+len(r.building) >= r.limit {
		r.mu.Unlock()
		return nil, errors.New("gosql: materialized object limit exceeded")
	}
	token := fmt.Sprintf("snapshot-%d", materializedSequence.Add(1))
	if r.building == nil {
		r.building = make(map[string]*query.Context)
	}
	r.building[token] = q
	r.mu.Unlock()
	defer func() { r.mu.Lock(); delete(r.building, token); r.mu.Unlock() }()
	temp, err := db.temporary.NewQueryScope("default")
	if err != nil {
		return nil, err
	}
	owner := query.NewWithTemp(context.Background(), db.memory, temp)
	state := &typedSourceState{q: owner, buffers: db.buffers, memoryBudget: db.materializedMemoryBudget()}
	for _, c := range prepared.Output() {
		state.definition.columns = append(state.definition.columns, binder.RelationColumn{Name: c.Name, Type: c.Type, Nullable: c.Nullable})
	}
	if err = owner.AddCleanup(state.Close); err != nil {
		_ = owner.Close()
		return nil, err
	}
	m := &Materialized{registry: r, token: token, state: state, columns: columns}
	err = prepared.Run(q, &materializedSink{state: state})
	err = errors.Join(err, ctx.Err())
	if err != nil {
		return nil, errors.Join(err, owner.Close())
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.Join(errors.New("gosql: DB closed during materialization"), owner.Close())
	}
	r.items[m.token] = m
	r.mu.Unlock()
	return m, nil
}

func (db *DB) materializedMemoryBudget() int64 { return db.spillMemoryBudget }

type materializedSink struct{ state *typedSourceState }

func (*materializedSink) Finalize(context.Context) error { return nil }
func (s *materializedSink) Consume(ctx context.Context, c *vector.DataChunk) error {
	b, err := vector.SerializeChunk(c, s.state.q.Memory, s.state.buffers)
	if errors.Is(err, memory.ErrLimitExceeded) && len(s.state.resident) > 0 {
		if spillErr := s.state.spillResident(ctx); spillErr != nil {
			return errors.Join(err, spillErr)
		}
		b, err = vector.SerializeChunk(c, s.state.q.Memory, s.state.buffers)
	}
	if err != nil {
		return err
	}
	if s.state.store != nil || s.state.residentBytes+int64(b.Len()) > s.state.memoryBudget {
		defer b.Release()
		if err = s.state.spillResident(ctx); err != nil {
			return err
		}
		return s.state.spillBuffer(ctx, b)
	}
	s.state.resident = append(s.state.resident, b)
	s.state.residentBytes += int64(b.Len())
	return nil
}

func (m *Materialized) Close() error {
	if m == nil {
		return nil
	}
	m.registry.mu.Lock()
	delete(m.registry.items, m.token)
	m.registry.mu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return m.cleanupLocked()
}
func (m *Materialized) cleanupLocked() error {
	if !m.closed || m.refs != 0 || m.cleaned {
		return nil
	}
	m.cleaned = true
	return m.state.q.Close()
}
func (m *Materialized) release() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refs--
	return m.cleanupLocked()
}
func (r *materializedRegistry) close() error {
	r.mu.Lock()
	r.closed = true
	for _, q := range r.building {
		q.Cancel()
	}
	items := r.items
	r.items = make(map[string]*Materialized)
	r.mu.Unlock()
	var errs []error
	for _, m := range items {
		errs = append(errs, m.Close())
	}
	return errors.Join(errs...)
}

func (r *materializedRegistry) adapters(q *query.Context) (session.TableBindAdapter, session.ScanOpenAdapter) {
	var mu sync.Mutex
	pinned := make(map[string]*Materialized)
	bind := session.TableBindAdapter{FunctionID: materializedFunctionID, Bind: func(ctx context.Context, token string) (binder.ResolvedRelation, error) {
		if err := ctx.Err(); err != nil {
			return binder.ResolvedRelation{}, err
		}
		mu.Lock()
		defer mu.Unlock()
		m := pinned[token]
		if m == nil {
			r.mu.Lock()
			m = r.items[token]
			if m == nil || r.closed {
				r.mu.Unlock()
				return binder.ResolvedRelation{}, errors.New("gosql: materialized snapshot is closed or unknown")
			}
			m.mu.Lock()
			m.refs++
			m.mu.Unlock()
			r.mu.Unlock()
			if err := q.AddCleanup(m.release); err != nil {
				_ = m.release()
				return binder.ResolvedRelation{}, err
			}
			pinned[token] = m
		}
		return binder.ResolvedRelation{ID: "materialized:" + token, Columns: m.state.definition.columns, Fingerprint: stats.DatasetFingerprint(token)}, nil
	}}
	scan := session.ScanOpenAdapter{FunctionID: materializedFunctionID, Open: func(ctx context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		mu.Lock()
		m := pinned[spec.Source]
		mu.Unlock()
		if m == nil || string(spec.Fingerprint) != m.token {
			return nil, errors.New("gosql: materialized source snapshot mismatch")
		}
		return &materializedReader{snapshot: m}, nil
	}}
	return bind, scan
}

type materializedReader struct {
	mu       sync.Mutex
	snapshot *Materialized
	index    int
	closed   bool
}

func (s *materializedReader) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.closed {
		return nil, false, nil
	}
	if s.index >= s.snapshot.state.materializedCount() {
		return nil, false, nil
	}
	c, err := s.snapshot.state.materializedChunk(ctx, s.index)
	if err != nil {
		return nil, false, err
	}
	s.index++
	return c, true, nil
}
func (s *materializedReader) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
