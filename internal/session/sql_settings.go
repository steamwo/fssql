package session

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func (e *StableSQLEngine) sessionSnapshot() (string, int) {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	zone, workers := e.zoneName, e.sessionWorkers
	if zone == "" {
		zone = "UTC"
	}
	if workers == 0 {
		workers = e.config.Workers
	}
	return zone, workers
}

type preparedSetting struct {
	engine      *StableSQLEngine
	name, value string
	reset       bool
}

func (*preparedSetting) Output() []planner.Column { return nil }
func (p *preparedSetting) Run(q *query.Context, sink pipeline.Sink) error {
	if err := q.Context().Err(); err != nil {
		return err
	}
	p.engine.stateMu.Lock()
	switch p.name {
	case "timezone":
		p.engine.zoneName = p.value
		if p.reset {
			p.engine.zoneName = ""
		}
	case "threads":
		workers, _ := strconv.Atoi(p.value)
		if p.reset {
			workers = 0
		}
		p.engine.sessionWorkers = workers
	}
	p.engine.stateMu.Unlock()
	return sink.Finalize(q.Context())
}
func (e *StableStatementEngine) prepareSetting(stmt *ast.SetStmt) (PreparedStatement, error) {
	name := strings.ToLower(stmt.Name.Name)
	if name == "max_workers" {
		name = "threads"
	}
	if name != "threads" && name != "timezone" {
		return nil, fmt.Errorf("unsupported session setting %q", stmt.Name.Name)
	}
	value := ""
	if !stmt.Reset {
		lit, ok := stmt.Value.(*ast.LiteralExpr)
		if !ok {
			return nil, fmt.Errorf("SET requires a literal or bound parameter")
		}
		value = lit.Value
		switch name {
		case "threads":
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 || n > 1024 {
				return nil, fmt.Errorf("threads must be between 1 and 1024")
			}
		case "timezone":
			if _, err := time.LoadLocation(value); err != nil {
				return nil, fmt.Errorf("invalid TimeZone %q", value)
			}
		}
	}
	return &preparedSetting{engine: e.selects, name: name, value: value, reset: stmt.Reset}, nil
}

func markQueryTimeZone(q *binder.BoundQuery, zone string) {
	seen := map[*binder.BoundQuery]bool{}
	var query func(*binder.BoundQuery)
	var expr func(*binder.BoundExpression)
	var relation func(*binder.BoundRelation)
	expr = func(e *binder.BoundExpression) {
		if e == nil {
			return
		}
		e.TimeZoneName = zone
		for _, c := range e.Children {
			expr(c)
		}
		query(e.Subquery)
	}
	relation = func(r *binder.BoundRelation) {
		if r == nil {
			return
		}
		expr(r.Condition)
		for _, e := range r.Projection {
			expr(e)
		}
		query(r.CTEQuery)
		for _, c := range r.Children {
			relation(c)
		}
	}
	query = func(q *binder.BoundQuery) {
		if q == nil || seen[q] {
			return
		}
		seen[q] = true
		relation(q.Source)
		for _, p := range q.Projection {
			expr(p.Expression)
		}
		expr(q.Where)
		expr(q.Having)
		for _, e := range q.GroupBy {
			expr(e)
		}
		for _, o := range q.OrderBy {
			expr(o.Expression)
		}
		expr(q.Limit)
		expr(q.Offset)
	}
	query(q)
}
