package session

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/optimizer"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// prepareBoundQuery reuses the production planner/optimizer/pipeline preparation
// for a query that was already bound by StatementBinder (for example COPY).
// It does not reparse or rebind and therefore preserves one statement snapshot.
func (e *StableSQLEngine) prepareBoundQuery(ctx context.Context, q *query.Context, bound *binder.BoundQuery, scans *RuntimeScanSourceFactory, relations planner.RelationPlanResolver) (*PreparedSQLQuery, error) {
	if e == nil || bound == nil || scans == nil || q == nil {
		return nil, fmt.Errorf("session: bound query preparation is uninitialized")
	}
	zoneName,workers:=e.sessionSnapshot()
 markQueryTimeZone(bound,zoneName)
 queryPlanner := planner.NewQueryPlanner(e.functions, relations)
	opt := &optimizer.Optimizer{Statistics: e.config.Statistics}
	rewritten, subqueries, err := rewriteScalarSubqueries(ctx, queryPlanner, opt, bound)
	if err != nil {
		return nil, err
	}
	logical, err := queryPlanner.PlanStable(ctx, rewritten)
	if err != nil {
		return nil, err
	}
	physical, err := opt.Optimize(ctx, logical)
	if err != nil {
		return nil, err
	}
	plans := make([]*planner.PhysicalPlan, 0, 1+len(subqueries))
	plans = append(plans, physical)
	for _, subquery := range subqueries {
		plans = append(plans, subquery.physical)
	}
	if err := bindPhysicalSourceSnapshot(q, plans...); err != nil {
		return nil, err
	}
	return &PreparedSQLQuery{engine: e, physical: physical, scans: scans, subqueries: subqueries,zoneName:zoneName,workers:workers}, nil
}
