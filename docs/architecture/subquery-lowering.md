# Subquery lowering

The planner follows DuckDB's separation of name binding, subquery decorrelation
and ordinary relational execution. It reuses fssql's existing spillable joins,
sorting and aggregates instead of adding per-outer-row query execution.

References: [DuckDB's decorrelation design](https://duckdb.org/2023/05/26/correlated-subqueries-in-sql)
and [aggregate DISTINCT semantics](https://duckdb.org/docs/stable/sql/functions/aggregates).
This is an incremental implementation, not full DuckDB decorrelation coverage.

## Membership marks

`IN (SELECT ...)` binds one output column to a common comparison type with its
left operand. A deduplicated LEFT JOIN records whether it matched. A second
aggregate records total and non-NULL row counts, globally or per correlation key.
The result is a nullable boolean:

| Match | Right set | Result |
| --- | --- | --- |
| yes | any | true |
| no | empty | false, including a NULL left operand |
| no | nonempty, left is NULL or right contains NULL | NULL |
| no | nonempty, neither condition above | false |

`NOT IN` applies SQL NOT to this mark, rather than treating it as a plain anti
join. The right output is materialized once in query-owned temporary storage.
Ready materializers bypass and close their unused dependency DAGs, preserving
volatile expression semantics and bounded cleanup.

## Equality correlation

Binding records outer column indexes and query depth. Local columns shadow outer
columns. AND-connected inner/outer equalities become materialized join keys;
ordinary inner predicates stay inside the child plan.

- EXISTS uses unique correlation keys and a LEFT JOIN presence test. Grouped
  HAVING is retained. A global aggregate without HAVING always produces a row.
- Scalar aggregates group by correlation keys and LEFT JOIN their results.
  Missing COUNT groups restore zero; other supported aggregates restore NULL.
- Scalar lookups group values by correlation keys and retain a row count.
  More than one row raises a scalar-cardinality error. Scalar DISTINCT is applied
  before that count. Grouped scalar queries similarly count the retained grouped
  results, including HAVING/DISTINCT, and return NULL for absent groups.
- Correlated IN uses per-key marks and NULL statistics. Grouped right queries
  retain their groups/HAVING; a single global aggregate uses scalar semantics,
  including its empty-input result.

## Parameter-domain correlation

Queries outside the equality fast path use a duplicate-free domain of their
outer parameters. The outer input is materialized once; domain values become
ordinary columns beside the inner source. Non-equality/OR predicates execute as
ordinary filters. Nested expression subqueries carry the domain columns across
query boundaries, including references that skip an intermediate query block.

Global aggregates retain an absent parameter group by combining matched rows
with one synthetic NULL row for each missing group. A presence marker makes
COUNT(*) return zero and masks other aggregate arguments on synthetic rows.
HAVING is evaluated after this completion. Scalar result rows are reduced to a
value and row count; multiple rows still raise a cardinality error. IN uses
per-domain match and NULL counts, including a NULL left operand and empty sets.
Different IN operands with identical correlation parameters share the same
materialized inner values, including volatile expressions.
Results attach to the original outer rows using NULL-safe hash keys, preserving
duplicate outer rows and NULL parameter identities. Resident, spilled and skew
fallback hash comparisons share the same NULL policy.

Constant LIMIT/OFFSET uses ROW_NUMBER partitioned by domain values and ordered
by the child ORDER BY. DISTINCT is evaluated before row numbering. As in DuckDB,
non-constant correlated LIMIT/OFFSET is rejected. Grouped outer query outputs
and HAVING can reference grouping columns from a child subquery; aggregate
arguments may combine local and outer columns.

## Relation propagation and aggregate ownership

Parameter columns now propagate through correlated derived tables/CTEs,
UNION ALL, relation projections and JOIN inputs. Each relation preserves its
original columns as a prefix and appends the domain. Query-level DISTINCT,
aggregation and constant LIMIT/OFFSET continue to operate per parameter set.
JOIN predicates can capture outer parameters or contain expression subqueries.
Domain identity and available equality conjuncts narrow candidate pairs through
hash joins before the full ON predicate is evaluated.

LEFT/RIGHT/FULL joins materialize stable per-input row numbers. Their matched
rows are shared with SEMI/ANTI checks that recover missing rows by identity,
preserving duplicates, all-NULL input rows and the parameter values of preserved
sides. Materialization is a projection-pushdown barrier: one consumer cannot
discard columns that another cache reader needs for row identity or results.

Binding records the owning query depth of aggregate calls. A call referencing
only outer columns belongs to the closest directly referenced outer scope;
constants and calls without direct column references remain locally owned.
The owning aggregate collector evaluates each call and passes its result back
to child queries. Argument subqueries are rebased with the call. Inner row
counts are not collapsed by an aggregate owned by the outer scope, so scalar
cardinality errors remain observable. Same-scope nested aggregates, invalid
grouping and arguments capturing a removed local scope are explicitly rejected.
Grouping legality follows the owning scope: GROUP BY `(g + 1)` does not make
the raw input column `g` available to a child query.

## LATERAL and scalar dependencies

FROM derived tables retain their own AST nodes and bind at their FROM position,
so right inputs can resolve preceding sibling columns. Explicit LATERAL and
implicit sibling correlation share the same binding path. Only dependent right
inputs add a sibling scope boundary; unused boundaries are removed without
changing references to enclosing queries. Correlation and aggregate walkers
track these boundaries alongside expression-subquery boundaries.
Explicit JOIN binds more tightly than comma FROM items. A joined input after
a comma retains its original table qualifiers while capturing preceding inputs
through one sibling scope, preserving outer-join cardinality.

INNER/CROSS and LEFT dependent joins evaluate the right input over a distinct
domain of referenced left values and parent parameters, then attach rows using
NULL-safe parameter identity. Existing outer-join completion preserves every
unmatched left input row, including duplicates and NULL parameters. Chained
lateral inputs, nested expression queries, local aggregates, UNION ALL,
constant per-parameter LIMIT/OFFSET, aliases and prepared values are supported.
Actual lateral dependencies under RIGHT/FULL/SEMI/ANTI joins and aggregates
owned by the sibling input are rejected, matching DuckDB 2.0.0.

Non-correlated scalar queries are prepared bottom-up. Each child physical plan
can consume already materialized scalar dependencies. Runtime preparation
follows only dependencies reachable from the final physical plans, so unused
EXISTS SELECT expressions do not execute merely because they contain nested
subqueries. Child constants remain query-owned managed vectors; zero/multiple
row semantics and failure cleanup apply at each nesting level.

Sampling an independent source before applying a correlated predicate remains
supported. Pushing correlation through the sampling operator itself is rejected,
as in DuckDB 2.0.0. Non-constant correlated LIMIT/OFFSET is likewise rejected.
LATERAL table-function arguments retain the existing static-argument binding
restriction; this implementation covers derived SELECT inputs. Full DuckDB
operator/grammar coverage, including quantified ANY/ALL, is not implied.
Non-equality execution without usable keys still uses a cross product/filter; large
parameter domains can therefore be expensive and the existing CROSS runtime
retains its build side in managed memory.

References: [DuckDB dependent-join lowering](https://github.com/duckdb/duckdb/blob/main/src/planner/subquery/flatten_dependent_join.cpp).
Regression cases `SQL.SUBQUERY.DOMAIN.001` through `.019` in `test/compat/stable.slt`
are checked against DuckDB 2.0.0, together with `SQL.SUBQUERY.RELATION.001` through
`.018`, `SQL.SUBQUERY.SCOPE.001` through `.020`, `SQL.SUBQUERY.LATERAL.001`
through `.024` and `SQL.SUBQUERY.NESTED.001` through `.018`.
`TestDomainCorrelatedSubqueries`, `TestRelationCorrelatedSubqueries` and
`TestOuterAggregateSubqueries`, `TestLateralDerivedTables` and
`TestNestedScalarDependencies` cover memory and low-budget execution. Additional
tests cover cross-batch limits/outer joins, shared input reuse, cardinality and
callback/rejection cleanup, prepared lateral values, dependency reuse and
pruning, and NULL-key hash spill/skew behavior.

## DISTINCT aggregates

COUNT/SUM/AVG share a sort-and-mark input when their DISTINCT argument is the
same. Only the first `(group, value)` occurrence contributes a non-NULL argument.
MIN/MAX accept DISTINCT without an extra de-duplication pass. Existing aggregate
types, overflow checks, empty-input semantics and spill storage remain in force.
