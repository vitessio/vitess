package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type Route struct {
	Source      abstract.Operator
	RouteOpCode engine.RouteOpcode
	Keyspace    *vindexes.Keyspace

	// The following two fields are used when routing information_schema queries
	SysTableTableSchema []evalengine.Expr
	SysTableTableName   map[string]evalengine.Expr

	// here we store the possible vindexes we can use so that when we add predicates to the plan,
	// we can quickly check if the new predicates enables any new vindex options
	vindexPreds []*vindexPlusPredicates

	// the best option available is stored here
	selected *vindexOption
}

var _ abstract.Operator = (*Route)(nil)

func (r *Route) TableID() semantics.TableSet {
	return r.Source.TableID()
}

func (r *Route) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	return r.Source.PushPredicate(expr, semTable)
}

func (r *Route) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return r.Source.UnsolvedPredicates(semTable)
}

func (r *Route) CheckValid() error {
	return r.CheckValid()
}

func (r *Route) Compact(semTable *semantics.SemTable) (abstract.Operator, error) {
	return r.Compact(semTable)
}
