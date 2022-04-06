package abstract

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type Update struct {
	tbl         *QueryTable
	assignments map[string]sqlparser.Expr
}

func (u *Update) TableID() semantics.TableSet {
	return u.tbl.ID
}

func (u *Update) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return nil
}

func (u *Update) CheckValid() error {
	return nil
}

func (u *Update) iLogical() {}

func (u *Update) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) (LogicalOperator, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "can't accept predicates")
}

func (u *Update) Compact(semTable *semantics.SemTable) (LogicalOperator, error) {
	return u, nil
}

var _ LogicalOperator = (*Update)(nil)
