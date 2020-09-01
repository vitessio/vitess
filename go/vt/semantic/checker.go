package semantic

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type checker struct {
	inGroupBy bool
}

func (s *checker) VisitDown(node sqlparser.SQLNode) error {
	switch node.(type) {
	case sqlparser.GroupBy:
		s.inGroupBy = true
	case *sqlparser.Subquery:
		if s.inGroupBy {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported: subqueries disallowed in GROUP or ORDER BY")
		}
	}
	return nil
}

func (s *checker) VisitUp(node sqlparser.SQLNode) error {
	switch node.(type) {
	case sqlparser.GroupBy:
		s.inGroupBy = false
	}
	return nil
}
