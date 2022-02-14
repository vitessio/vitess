package abstract

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// Concatenate represents a UNION ALL/DISTINCT.
type Concatenate struct {
	Distinct    bool
	SelectStmts []*sqlparser.Select
	Sources     []LogicalOperator
	OrderBy     sqlparser.OrderBy
	Limit       *sqlparser.Limit
}

var _ LogicalOperator = (*Concatenate)(nil)

func (*Concatenate) iLogical() {}

// TableID implements the Operator interface
func (c *Concatenate) TableID() semantics.TableSet {
	var tableSet semantics.TableSet
	for _, source := range c.Sources {
		tableSet.MergeInPlace(source.TableID())
	}
	return tableSet
}

// PushPredicate implements the Operator interface
func (c *Concatenate) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) (LogicalOperator, error) {
	newSources := make([]LogicalOperator, 0, len(c.Sources))
	for index, source := range c.Sources {
		if len(c.SelectStmts[index].SelectExprs) != 1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't push predicates on concatenate")
		}
		if _, isStarExpr := c.SelectStmts[index].SelectExprs[0].(*sqlparser.StarExpr); !isStarExpr {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't push predicates on concatenate")
		}

		newSrc, err := source.PushPredicate(expr, semTable)
		if err != nil {
			return nil, err
		}
		newSources = append(newSources, newSrc)
	}
	c.Sources = newSources
	return c, nil
}

// UnsolvedPredicates implements the Operator interface
func (c *Concatenate) UnsolvedPredicates(*semantics.SemTable) []sqlparser.Expr {
	return nil
}

// CheckValid implements the Operator interface
func (c *Concatenate) CheckValid() error {
	for _, source := range c.Sources {
		err := source.CheckValid()
		if err != nil {
			return err
		}
	}
	return nil
}

// Compact implements the Operator interface
func (c *Concatenate) Compact(*semantics.SemTable) (LogicalOperator, error) {
	var newSources []LogicalOperator
	var newSels []*sqlparser.Select
	for i, source := range c.Sources {
		other, isConcat := source.(*Concatenate)
		if !isConcat {
			newSources = append(newSources, source)
			newSels = append(newSels, c.SelectStmts[i])
			continue
		}
		switch {
		case other.Limit == nil && len(other.OrderBy) == 0 && !other.Distinct:
			fallthrough
		case c.Distinct && other.Limit == nil:
			// if the current UNION is a DISTINCT, we can safely ignore everything from children UNIONs, except LIMIT
			newSources = append(newSources, other.Sources...)
			newSels = append(newSels, other.SelectStmts...)

		default:
			newSources = append(newSources, other)
			newSels = append(newSels, nil)
		}
	}
	c.Sources = newSources
	c.SelectStmts = newSels
	return c, nil
}
