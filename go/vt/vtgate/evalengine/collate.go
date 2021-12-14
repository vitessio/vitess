package evalengine

import (
	"vitess.io/vitess/go/mysql/collations"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var collationNull = collations.TypedCollation{
	Collation:    collations.CollationBinaryID,
	Coercibility: collations.CoerceIgnorable,
	Repertoire:   collations.RepertoireASCII,
}

var collationNumeric = collations.TypedCollation{
	Collation:    collations.CollationBinaryID,
	Coercibility: collations.CoerceNumeric,
	Repertoire:   collations.RepertoireASCII,
}

func (c *CollateExpr) eval(env *ExpressionEnv) (EvalResult, error) {
	res, err := c.Inner.eval(env)
	if err != nil {
		return EvalResult{}, err
	}
	if err := collations.Local().EnsureCollate(res.collation.Collation, c.TypedCollation.Collation); err != nil {
		return EvalResult{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, err.Error())
	}
	res.collation = c.TypedCollation
	return res, nil
}

func (c *CollateExpr) collation() collations.TypedCollation {
	return c.TypedCollation
}

func (t TupleExpr) collation() collations.TypedCollation {
	// a Tuple does not have a collation, but an individual collation for every element of the tuple
	return collations.TypedCollation{}
}

func (l *Literal) collation() collations.TypedCollation {
	return l.Val.collation
}

func (bv *BindVariable) collation() collations.TypedCollation {
	return bv.coll
}

func (c *Column) collation() collations.TypedCollation {
	return c.coll
}
