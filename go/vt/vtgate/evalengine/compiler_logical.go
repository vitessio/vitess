package evalengine

import "vitess.io/vitess/go/mysql/collations"

func (c *compiler) compileCase(cs *CaseExpr) (ctype, error) {
	var ca collationAggregation
	var ta typeAggregation
	var local = collations.Local()

	for _, wt := range cs.cases {
		when, err := c.compileExpr(wt.when)
		if err != nil {
			return ctype{}, err
		}

		if err := c.compileCheckTrue(when, 1); err != nil {
			return ctype{}, err
		}

		then, err := c.compileExpr(wt.then)
		if err != nil {
			return ctype{}, err
		}

		ta.add(then.Type, then.Flag)
		if err := ca.add(local, then.Col); err != nil {
			return ctype{}, err
		}
	}

	if cs.Else != nil {
		els, err := c.compileExpr(cs.Else)
		if err != nil {
			return ctype{}, err
		}

		ta.add(els.Type, els.Flag)
		if err := ca.add(local, els.Col); err != nil {
			return ctype{}, err
		}
	}

	ct := ctype{Type: ta.result(), Col: ca.result()}
	c.asm.CmpCase(len(cs.cases), cs.Else != nil, ct.Type, ct.Col)
	return ct, nil
}
