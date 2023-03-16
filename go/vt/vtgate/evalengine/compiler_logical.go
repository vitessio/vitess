/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package evalengine

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
)

func (c *compiler) compileIs(is *IsExpr) (ctype, error) {
	_, err := c.compileExpr(is.Inner)
	if err != nil {
		return ctype{}, err
	}
	c.asm.Is(is.Check)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagIsBoolean}, nil
}

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
