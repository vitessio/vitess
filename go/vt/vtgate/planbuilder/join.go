// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"encoding/json"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// This file contains routines for processing the FROM
// clause. Functions in this file manipulate various data
// structures. If they return an error, one should assume
// that the data structures may be in an inconsistent state.
// In general, the error should just be returned back to the
// application.

// joinBuilder is used to build a Join primitive.
// It's used to buid a normal join or a left join
// operation.
// TODO(sougou): struct is incomplete.
type joinBuilder struct {
	LeftOrder, RightOrder int
	// Left and Right are the nodes for the join.
	Left, Right planBuilder
	symtab      *symtab
	Colsyms     []*colsym
	// Join is the join plan.
	Join *Join
}

// Symtab returns the associated symtab.
func (jb *joinBuilder) Symtab() *symtab {
	return jb.symtab
}

// Order returns the order of the node.
func (jb *joinBuilder) Order() int {
	return jb.RightOrder
}

func (jb *joinBuilder) SupplyVar(col *sqlparser.ColName, varname string) {
	switch meta := col.Metadata.(type) {
	case *colsym:
		for i, colsym := range jb.Colsyms {
			if jb.Join.Cols[i] > 0 {
				continue
			}
			if meta == colsym {
				jb.Join.Vars[varname] = -jb.Join.Cols[i] - 1
				return
			}
		}
		panic("unexpected")
	case *tableAlias:
		ref := newColref(col)
		for i, colsym := range jb.Colsyms {
			if jb.Join.Cols[i] > 0 {
				continue
			}
			if colsym.Underlying == ref {
				jb.Join.Vars[varname] = -jb.Join.Cols[i] - 1
				return
			}
		}
		jb.Join.Vars[varname] = jb.Left.SupplyCol(col)
		return
	}
	panic("unexpected")
}

func (jb *joinBuilder) SupplyCol(col *sqlparser.ColName) int {
	switch meta := col.Metadata.(type) {
	case *colsym:
		panic("unexpected")
	case *tableAlias:
		ref := newColref(col)
		for i, colsym := range jb.Colsyms {
			if colsym.Underlying == ref {
				return i
			}
		}
		routeNumber := meta.Route().Order()
		jb.Colsyms = append(jb.Colsyms, &colsym{Underlying: ref})
		if routeNumber <= jb.LeftOrder {
			ret := jb.Left.SupplyCol(col)
			jb.Join.Cols = append(jb.Join.Cols, -ret-1)
		} else {
			ret := jb.Right.SupplyCol(col)
			jb.Join.Cols = append(jb.Join.Cols, ret+1)
		}
		return len(jb.Join.Cols) - 1
	}
	panic("unexpected")
}

// MarshalJSON marshals joinBuilder into a readable form.
// It's used for testing and diagnostics. The representation
// cannot be used to reconstruct a joinBuilder.
func (jb *joinBuilder) MarshalJSON() ([]byte, error) {
	marshalJoin := struct {
		LeftOrder   int
		RightOrder  int
		Left, Right planBuilder
		Join        *Join
	}{
		LeftOrder:  jb.LeftOrder,
		RightOrder: jb.RightOrder,
		Left:       jb.Left,
		Right:      jb.Right,
		Join: &Join{
			IsLeft: jb.Join.IsLeft,
			Cols:   jb.Join.Cols,
			Vars:   jb.Join.Vars,
		},
	}
	return json.Marshal(marshalJoin)
}
