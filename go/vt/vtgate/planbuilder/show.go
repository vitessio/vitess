// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
)

// buildSelectPlan is the new function to build a Select plan.
func buildShowPlan(node *sqlparser.Show, vschema VSchema) (primitive engine.Primitive, err error) {
	if (node.Type == sqlparser.ShowUnsupportedStr){
		return nil, errors.New("unsupported show statement")
	}

	route := &engine.Route{
		Opcode: engine.Show,
		Query: node.Type,
	}

	return route, nil
}
