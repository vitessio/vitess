// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import "github.com/youtube/vitess/go/vt/sqlparser"

// appendFilters breaks up the BoolExpr into AND-separated conditions
// and appends them to filters, which can be shuffled and recombined
// as needed.
func appendFilters(filters []sqlparser.BoolExpr, node sqlparser.BoolExpr) []sqlparser.BoolExpr {
	if node, ok := node.(*sqlparser.AndExpr); ok {
		filters = appendFilters(filters, node.Left)
		return appendFilters(filters, node.Right)
	}
	return append(filters, node)
}
