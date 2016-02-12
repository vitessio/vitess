// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func buildUpdatePlan(upd *sqlparser.Update, vschema *VSchema) (*Route, error) {
	route := &Route{
		Query: generateQuery(upd),
	}
	tablename := sqlparser.GetTableName(upd.Table)
	var err error
	route.Table, err = vschema.FindTable(tablename)
	if err != nil {
		return nil, err
	}
	route.Keyspace = route.Table.Keyspace
	if !route.Keyspace.Sharded {
		// TODO(sougou): subquery check
		route.PlanID = UpdateUnsharded
		return route, nil
	}

	err = getWhereRouting(upd.Where, route)
	if err != nil {
		return nil, err
	}
	route.PlanID = UpdateEqual
	if isIndexChanging(upd.Exprs, route.Table.ColVindexes) {
		return nil, errors.New("index is changing")
	}
	return route, nil
}

func isIndexChanging(setClauses sqlparser.UpdateExprs, colVindexes []*ColVindex) bool {
	vindexCols := make([]string, len(colVindexes))
	for i, index := range colVindexes {
		vindexCols[i] = index.Col
	}
	for _, assignment := range setClauses {
		if sqlparser.StringIn(string(assignment.Name.Name), vindexCols...) {
			return true
		}
	}
	return false
}

func buildDeletePlan(del *sqlparser.Delete, vschema *VSchema) (*Route, error) {
	route := &Route{
		Query: generateQuery(del),
	}
	tablename := sqlparser.GetTableName(del.Table)
	var err error
	route.Table, err = vschema.FindTable(tablename)
	if err != nil {
		return nil, err
	}
	route.Keyspace = route.Table.Keyspace
	if !route.Keyspace.Sharded {
		// TODO(sougou): subquery check
		route.PlanID = DeleteUnsharded
		return route, nil
	}

	err = getWhereRouting(del.Where, route)
	if err != nil {
		return nil, err
	}
	route.PlanID = DeleteEqual
	route.Subquery = generateDeleteSubquery(del, route.Table)
	return route, nil
}

func generateDeleteSubquery(del *sqlparser.Delete, table *Table) string {
	if len(table.Owned) == 0 {
		return ""
	}
	buf := bytes.NewBuffer(nil)
	buf.WriteString("select ")
	prefix := ""
	for _, cv := range table.Owned {
		buf.WriteString(prefix)
		buf.WriteString(cv.Col)
		prefix = ", "
	}
	fmt.Fprintf(buf, " from %s", table.Name)
	buf.WriteString(sqlparser.String(del.Where))
	buf.WriteString(" for update")
	return buf.String()
}
