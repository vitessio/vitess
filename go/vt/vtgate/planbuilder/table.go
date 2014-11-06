// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

func getTableRouting(tablename string, schema *Schema) *Plan {
	if tablename == "" {
		return &Plan{
			ID:     NoPlan,
			Reason: "complex table expression",
		}
	}
	table := schema.Tables[tablename]
	if table == nil {
		return &Plan{
			ID:        NoPlan,
			Reason:    "table not found",
			TableName: tablename,
		}
	}
	if table.Keyspace.ShardingScheme == Unsharded {
		return &Plan{
			ID:        SelectUnsharded,
			TableName: tablename,
		}
	}
	return nil
}
