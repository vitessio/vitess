/*
Copyright 2020 The Vitess Authors.

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

package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func buildOtherReadAndAdmin(sql string, vschema plancontext.VSchema) (*planResult, error) {
	destination, keyspace, _, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}

	if destination == nil {
		destination = key.DestinationAnyShard{}
	}

	return newPlanResult(&engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             sql, // This is original sql query to be passed as the parser can provide partial ddl AST.
		SingleShardOnly:   true,
	}), nil
}

// checkDoLockFuncs rejects a DO statement that acquires or releases a named lock. The statement is sent
// as-is to any shard, so the lock would be taken on a pooled vttablet connection that goes on to serve
// other sessions, and released from whichever session borrows that connection next. SELECT runs these
// functions on the session's lock connection instead.
func checkDoLockFuncs(stmt *sqlparser.OtherAdmin) error {
	for _, expr := range stmt.Exprs {
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			lockFunc, ok := node.(*sqlparser.LockingFunc)
			if !ok {
				return true, nil
			}
			switch lockFunc.Type {
			case sqlparser.GetLock, sqlparser.ReleaseLock, sqlparser.ReleaseAllLocks:
				return false, vterrors.VT12001(lockFunc.Type.ToString() + " in a DO statement, use SELECT instead")
			}
			return true, nil
		}, expr)
		if err != nil {
			return err
		}
	}
	return nil
}
