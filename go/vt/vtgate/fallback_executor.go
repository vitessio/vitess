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

package vtgate

import (
	"context"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
)

var _ executeMethod = (*fallbackExecutor)(nil)

type fallbackExecutor struct {
	exA executeMethod
	exB executeMethod
}

/*
This method implements the fall back logic.
If exA(plan execute)  is not able to plan the statement then it fall backs to exB(execute) method.
There is no fallback for parsing errors.
*/
func (f *fallbackExecutor) execute(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*query.BindVariable, logStats *LogStats) (sqlparser.StatementType, *sqltypes.Result, error) {
	stmtType, qr, err := f.exA.execute(ctx, safeSession, sql, bindVars, logStats)
	if err == planbuilder.ErrPlanNotSupported {
		return f.exB.execute(ctx, safeSession, sql, bindVars, logStats)
	}
	return stmtType, qr, err
}
