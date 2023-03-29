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
	"time"

	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var SystemTime = time.Now

type (
	builtinNow struct {
		CallExpr
		utc      bool
		onlyTime bool
		prec     uint8
	}

	builtinSysdate struct {
		CallExpr
		prec uint8
	}

	builtinCurdate struct {
		CallExpr
	}
)

var _ Expr = (*builtinNow)(nil)
var _ Expr = (*builtinSysdate)(nil)
var _ Expr = (*builtinCurdate)(nil)

var (
	formatTime     [7]*datetime.Strftime
	formatDateTime [7]*datetime.Strftime
	formatDate     *datetime.Strftime
)

func init() {
	const fmtTime = "%H:%i:%s"
	const fmtDate = "%Y-%m-%d"
	const fmtDateTime = fmtDate + " " + fmtTime

	formatTime[0], _ = datetime.New(fmtTime, 0)
	formatDateTime[0], _ = datetime.New(fmtDateTime, 0)
	formatDate, _ = datetime.New(fmtDate, 0)

	for i := 1; i <= 6; i++ {
		formatTime[i], _ = datetime.New(fmtTime+".%f", uint8(i))
		formatDateTime[i], _ = datetime.New(fmtDateTime+".%f", uint8(i))
	}
}

func (call *builtinNow) eval(env *ExpressionEnv) (eval, error) {
	now := env.time(call.utc)
	if call.onlyTime {
		buf := formatTime[call.prec].Format(now)
		return newEvalRaw(sqltypes.Time, buf, collationBinary), nil
	} else {
		buf := formatDateTime[call.prec].Format(now)
		return newEvalRaw(sqltypes.Datetime, buf, collationBinary), nil
	}
}

func (call *builtinNow) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	if call.onlyTime {
		return sqltypes.Time, 0
	}
	return sqltypes.Datetime, 0
}

func (call *builtinNow) constant() bool {
	return false
}

func (call *builtinSysdate) eval(env *ExpressionEnv) (eval, error) {
	now := SystemTime()
	if tz := env.currentTimezone(); tz != nil {
		now = now.In(tz)
	}
	return newEvalRaw(sqltypes.Datetime, formatDateTime[call.prec].Format(now), collationBinary), nil
}

func (call *builtinSysdate) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Datetime, 0
}

func (call *builtinSysdate) constant() bool {
	return false
}

func (call *builtinCurdate) eval(env *ExpressionEnv) (eval, error) {
	now := env.time(false)
	return newEvalRaw(sqltypes.Date, formatDate.Format(now), collationBinary), nil
}

func (call *builtinCurdate) typeof(_ *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Date, 0
}

func (call *builtinCurdate) constant() bool {
	return false
}
