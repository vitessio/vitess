/*
Copyright 2021 The Vitess Authors.

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
	"strconv"
	"time"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

func validateThrottleParams(alterMigrationType sqlparser.AlterMigrationType, expireString string, ratioLiteral *sqlparser.Literal) (duration time.Duration, ratio float64, err error) {
	switch alterMigrationType {
	case sqlparser.UnthrottleMigrationType,
		sqlparser.UnthrottleAllMigrationType:
		// Unthrottling is like throttling with duration=0
		duration = 0
	default:
		duration = time.Hour * 24 * 365 * 100
		if expireString != "" {
			duration, err = time.ParseDuration(expireString)
			if err != nil || duration < 0 {
				return duration, ratio, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid EXPIRE value: %s. Try '120s', '30m', '1h', etc. Allowed units are (s)ec, (m)in, (h)hour", expireString)
			}
		}
	}
	ratio = 1.0
	if ratioLiteral != nil {
		ratio, err = strconv.ParseFloat(ratioLiteral.Val, 64)
		if err != nil || ratio < 0 || ratio > 1 {
			return duration, ratio, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid RATIO value: %s. Try any decimal number between '0.0' (no throttle) and `1.0` (fully throttled)", ratioLiteral.Val)
		}
	}
	return duration, ratio, nil
}

func buildAlterMigrationThrottleAppPlan(query string, alterMigration *sqlparser.AlterMigration, keyspace *vindexes.Keyspace) (*planResult, error) {
	duration, ratio, err := validateThrottleParams(alterMigration.Type, alterMigration.Expire, alterMigration.Ratio)
	if err != nil {
		return nil, err
	}
	expireAt := time.Now().Add(duration)
	appName := alterMigration.UUID
	if appName == "" {
		appName = throttlerapp.OnlineDDLName.String()
	}
	throttledAppRule := &topodatapb.ThrottledAppRule{
		Name:      appName,
		ExpiresAt: logutil.TimeToProto(expireAt),
		Ratio:     ratio,
	}
	return newPlanResult(&engine.ThrottleApp{
		Keyspace:         keyspace,
		ThrottledAppRule: throttledAppRule,
	}), nil
}

func buildAlterMigrationPlan(query string, alterMigration *sqlparser.AlterMigration, vschema plancontext.VSchema, enableOnlineDDL bool) (*planResult, error) {
	if !enableOnlineDDL {
		return nil, schema.ErrOnlineDDLDisabled
	}

	dest, ks, tabletType, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.VT09005()
	}

	switch alterMigration.Type {
	case sqlparser.ThrottleMigrationType,
		sqlparser.ThrottleAllMigrationType,
		sqlparser.UnthrottleMigrationType,
		sqlparser.UnthrottleAllMigrationType:
		// ALTER VITESS_MIGRATION ... THROTTLE ... queries go to topo (similarly to `vtctldclient UpdateThrottlerConfig`)
		return buildAlterMigrationThrottleAppPlan(query, alterMigration, ks)
	}

	if tabletType != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.VT09006("ALTER")
	}

	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	send := &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             query,
	}
	return newPlanResult(send), nil
}

func buildRevertMigrationPlan(query string, stmt *sqlparser.RevertMigration, vschema plancontext.VSchema, enableOnlineDDL bool) (*planResult, error) {
	if !enableOnlineDDL {
		return nil, schema.ErrOnlineDDLDisabled
	}
	dest, ks, tabletType, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.VT09005()
	}

	if tabletType != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.VT09006("REVERT")
	}

	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	emig := &engine.RevertMigration{
		Keyspace:          ks,
		TargetDestination: dest,
		Stmt:              stmt,
		Query:             query,
	}
	return newPlanResult(emig), nil
}

func buildShowMigrationLogsPlan(query string, vschema plancontext.VSchema, enableOnlineDDL bool) (*planResult, error) {
	if !enableOnlineDDL {
		return nil, schema.ErrOnlineDDLDisabled
	}
	dest, ks, tabletType, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.VT09005()
	}

	if tabletType != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.VT09006("SHOW")
	}

	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	send := &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             query,
	}
	return newPlanResult(send), nil
}
