//go:build debug2PC

/*
Copyright 2024 The Vitess Authors.

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

	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const DebugTwoPc = true

// checkTestFailure is used to simulate failures in 2PC flow for testing when DebugTwoPc is true.
func checkTestFailure(ctx context.Context, expectCaller string, target *querypb.Target) error {
	callerID := callerid.EffectiveCallerIDFromContext(ctx)
	if callerID == nil || callerID.GetPrincipal() != expectCaller {
		return nil
	}
	switch callerID.Principal {
	case "TRCreated_FailNow":
		log.Errorf("Fail After TR created")
		// no commit decision is made. Transaction should be a rolled back.
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Fail After TR created")
	case "RMPrepare_-40_FailNow":
		if target.Shard != "-40" {
			return nil
		}
		log.Errorf("Fail During RM prepare")
		// no commit decision is made. Transaction should be a rolled back.
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Fail During RM prepare")
	case "RMPrepared_FailNow":
		log.Errorf("Fail After RM prepared")
		// no commit decision is made. Transaction should be a rolled back.
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Fail After RM prepared")
	case "MMCommitted_FailNow":
		log.Errorf("Fail After MM commit")
		//  commit decision is made. Transaction should be committed.
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Fail After MM commit")
	case "RMCommit_-40_FailNow":
		if target.Shard != "-40" {
			return nil
		}
		log.Errorf("Fail During RM commit")
		// commit decision is made. Transaction should be a committed.
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Fail During RM commit")
	default:
		return nil
	}
}
