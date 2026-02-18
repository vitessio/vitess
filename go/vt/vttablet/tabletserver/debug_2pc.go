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

package tabletserver

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/log"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const DebugTwoPc = true

// readFileForTestSynchronization is a test-only function that reads a file
// that we use for synchronizing some of the tests.
func readFileForTestSynchronization(fileName string) string {
	res, _ := os.ReadFile(path.Join(os.Getenv("VTDATAROOT"), fileName))
	return string(res)
}

// commitPreparedDelayForTest is a test-only function that delays the commit that have already been prepared.
func commitPreparedDelayForTest(tsv *TabletServer) {
	sh := readFileForTestSynchronization("VT_DELAY_COMMIT_SHARD")
	if tsv.sm.target.Shard == sh {
		delay := readFileForTestSynchronization("VT_DELAY_COMMIT_TIME")
		delVal, _ := strconv.Atoi(delay)
		log.Info(fmt.Sprintf("Delaying commit for shard %v for %d seconds", sh, delVal))
		time.Sleep(time.Duration(delVal) * time.Second)
	}
}

// checkTestFailure is used to simulate failures in 2PC flow for testing when DebugTwoPc is true.
func checkTestFailure(ctx context.Context, shard string) error {
	if shard != "80-" {
		return nil
	}
	callerID := callerid.EffectiveCallerIDFromContext(ctx)
	if callerID == nil {
		return nil
	}
	switch callerID.Principal {
	case "CP_80-_R":
		// retryable error.
		return vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "commit prepared: retryable error")
	case "CP_80-_NR":
		// non retryable error.
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "commit prepared: non retryable error")
	default:
		return nil
	}
}
