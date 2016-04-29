// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/automation/resolver"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
	"golang.org/x/net/context"
)

const retryInterval time.Duration = 5 * time.Second

// ExecuteVtworker executes the vtworker command in "args" via an RPC to
// "server".
// The output of the RPC, a stream of LoggerEvent messages, is concatenated into
// one output string.
// If a retryable error is encountered (e.g. the vtworker process is already
// executing another command), this function will keep retrying infinitely until
// "ctx" is cancelled.
func ExecuteVtworker(ctx context.Context, server string, args []string) (string, error) {
	var output bytes.Buffer
	loggerToBufferFunc := createLoggerEventToBufferFunction(&output)
	outputLogger := newOutputLogger(loggerToBufferFunc)

	startMsg := fmt.Sprintf("Executing remote vtworker command: %v server: %v", args, server)
	outputLogger.Infof(startMsg)
	log.Info(startMsg)

	var err error
	loggedRetryStart := make(map[string]bool)
	var retryStart time.Time
	var addrOfLastVtworkerTried string
	// List of resolved addresses which are logged every time they change.
	var lastSortedAddrs []string
retryLoop:
	for {
		// Resolve "server" to a list of addresses before each retry.
		// If "server" resolves to multiple addresses, try them all without a wait
		// between each try.
		// Note that "addrs" will be shuffled by resolver.Resolve() to avoid that
		// two concurrent calls try the same task.
		var addrs []string
		addrs, err = resolver.Resolve(server)
		if err != nil {
			break retryLoop
		}

		// Log resolved addresses if they have changed since the last retry.
		sortedAddrs := make([]string, len(addrs))
		copy(sortedAddrs, addrs)
		sort.Strings(sortedAddrs)
		if addrs[0] != server && !reflect.DeepEqual(sortedAddrs, lastSortedAddrs) {
			addrsResolvedMsg := fmt.Sprintf("vtworker hostname: %v resolved to addresses: %v", server, addrs)
			outputLogger.Infof(addrsResolvedMsg)
			log.Info(addrsResolvedMsg)
			lastSortedAddrs = sortedAddrs
		}

		for _, addr := range addrs {
			addrOfLastVtworkerTried = addr
			err = vtworkerclient.RunCommandAndWait(
				ctx, addr, args,
				loggerToBufferFunc)
			if err == nil {
				break retryLoop
			}

			if !isRetryable(err) {
				break retryLoop
			}

			// Log retry once per unique address.
			if !loggedRetryStart[addr] {
				loggedRetryStart[addr] = true
				retryStart = time.Now()
				retryStartMsg := fmt.Sprintf("vtworker (%s) responded with a retryable error (%v). continuing to retry every %.0f seconds until cancelled.", addr, err, retryInterval.Seconds())
				outputLogger.Infof(retryStartMsg)
				log.Info(retryStartMsg)
			}
		}

		// Sleep until the next retry.
		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			// Context is up. The next retry would result in a non-retryable error, so
			// break out early.
			timer.Stop()
			err = ctx.Err()
			break retryLoop
		case <-timer.C:
		}
	} // retry loop

	if len(loggedRetryStart) > 0 {
		// Log end of retrying explicitly as well.
		d := time.Now().Sub(retryStart)
		retryEndMsg := fmt.Sprintf("Stopped retrying after %.1f seconds.", d.Seconds())
		outputLogger.Infof(retryEndMsg)
		log.Info(retryEndMsg)
	}

	endMsg := fmt.Sprintf("Executed remote vtworker command: %v server: %v (%v) err: %v", args, server, addrOfLastVtworkerTried, err)
	outputLogger.Infof(endMsg)
	// Log full output to log file (but not to the buffer).
	log.Infof("%v output (starting on next line):\n%v", endMsg, output.String())

	return output.String(), err
}

// TODO(mberlin): Discuss with the team if it should go to the vterrors package.
// TODO(mberlin): Add other error codes here as well?
func isRetryable(err error) bool {
	switch vterrors.RecoverVtErrorCode(err) {
	case vtrpcpb.ErrorCode_TRANSIENT_ERROR:
		return true
	default:
		return false
	}
}
