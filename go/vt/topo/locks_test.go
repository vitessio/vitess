/*
Copyright 2022 The Vitess Authors.

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

package topo

import (
	"os"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/internal/flag"
)

// TestGetLockTimeout tests the behaviour of
// getLockTimeout function in different situations where
// the two flags `remote_operation_timeout` and `lock-timeout` are
// provided or not.
func TestGetLockTimeout(t *testing.T) {
	tests := []struct {
		description                 string
		lockTimeoutValue            string
		remoteOperationTimeoutValue string
		expectedLockTimeout         time.Duration
	}{
		{
			description:                 "no flags specified",
			lockTimeoutValue:            "",
			remoteOperationTimeoutValue: "",
			expectedLockTimeout:         45 * time.Second,
		}, {
			description:                 "lock-timeout flag specified",
			lockTimeoutValue:            "33s",
			remoteOperationTimeoutValue: "",
			expectedLockTimeout:         33 * time.Second,
		}, {
			description:                 "remote operation timeout flag specified",
			lockTimeoutValue:            "",
			remoteOperationTimeoutValue: "33s",
			expectedLockTimeout:         33 * time.Second,
		}, {
			description:                 "both flags specified",
			lockTimeoutValue:            "33s",
			remoteOperationTimeoutValue: "22s",
			expectedLockTimeout:         33 * time.Second,
		}, {
			description:                 "remote operation timeout flag specified to the default",
			lockTimeoutValue:            "",
			remoteOperationTimeoutValue: "15s",
			expectedLockTimeout:         15 * time.Second,
		}, {
			description:                 "lock-timeout flag specified to the default",
			lockTimeoutValue:            "45s",
			remoteOperationTimeoutValue: "33s",
			expectedLockTimeout:         45 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			oldLockTimeout := LockTimeout
			oldRemoteOpsTimeout := RemoteOperationTimeout
			defer func() {
				LockTimeout = oldLockTimeout
				RemoteOperationTimeout = oldRemoteOpsTimeout
			}()
			var args []string
			if tt.lockTimeoutValue != "" {
				args = append(args, "--lock-timeout", tt.lockTimeoutValue)
			}
			if tt.remoteOperationTimeoutValue != "" {
				args = append(args, "--remote_operation_timeout", tt.remoteOperationTimeoutValue)
			}
			os.Args = os.Args[0:1]
			os.Args = append(os.Args, args...)

			fs := pflag.NewFlagSet("test", pflag.ExitOnError)
			registerTopoLockFlags(fs)
			flag.Parse(fs)

			val := getLockTimeout()
			require.Equal(t, tt.expectedLockTimeout, val)
		})
	}

}
