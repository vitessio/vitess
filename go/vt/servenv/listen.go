/*
Copyright 2026 The Vitess Authors.

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

package servenv

import (
	"net"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/utils"
)

var (
	// reusePort indicates that SO_REUSEPORT should be set when
	// binding sockets.
	reusePort = false
)

// Listen is used to bind sockets. By default, it is the same as
// net.Listen, however it can be used to configure socket options.
func Listen(protocol, address string) (net.Listener, error) {
	if reusePort {
		return netutil.ListenReusePort(protocol, address)
	}

	return net.Listen(protocol, address)
}

func registerReusePortFlag(fs *pflag.FlagSet) {
	utils.SetFlagBoolVar(fs, &reusePort, "reuse-port", reusePort, "Enable SO_REUSEPORT when binding sockets; available on Linux 3.9+ (default false)")
}

func init() {
	OnParseFor("vtgate", registerReusePortFlag)
}
