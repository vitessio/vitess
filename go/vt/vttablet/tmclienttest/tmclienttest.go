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

package tmclienttest

import (
	"os"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

const tmclientProtocolFlagName = "tablet_manager_protocol"

// SetProtocol is a helper function to set the tmclient --tablet_maanager_protocol
// flag value for tests. If successful, it returns a function that, when called,
// returns the flag to its previous value.
//
// Note that because this variable is bound to a flag, the effects of this
// function are global, not scoped to the calling test-case. Therefore it should
// not be used in conjunction with t.Parallel.
func SetProtocol(name string, protocol string) (reset func()) {
	var tmp []string
	tmp, os.Args = os.Args[:], []string{name}
	defer func() { os.Args = tmp }()

	servenv.OnParseFor(name, func(fs *pflag.FlagSet) {
		if fs.Lookup(tmclientProtocolFlagName) != nil {
			return
		}

		tmclient.RegisterFlags(fs)
	})
	servenv.ParseFlags(name)

	switch oldVal, err := pflag.CommandLine.GetString(tmclientProtocolFlagName); err {
	case nil:
		reset = func() { SetProtocol(name, oldVal) }
	default:
		log.Errorf("failed to get string value for flag %q: %v", tmclientProtocolFlagName, err)
		reset = func() {}
	}

	if err := pflag.Set(tmclientProtocolFlagName, protocol); err != nil {
		msg := "failed to set flag %q to %q: %v"
		log.Errorf(msg, tmclientProtocolFlagName, protocol, err)
		reset = func() {}
	}

	return reset
}
