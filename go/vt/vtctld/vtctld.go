/*
Copyright 2019 The Vitess Authors.

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

// Package vtctld holds the flags of the vtctld server and its
// /debug/health handler.
package vtctld

import (
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
)

var (
	sanitizeLogMessages = false
)

func init() {
	for _, cmd := range []string{"vtcombo", "vtctld"} {
		servenv.OnParseFor(cmd, registerVtctldFlags)
	}
}

func registerVtctldFlags(fs *pflag.FlagSet) {
	utils.SetFlagBoolVar(fs, &sanitizeLogMessages, "vtctld-sanitize-log-messages", sanitizeLogMessages, "When true, vtctld sanitizes logging.")

	// These flags configured the HTTP API under /api/ that served the
	// vtctld web UI, which VTAdmin replaced in v16. The API has been
	// removed; the flags are kept for one release so that processes
	// started with them keep starting.
	const deprecationMsg = "this flag is a no-op and will be removed in v26"
	fs.String("cell", "", "(DEPRECATED) This flag is a no-op: the HTTP API it configured has been removed.")
	_ = fs.MarkDeprecated("cell", deprecationMsg)
	fs.Bool("proxy-tablets", false, "(DEPRECATED) This flag is a no-op: the HTTP API it configured has been removed.")
	_ = fs.MarkDeprecated("proxy-tablets", deprecationMsg)
	fs.Duration("action-timeout", time.Minute, "(DEPRECATED) This flag is a no-op: the HTTP API it configured has been removed.")
	_ = fs.MarkDeprecated("action-timeout", deprecationMsg)
	fs.Duration("tablet-health-keep-alive", 5*time.Minute, "(DEPRECATED) This flag is a no-op: the HTTP API it configured has been removed.")
	_ = fs.MarkDeprecated("tablet-health-keep-alive", deprecationMsg)
}
