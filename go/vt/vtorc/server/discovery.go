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

package server

import (
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/logic"
	"vitess.io/vitess/go/vt/vtorc/process"
)

// RegisterFlags registers the flags required by VTOrc
func RegisterFlags(fs *pflag.FlagSet) {
	fs.String("orc_web_dir", "", "")
	fs.MarkDeprecated("orc_web_dir", "Web directory is no longer needed by VTOrc, please specify the --port flag to gain access to the debug pages and API of VTOrc")
}

// StartVTOrcDiscovery starts VTOrc discovery serving
func StartVTOrcDiscovery() {
	process.ContinuousRegistration(string(process.VTOrcExecutionHTTPMode), "")
	inst.SetMaintenanceOwner(process.ThisHostname)

	log.Info("Starting Discovery")
	go logic.ContinuousDiscovery()
}
