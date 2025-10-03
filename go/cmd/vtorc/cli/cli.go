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

package cli

import (
	"os"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/viperutil/debug"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/logic"
	"vitess.io/vitess/go/vt/vtorc/server"
)

var (
	Main = &cobra.Command{
		Use:   "vtorc",
		Short: "VTOrc is the automated fault detection and repair tool in Vitess.",
		Example: `vtorc \
	--topo-implementation etcd2 \
	--topo-global-server-address localhost:2379 \
	--topo-global-root /vitess/global \
	--log_dir $VTDATAROOT/tmp \
	--port 15000 \
	--instance-poll-time "1s" \
	--topo-information-refresh-duration "30s" \
	--alsologtostderr`,
		Args:    cobra.NoArgs,
		Version: servenv.AppVersion.String(),
		PreRunE: servenv.CobraPreRunE,
		Run:     run,
	}
)

func run(cmd *cobra.Command, args []string) {
	servenv.Init()
	inst.RegisterStats()

	log.Info("starting vtorc")
	if config.GetAuditToSyslog() {
		inst.EnableAuditSyslog()
	}
	config.MarkConfigurationLoaded()
	if err := config.Validate(); err != nil {
		log.Errorf("Failed to validate config: %+v", err)
		os.Exit(1)
	}

	// Log final config values to debug if something goes wrong.
	log.Infof("Running with Configuration - %v", debug.AllSettings())
	server.StartVTOrcDiscovery()

	server.RegisterVTOrcAPIEndpoints()
	servenv.OnRun(func() {
		addStatusParts()
	})

	// For backward compatability, we require that VTOrc functions even when the --port flag is not provided.
	// In this case, it should function like before but without the servenv pages.
	// Therefore, currently we don't check for the --port flag to be necessary, but release 16+ that check
	// can be added to always have the serenv page running in VTOrc.
	servenv.RunDefault()
}

// addStatusParts adds UI parts to the /debug/status page of VTOrc
func addStatusParts() {
	servenv.AddStatusPart("Recent Recoveries", logic.TopologyRecoveriesTemplate, func() any {
		recoveries, _ := logic.ReadRecentRecoveries(0)
		return recoveries
	})
}

func init() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterFlags()

	servenv.MoveFlagsToCobraCommand(Main)

	logic.RegisterFlags(Main.Flags())
	acl.RegisterFlags(Main.Flags())
}
