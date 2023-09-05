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
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/acl"
	_flag "vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/viperutil"
	viperdebug "vitess.io/vitess/go/viperutil/debug"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/logic"
	"vitess.io/vitess/go/vt/vtorc/server"
)

var (
	configFile string
	Main       = &cobra.Command{
		Use:     "vtorc",
		Short:   "", // TODO
		Args:    cobra.NoArgs,
		Version: servenv.AppVersion.String(),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			_flag.TrickGlog()

			watchCancel, err := viperutil.LoadConfig()
			if err != nil {
				return fmt.Errorf("%s: failed to read in config: %s", cmd.Name(), err)
			}

			servenv.OnTerm(watchCancel)
			servenv.HTTPHandleFunc("/debug/config", viperdebug.HandlerFunc)
			return nil
		},
		Run: run,
	}
)

func run(cmd *cobra.Command, args []string) {
	servenv.Init()
	config.UpdateConfigValuesFromFlags()
	inst.RegisterStats()

	log.Info("starting vtorc")
	if len(configFile) > 0 {
		config.ForceRead(configFile)
	} else {
		config.Read("/etc/vtorc.conf.json", "conf/vtorc.conf.json", "vtorc.conf.json")
	}
	if config.Config.AuditToSyslog {
		inst.EnableAuditSyslog()
	}
	config.MarkConfigurationLoaded()

	// Log final config values to debug if something goes wrong.
	config.LogConfigValues()
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
		recoveries, _ := logic.ReadRecentRecoveries(false, 0)
		return recoveries
	})
}

func init() {
	logic.RegisterFlags(Main.Flags())
	server.RegisterFlags(Main.Flags())
	config.RegisterFlags(Main.Flags())
	acl.RegisterFlags(Main.Flags())
	Main.Flags().StringVar(&configFile, "config", "", "config file name")
}
