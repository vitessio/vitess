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

package command

import (
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/zkctl"
)

var (
	zkCfg   = "6@<hostname>:3801:3802:3803"
	myID    uint
	zkExtra []string

	zkd *zkctl.Zkd

	Root = &cobra.Command{
		Use:   "zkctl",
		Short: "Initializes and controls zookeeper with Vitess-specific configuration.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := servenv.CobraPreRunE(cmd, args); err != nil {
				return err
			}

			zkConfig := zkctl.MakeZkConfigFromString(zkCfg, uint32(myID))
			zkConfig.Extra = zkExtra
			zkd = zkctl.NewZkd(zkConfig)

			return nil
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			logutil.Flush()
		},
	}
)

func init() {
	Root.PersistentFlags().StringVar(&zkCfg, "zk.cfg", zkCfg,
		"zkid@server1:leaderPort1:electionPort1:clientPort1,...)")
	Root.PersistentFlags().UintVar(&myID, "zk.myid", myID,
		"which server do you want to be? only needed when running multiple instance on one box, otherwise myid is implied by hostname")
	Root.PersistentFlags().StringArrayVar(&zkExtra, "zk.extra", zkExtra,
		"extra config line(s) to append verbatim to config (flag can be specified more than once)")

	servenv.MovePersistentFlagsToCobraCommand(Root)
}
