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

package migrate

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// mount is the base command for all actions related to the mount action.
	mount = &cobra.Command{
		Use:                   "Mount [command] [command-flags]",
		Short:                 "Mount is used to link an external Vitess cluster in order to migrate data from it.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"mount"},
		Args:                  cobra.ExactArgs(1),
	}
)

var mountOptions struct {
	TopoType   string
	TopoServer string
	TopoRoot   string
}

var register = &cobra.Command{
	Use:                   "register",
	Short:                 "Register an external Vitess Cluster",
	Example:               `vtctldclient --server localhost:15999 mount register --topo-type etcd2 --topo-server localhost:12379 --topo-root /vitess/global ext1`,
	DisableFlagsInUseLine: true,
	Aliases:               []string{"Register"},
	Args:                  cobra.ExactArgs(1),
	RunE:                  commandRegister,
}

func commandRegister(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.MountRegisterRequest{
		TopoType:   mountOptions.TopoType,
		TopoServer: mountOptions.TopoServer,
		TopoRoot:   mountOptions.TopoRoot,
		Name:       cmd.Flags().Arg(0),
	}
	_, err := common.GetClient().MountRegister(common.GetCommandCtx(), req)
	if err != nil {
		return err
	}
	fmt.Printf("Mount %s registered successfully\n", req.Name)
	return nil
}

var unregister = &cobra.Command{
	Use:                   "unregister",
	Short:                 "Unregister a previously mounted external Vitess Cluster",
	Example:               `vtctldclient --server localhost:15999 mount unregister ext1`,
	DisableFlagsInUseLine: true,
	Aliases:               []string{"Unregister"},
	Args:                  cobra.ExactArgs(1),
	RunE:                  commandUnregister,
}

func commandUnregister(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.MountUnregisterRequest{
		Name: args[0],
	}
	_, err := common.GetClient().MountUnregister(common.GetCommandCtx(), req)
	if err != nil {
		return err
	}
	fmt.Printf("Mount %s unregistered successfully\n", req.Name)
	return nil
}

var show = &cobra.Command{
	Use:                   "show",
	Short:                 "Show attributes of a previously mounted external Vitess Cluster",
	Example:               `vtctldclient --server localhost:15999 Mount Show ext1`,
	DisableFlagsInUseLine: true,
	Aliases:               []string{"Show"},
	Args:                  cobra.ExactArgs(1),
	RunE:                  commandShow,
}

func commandShow(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.MountShowRequest{
		Name: args[0],
	}
	resp, err := common.GetClient().MountShow(common.GetCommandCtx(), req)
	if err != nil {
		return err
	}
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", string(data))
	return nil
}

var list = &cobra.Command{
	Use:                   "list",
	Short:                 "List all mounted external Vitess Clusters",
	Example:               `vtctldclient --server localhost:15999 mount list`,
	DisableFlagsInUseLine: true,
	Aliases:               []string{"List"},
	Args:                  cobra.NoArgs,
	RunE:                  commandList,
}

func commandList(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.MountListRequest{}
	resp, err := common.GetClient().MountList(common.GetCommandCtx(), req)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", string(data))
	return nil
}

func registerCommands(root *cobra.Command) {
	root.AddCommand(mount)
	addRegisterFlags(register)
	mount.AddCommand(register)
	mount.AddCommand(unregister)
	mount.AddCommand(show)
	mount.AddCommand(list)
}

func addRegisterFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&mountOptions.TopoType, "topo-type", "", "Topo server implementation to use")
	cmd.Flags().StringVar(&mountOptions.TopoServer, "topo-server", "", "Topo server address")
	cmd.Flags().StringVar(&mountOptions.TopoRoot, "topo-root", "", "Topo server root path")
	cmd.MarkFlagRequired("topo-type")
	cmd.MarkFlagRequired("topo-server")
	cmd.MarkFlagRequired("topo-root")
}

func init() {
	common.RegisterCommandHandler("Mount", registerCommands)
}
