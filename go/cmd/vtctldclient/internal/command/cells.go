/*
Copyright 2021 The Vitess Authors.

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
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// GetCellInfoNames makes a GetCellInfoNames gRPC call to a vtctld.
	GetCellInfoNames = &cobra.Command{
		Use:  "GetCellInfoNames",
		Args: cobra.NoArgs,
		RunE: commandGetCellInfoNames,
	}
	// GetCellInfo makes a GetCellInfo gRPC call to a vtctld.
	GetCellInfo = &cobra.Command{
		Use:  "GetCellInfo cell",
		Args: cobra.ExactArgs(1),
		RunE: commandGetCellInfo,
	}
	// GetCellsAliases makes a GetCellsAliases gRPC call to a vtctld.
	GetCellsAliases = &cobra.Command{
		Use:  "GetCellsAliases",
		Args: cobra.NoArgs,
		RunE: commandGetCellsAliases,
	}
)

func commandGetCellInfoNames(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	resp, err := client.GetCellInfoNames(commandCtx, &vtctldatapb.GetCellInfoNamesRequest{})
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", strings.Join(resp.Names, "\n"))

	return nil
}

func commandGetCellInfo(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	cell := cmd.Flags().Arg(0)

	resp, err := client.GetCellInfo(commandCtx, &vtctldatapb.GetCellInfoRequest{Cell: cell})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.CellInfo)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func commandGetCellsAliases(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	resp, err := client.GetCellsAliases(commandCtx, &vtctldatapb.GetCellsAliasesRequest{})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.Aliases)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func init() {
	Root.AddCommand(GetCellInfoNames)
	Root.AddCommand(GetCellInfo)
	Root.AddCommand(GetCellsAliases)
}
