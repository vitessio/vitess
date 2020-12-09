package main

import (
	"fmt"

	"github.com/spf13/cobra"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	getKeyspaceCmd = &cobra.Command{
		Use:     "GetKeyspace keyspace",
		Aliases: []string{"getkeyspace"},
		Args:    cobra.ExactArgs(1),
		RunE:    commandGetKeyspace,
	}
	getKeyspacesCmd = &cobra.Command{
		Use:     "GetKeyspaces",
		Aliases: []string{"getkeyspaces"},
		Args:    cobra.NoArgs,
		RunE:    commandGetKeyspaces,
	}
)

func commandGetKeyspace(cmd *cobra.Command, args []string) error {
	ks := cmd.Flags().Arg(0)
	resp, err := client.GetKeyspace(commandCtx, &vtctldatapb.GetKeyspaceRequest{
		Keyspace: ks,
	})

	if err != nil {
		return err
	}

	fmt.Printf("%+v\n", resp.Keyspace)

	return nil
}

func commandGetKeyspaces(cmd *cobra.Command, args []string) error {
	resp, err := client.GetKeyspaces(commandCtx, &vtctldatapb.GetKeyspacesRequest{})
	if err != nil {
		return err
	}

	fmt.Printf("%+v\n", resp.Keyspaces)

	return nil
}

func init() {
	rootCmd.AddCommand(getKeyspaceCmd)
	rootCmd.AddCommand(getKeyspacesCmd)
}
