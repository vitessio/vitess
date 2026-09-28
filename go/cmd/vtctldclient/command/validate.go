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
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/vt/topo/topoproto"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// Validate makes a Validate gRPC call to a vtctld.
	Validate = &cobra.Command{
		Use:                   "Validate [--ping-tablets]",
		Short:                 "Validates that all nodes reachable from the global replication graph, as well as all tablets in discoverable cells, are consistent.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.NoArgs,
		RunE:                  commandValidate,
	}
	// ValidateKeyspace makes a ValidateKeyspace gRPC call to a vtctld.
	ValidateKeyspace = &cobra.Command{
		Use:                   "ValidateKeyspace [--ping-tablets] <keyspace>",
		Short:                 "Validates that all nodes reachable from the specified keyspace are consistent.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandValidateKeyspace,
	}
	// ValidateShard makes a ValidateShard gRPC call to a vtctld.
	ValidateShard = &cobra.Command{
		Use:                   "ValidateShard [--ping-tablets] <keyspace/shard>",
		Short:                 "Validates that all nodes reachable from the specified shard are consistent.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandValidateShard,
	}
)

var validateOptions = struct {
	PingTablets bool
}{}

func commandValidate(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	resp, err := client.Validate(commandCtx, &vtctldatapb.ValidateRequest{
		PingTablets: validateOptions.PingTablets,
	})
	if err != nil {
		return err
	}

	buf := &strings.Builder{}
	if err := consumeValidationResults(resp, buf); err != nil {
		fmt.Printf("Validation results:\n%s", buf.String() /* note: this should have a trailing newline already */)
		return err
	}

	fmt.Println("Validation complete; no issues found.")
	return nil
}

var validateKeyspaceOptions = struct {
	PingTablets bool
}{}

func commandValidateKeyspace(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	keyspace := cmd.Flags().Arg(0)
	resp, err := client.ValidateKeyspace(commandCtx, &vtctldatapb.ValidateKeyspaceRequest{
		Keyspace:    keyspace,
		PingTablets: validateKeyspaceOptions.PingTablets,
	})
	if err != nil {
		return err
	}

	buf := &strings.Builder{}
	if err := consumeKeyspaceValidationResults(keyspace, resp, buf); err != nil {
		fmt.Printf("Validation results:\n%s", buf.String() /* note: this should have a trailing newline already */)
		return err
	}

	fmt.Printf("Validation of %s complete; no issues found.\n", keyspace)
	return nil
}

var validateShardOptions = struct {
	PingTablets bool
}{}

func commandValidateShard(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return fmt.Errorf("could not parse <keyspace/shard> from %s: %w", cmd.Flags().Arg(0), err)
	}

	cli.FinishedParsing(cmd)

	resp, err := client.ValidateShard(commandCtx, &vtctldatapb.ValidateShardRequest{
		Keyspace:    keyspace,
		Shard:       shard,
		PingTablets: validateShardOptions.PingTablets,
	})
	if err != nil {
		return err
	}

	buf := &strings.Builder{}
	if err := consumeShardValidationResults(keyspace, shard, resp, buf); err != nil {
		fmt.Printf("Validation results:\n%s", buf.String() /* note: this should have a trailing newline already */)
		return err
	}

	fmt.Printf("Validation of %s/%s complete; no issues found.\n", keyspace, shard)
	return nil
}

func consumeValidationResults(resp *vtctldatapb.ValidateResponse, buf *strings.Builder) error {
	for _, result := range resp.Results {
		fmt.Fprintf(buf, "- %s\n", result)
	}

	for keyspace, keyspaceResults := range resp.ResultsByKeyspace {
		buf2 := &strings.Builder{}
		if err := consumeKeyspaceValidationResults(keyspace, keyspaceResults, buf2); err != nil {
			fmt.Fprintf(buf, "\nFor %s:\n%s", keyspace, buf2.String() /* note: this should have a trailing newline */)
		}
	}

	if buf.Len() > 0 {
		return errors.New("some issues were found during validation; see above for details")
	}

	return nil
}

func consumeKeyspaceValidationResults(keyspace string, resp *vtctldatapb.ValidateKeyspaceResponse, buf *strings.Builder) error {
	for _, result := range resp.Results {
		fmt.Fprintf(buf, "- %s\n", result)
	}

	for shard, shardResults := range resp.ResultsByShard {
		if len(shardResults.Results) == 0 {
			continue
		}

		fmt.Fprintf(buf, "\nFor %s/%s:\n", keyspace, shard)
		_ = consumeShardValidationResults(keyspace, shard, shardResults, buf)
	}

	if buf.Len() > 0 {
		return fmt.Errorf("keyspace %s had validation issues; see above for details", keyspace)
	}

	return nil
}

func consumeShardValidationResults(keyspace string, shard string, resp *vtctldatapb.ValidateShardResponse, buf *strings.Builder) error {
	for _, result := range resp.Results {
		fmt.Fprintf(buf, "- %s\n", result)
	}

	if buf.Len() > 0 {
		return fmt.Errorf("shard %s/%s had validation issues; see above for details", keyspace, shard)
	}

	return nil
}

func init() {
	pingTabletsName := "ping-tablets"
	pingTabletsShort := "p"
	pingTabletsDefault := false
	pingTabletsUsage := "Indicates whether all tablets should be pinged during the validation process."

	Validate.Flags().BoolVarP(&validateOptions.PingTablets, pingTabletsName, pingTabletsShort, pingTabletsDefault, pingTabletsUsage)
	ValidateKeyspace.Flags().BoolVarP(&validateKeyspaceOptions.PingTablets, pingTabletsName, pingTabletsShort, pingTabletsDefault, pingTabletsUsage)
	ValidateShard.Flags().BoolVarP(&validateShardOptions.PingTablets, pingTabletsName, pingTabletsShort, pingTabletsDefault, pingTabletsUsage)

	Root.AddCommand(Validate)
	Root.AddCommand(ValidateKeyspace)
	Root.AddCommand(ValidateShard)
}
