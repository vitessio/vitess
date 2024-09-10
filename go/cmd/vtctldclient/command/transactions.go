/*
Copyright 2024 The Vitess Authors.

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

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	DistributedTransaction = &cobra.Command{
		Use:   "DistributedTransaction <cmd>",
		Short: "Operates on distributed transaction",
		Args:  cobra.MinimumNArgs(2),

		DisableFlagsInUseLine: true,
	}

	// GetUnresolvedTransactions makes an GetUnresolvedTransactions gRPC call to a vtctld.
	GetUnresolvedTransactions = &cobra.Command{
		Use:     "list <keyspace>",
		Short:   "Retrieves unresolved transactions for the given keyspace.",
		Aliases: []string{"List"},
		Args:    cobra.ExactArgs(1),
		RunE:    commandGetUnresolvedTransactions,

		DisableFlagsInUseLine: true,
	}

	// ConcludeTransaction makes a ConcludeTransaction gRPC call to a vtctld.
	ConcludeTransaction = &cobra.Command{
		Use:     "conclude <dtid> [<keyspace/shard> ...]",
		Short:   "Concludes the unresolved transaction by rolling back the prepared transaction on all participating shards and removing the transaction metadata record.",
		Aliases: []string{"Conclude"},
		Args:    cobra.MinimumNArgs(1),
		RunE:    commandConcludeTransaction,

		DisableFlagsInUseLine: true,
	}
)

func commandGetUnresolvedTransactions(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	keyspace := cmd.Flags().Arg(0)
	resp, err := client.GetUnresolvedTransactions(commandCtx,
		&vtctldatapb.GetUnresolvedTransactionsRequest{
			Keyspace: keyspace,
		})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.Transactions)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", data)
	return nil
}

func commandConcludeTransaction(cmd *cobra.Command, args []string) error {
	allArgs := cmd.Flags().Args()
	shards, err := cli.ParseKeyspaceShards(allArgs[1:])
	if err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	dtid := allArgs[0]
	var participants []*querypb.Target
	for _, shard := range shards {
		participants = append(participants, &querypb.Target{
			Keyspace: shard.Keyspace,
			Shard:    shard.Name,
		})
	}
	_, err = client.ConcludeTransaction(commandCtx,
		&vtctldatapb.ConcludeTransactionRequest{
			Dtid:         dtid,
			Participants: participants,
		})
	if err != nil {
		return err
	}
	fmt.Println("Successfully concluded the distributed transaction")

	return nil
}

func init() {
	DistributedTransaction.AddCommand(GetUnresolvedTransactions)
	DistributedTransaction.AddCommand(ConcludeTransaction)

	Root.AddCommand(DistributedTransaction)
}
