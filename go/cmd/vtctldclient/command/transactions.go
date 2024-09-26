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
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	DistributedTransaction = &cobra.Command{
		Use:   "DistributedTransaction [command] [command-flags]",
		Short: "Perform commands on distributed transaction",
		Args:  cobra.ExactArgs(1),

		DisableFlagsInUseLine: true,
	}

	unresolvedTransactionsOptions = struct {
		Keyspace   string
		AbandonAge int64 // in seconds
	}{}

	// GetUnresolvedTransactions makes an GetUnresolvedTransactions gRPC call to a vtctld.
	GetUnresolvedTransactions = &cobra.Command{
		Use:     "unresolved-list --keyspace <keyspace> --abandon-age <abandon_time_seconds>",
		Short:   "Retrieves unresolved transactions for the given keyspace.",
		Aliases: []string{"List"},
		Args:    cobra.NoArgs,
		RunE:    commandGetUnresolvedTransactions,

		DisableFlagsInUseLine: true,
	}

	concludeTransactionOptions = struct {
		Dtid string
	}{}

	// ConcludeTransaction makes a ConcludeTransaction gRPC call to a vtctld.
	ConcludeTransaction = &cobra.Command{
		Use:     "conclude --dtid <dtid>",
		Short:   "Concludes the unresolved transaction by rolling back the prepared transaction on each participating shard and removing the transaction metadata record.",
		Aliases: []string{"Conclude"},
		Args:    cobra.NoArgs,
		RunE:    commandConcludeTransaction,

		DisableFlagsInUseLine: true,
	}
)

type ConcludeTransactionOutput struct {
	Dtid    string `json:"dtid"`
	Message string `json:"message"`
	Error   string `json:"error,omitempty"`
}

const (
	concludeSuccess = "Successfully concluded the distributed transaction"
	concludeFailure = "Failed to conclude the distributed transaction"
)

func commandGetUnresolvedTransactions(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	resp, err := client.GetUnresolvedTransactions(commandCtx,
		&vtctldatapb.GetUnresolvedTransactionsRequest{
			Keyspace:   unresolvedTransactionsOptions.Keyspace,
			AbandonAge: unresolvedTransactionsOptions.AbandonAge,
		})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.Transactions)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func commandConcludeTransaction(cmd *cobra.Command, args []string) (err error) {
	cli.FinishedParsing(cmd)

	output := ConcludeTransactionOutput{
		Dtid:    concludeTransactionOptions.Dtid,
		Message: concludeSuccess,
	}

	_, err = client.ConcludeTransaction(commandCtx,
		&vtctldatapb.ConcludeTransactionRequest{
			Dtid: concludeTransactionOptions.Dtid,
		})
	if err != nil {
		output.Message = concludeFailure
		output.Error = err.Error()
	}

	data, _ := cli.MarshalJSON(output)
	fmt.Println(string(data))

	return err
}

func init() {
	GetUnresolvedTransactions.Flags().StringVarP(&unresolvedTransactionsOptions.Keyspace, "keyspace", "k", "", "unresolved transactions list for the given keyspace.")
	GetUnresolvedTransactions.Flags().Int64VarP(&unresolvedTransactionsOptions.AbandonAge, "abandon-age", "a", 0, "unresolved transactions list which are older than the specified age(in seconds).")
	DistributedTransaction.AddCommand(GetUnresolvedTransactions)

	ConcludeTransaction.Flags().StringVarP(&concludeTransactionOptions.Dtid, "dtid", "d", "", "conclude transaction for the given distributed transaction ID.")
	DistributedTransaction.AddCommand(ConcludeTransaction)

	Root.AddCommand(DistributedTransaction)
}
