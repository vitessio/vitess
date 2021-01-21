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
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/topoproto"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// InitShardPrimary makes an InitShardPrimary gRPC call to a vtctld.
	InitShardPrimary = &cobra.Command{
		Use:  "InitShardPrimary",
		Args: cobra.ExactArgs(2),
		RunE: commandInitShardPrimary,
	}
)

var initShardPrimaryOptions = struct {
	WaitReplicasTimeout time.Duration
	Force               bool
}{}

func commandInitShardPrimary(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	tabletAlias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(1))
	if err != nil {
		return err
	}

	resp, err := client.InitShardPrimary(commandCtx, &vtctldatapb.InitShardPrimaryRequest{
		Keyspace:                keyspace,
		Shard:                   shard,
		PrimaryElectTabletAlias: tabletAlias,
		WaitReplicasTimeout:     ptypes.DurationProto(initShardPrimaryOptions.WaitReplicasTimeout),
		Force:                   initShardPrimaryOptions.Force,
	})

	for _, event := range resp.Events {
		log.Infof("%v", event)
	}

	return err
}

func init() {
	InitShardPrimary.Flags().DurationVar(&initShardPrimaryOptions.WaitReplicasTimeout, "wait-replicas-timeout", 30*time.Second, "time to wait for replicas to catch up in reparenting")
	InitShardPrimary.Flags().BoolVar(&initShardPrimaryOptions.Force, "force", false, "will force the reparent even if the provided tablet is not a master or the shard master")
	Root.AddCommand(InitShardPrimary)
}
