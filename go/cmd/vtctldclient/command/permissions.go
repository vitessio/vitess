/*
Copyright 2025 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/topo/topoproto"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// GetPermissions makes a GetPermissions gRPC call to a vtctld.
	GetPermissions = &cobra.Command{
		Use:                   "GetPermissions <tablet_alias>",
		Short:                 "Displays the permissions for a tablet.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetPermissions,
	}
	// ValidatePermissionsShard makes a ValidatePermissionsKeyspace gRPC call to a
	// vtctld with the specified shard to examine in the keyspace.
	ValidatePermissionsShard = &cobra.Command{
		Use:                   "ValidatePermissionsShard <keyspace/shard>",
		Short:                 "Validates that the permissions on the primary match all of the replicas.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandValidatePermissionsShard,
	}
	// ValidatePermissionsKeyspace makes a ValidatePermissionsKeyspace gRPC call to a
	// vtctld.
	ValidatePermissionsKeyspace = &cobra.Command{
		Use:                   "ValidatePermissionsKeyspace <keyspace name>",
		Short:                 "Validates that the permissions on the primary of the first shard match those of all of the other tablets in the keyspace.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandValidatePermissionsKeyspace,
	}
)

// redactUserPermissions ensures sensitive info is redacted from a
// *tabletmanagerdatapb.Permissions response.
func redactUserPermissions(perms *tabletmanagerdatapb.Permissions) {
	if perms == nil {
		return
	}
	for _, up := range perms.UserPermissions {
		if up == nil {
			continue
		}
		if up.Privileges != nil {
			// Remove the "authentication_string" field, which is a
			// sensitive field from the mysql.users table. This is
			// redacted server-side in v23+ so this line can be
			// removed in the future.
			delete(up.Privileges, "authentication_string")
		}
	}
}

func commandGetPermissions(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.GetPermissions(commandCtx, &vtctldatapb.GetPermissionsRequest{
		TabletAlias: alias,
	})
	if err != nil {
		return err
	}
	// Obfuscate the secrets so as not to potentially display sensitive info.
	if resp != nil && resp.Permissions != nil {
		redactUserPermissions(resp.Permissions)
	}
	cli.DefaultMarshalOptions.EmitUnpopulated = false
	p, err := cli.MarshalJSON(resp.Permissions)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", p)

	return nil
}

func commandValidatePermissionsKeyspace(cmd *cobra.Command, args []string) error {
	keyspace := cmd.Flags().Arg(0)

	cli.FinishedParsing(cmd)

	_, err := client.ValidatePermissionsKeyspace(commandCtx, &vtctldatapb.ValidatePermissionsKeyspaceRequest{
		Keyspace: keyspace,
	})

	return err
}

func commandValidatePermissionsShard(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	_, err = client.ValidatePermissionsKeyspace(commandCtx, &vtctldatapb.ValidatePermissionsKeyspaceRequest{
		Keyspace: keyspace,
		Shards:   []string{shard},
	})

	return err
}

func init() {
	Root.AddCommand(GetPermissions)
	Root.AddCommand(ValidatePermissionsKeyspace)
	Root.AddCommand(ValidatePermissionsShard)
}
