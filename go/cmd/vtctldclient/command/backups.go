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
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/topoproto"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// Backup makes a Backup gRPC call to a vtctld.
	Backup = &cobra.Command{
		Use:                   "Backup [--concurrency <concurrency>] [--allow-primary] [--upgrade-safe] <tablet_alias>",
		Short:                 "Uses the BackupStorage service on the given tablet to create and store a new backup.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandBackup,
	}
	// BackupShard makes a BackupShard gRPC call to a vtctld.
	BackupShard = &cobra.Command{
		Use:   "BackupShard [--concurrency <concurrency>] [--allow-primary] [--upgrade-safe] <keyspace/shard>",
		Short: "Finds the most up-to-date REPLICA, RDONLY, or SPARE tablet in the given shard and uses the BackupStorage service on that tablet to create and store a new backup.",
		Long: `Finds the most up-to-date REPLICA, RDONLY, or SPARE tablet in the given shard and uses the BackupStorage service on that tablet to create and store a new backup.

If no replica-type tablet can be found, the backup can be taken on the primary if --allow-primary is specified.`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandBackupShard,
	}
	// GetBackups makes a GetBackups gRPC call to a vtctld.
	GetBackups = &cobra.Command{
		Use:                   "GetBackups [--limit <limit>] [--json] <keyspace/shard>",
		Short:                 "Lists backups for the given shard.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetBackups,
	}
	// RemoveBackup makes a RemoveBackup gRPC call to a vtctld.
	RemoveBackup = &cobra.Command{
		Use:                   "RemoveBackup <keyspace/shard> <backup name>",
		Short:                 "Removes the given backup from the BackupStorage used by vtctld.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandRemoveBackup,
	}
	// RestoreFromBackup makes a RestoreFromBackup gRPC call to a vtctld.
	RestoreFromBackup = &cobra.Command{
		Use:                   "RestoreFromBackup [--backup-timestamp|-t <YYYY-mm-DD.HHMMSS>] <tablet_alias>",
		Short:                 "Stops mysqld on the specified tablet and restores the data from either the latest backup or closest before `backup-timestamp`.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandRestoreFromBackup,
	}
)

var backupOptions = struct {
	AllowPrimary bool
	Concurrency  uint64
	UpgradeSafe  bool
}{}

func commandBackup(cmd *cobra.Command, args []string) error {
	tabletAlias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	stream, err := client.Backup(commandCtx, &vtctldatapb.BackupRequest{
		TabletAlias:  tabletAlias,
		AllowPrimary: backupOptions.AllowPrimary,
		Concurrency:  backupOptions.Concurrency,
		UpgradeSafe:  backupOptions.UpgradeSafe,
	})
	if err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		switch err {
		case nil:
			fmt.Printf("%s/%s (%s): %v\n", resp.Keyspace, resp.Shard, topoproto.TabletAliasString(resp.TabletAlias), resp.Event)
		case io.EOF:
			return nil
		default:
			return err
		}
	}
}

var backupShardOptions = struct {
	AllowPrimary bool
	Concurrency  uint64
	UpgradeSafe  bool
}{}

func commandBackupShard(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	stream, err := client.BackupShard(commandCtx, &vtctldatapb.BackupShardRequest{
		Keyspace:     keyspace,
		Shard:        shard,
		AllowPrimary: backupOptions.AllowPrimary,
		Concurrency:  backupOptions.Concurrency,
		UpgradeSafe:  backupOptions.UpgradeSafe,
	})
	if err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		switch err {
		case nil:
			fmt.Printf("%s/%s (%s): %v\n", resp.Keyspace, resp.Shard, topoproto.TabletAliasString(resp.TabletAlias), resp.Event)
		case io.EOF:
			return nil
		default:
			return err
		}
	}
}

var getBackupsOptions = struct {
	Limit      uint32
	OutputJSON bool
}{}

func commandGetBackups(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.GetBackups(commandCtx, &vtctldatapb.GetBackupsRequest{
		Keyspace: keyspace,
		Shard:    shard,
		Limit:    getBackupsOptions.Limit,
	})
	if err != nil {
		return err
	}

	if getBackupsOptions.OutputJSON {
		data, err := cli.MarshalJSON(resp)
		if err != nil {
			return err
		}

		fmt.Printf("%s\n", data)
		return nil
	}

	names := make([]string, len(resp.Backups))
	for i, b := range resp.Backups {
		names[i] = b.Name
	}

	fmt.Printf("%s\n", strings.Join(names, "\n"))

	return nil
}

func commandRemoveBackup(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	name := cmd.Flags().Arg(1)

	cli.FinishedParsing(cmd)

	_, err = client.RemoveBackup(commandCtx, &vtctldatapb.RemoveBackupRequest{
		Keyspace: keyspace,
		Shard:    shard,
		Name:     name,
	})
	return err
}

var restoreFromBackupOptions = struct {
	BackupTimestamp string
}{}

func commandRestoreFromBackup(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	req := &vtctldatapb.RestoreFromBackupRequest{
		TabletAlias: alias,
	}

	if restoreFromBackupOptions.BackupTimestamp != "" {
		t, err := time.Parse(mysqlctl.BackupTimestampFormat, restoreFromBackupOptions.BackupTimestamp)
		if err != nil {
			return err
		}

		req.BackupTime = protoutil.TimeToProto(t)
	}

	cli.FinishedParsing(cmd)

	stream, err := client.RestoreFromBackup(commandCtx, req)
	if err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		switch err {
		case nil:
			fmt.Printf("%s/%s (%s): %v\n", resp.Keyspace, resp.Shard, topoproto.TabletAliasString(resp.TabletAlias), resp.Event)
		case io.EOF:
			return nil
		default:
			return err
		}
	}
}

func init() {
	Backup.Flags().BoolVar(&backupOptions.AllowPrimary, "allow-primary", false, "Allow the primary of a shard to be used for the backup. WARNING: If using the builtin backup engine, this will shutdown mysqld on the primary and stop writes for the duration of the backup.")
	Backup.Flags().Uint64Var(&backupOptions.Concurrency, "concurrency", 4, "Specifies the number of compression/checksum jobs to run simultaneously.")
	Backup.Flags().BoolVar(&backupOptions.UpgradeSafe, "upgrade-safe", false, "Whether to use innodb_fast_shutdown=0 for the backup so it is safe to use for MySQL upgrades.")
	Root.AddCommand(Backup)

	BackupShard.Flags().BoolVar(&backupShardOptions.AllowPrimary, "allow-primary", false, "Allow the primary of a shard to be used for the backup. WARNING: If using the builtin backup engine, this will shutdown mysqld on the primary and stop writes for the duration of the backup.")
	BackupShard.Flags().Uint64Var(&backupShardOptions.Concurrency, "concurrency", 4, "Specifies the number of compression/checksum jobs to run simultaneously.")
	BackupShard.Flags().BoolVar(&backupOptions.UpgradeSafe, "upgrade-safe", false, "Whether to use innodb_fast_shutdown=0 for the backup so it is safe to use for MySQL upgrades.")
	Root.AddCommand(BackupShard)

	GetBackups.Flags().Uint32VarP(&getBackupsOptions.Limit, "limit", "l", 0, "Retrieve only the most recent N backups.")
	GetBackups.Flags().BoolVarP(&getBackupsOptions.OutputJSON, "json", "j", false, "Output backup info in JSON format rather than a list of backups.")
	Root.AddCommand(GetBackups)

	Root.AddCommand(RemoveBackup)

	RestoreFromBackup.Flags().StringVarP(&restoreFromBackupOptions.BackupTimestamp, "backup-timestamp", "t", "", "Use the backup taken at, or closest before, this timestamp. Omit to use the latest backup. Timestamp format is \"YYYY-mm-DD.HHMMSS\".")
	Root.AddCommand(RestoreFromBackup)
}
