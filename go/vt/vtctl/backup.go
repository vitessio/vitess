/*
Copyright 2019 The Vitess Authors.

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

package vtctl

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/wrangler"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func init() {
	addCommand("Shards", command{
		name:   "ListBackups",
		method: commandListBackups,
		params: "<keyspace/shard>",
		help:   "Lists all the backups for a shard.",
	})
	addCommand("Shards", command{
		name:   "BackupShard",
		method: commandBackupShard,
		params: "[--allow_primary=false] <keyspace/shard>",
		help:   "Chooses a tablet and creates a backup for a shard.",
	})
	addCommand("Shards", command{
		name:   "RemoveBackup",
		method: commandRemoveBackup,
		params: "<keyspace/shard> <backup name>",
		help:   "Removes a backup for the BackupStorage.",
	})
	addCommand("Tablets", command{
		name:   "Backup",
		method: commandBackup,
		params: "[--concurrency=4] [--allow_primary=false] [--incremental_from_pos=<pos>] <tablet alias>",
		help:   "Run a full or an incremental backup. Uses the BackupStorage service to store a new backup. With full backup, stops mysqld, takes the backup, starts mysqld and resumes replication. With incremental backup (indicated by '--incremental_from_pos', rotate and copy binary logs without disrupting the mysqld service).",
	})
	addCommand("Tablets", command{
		name:   "RestoreFromBackup",
		method: commandRestoreFromBackup,
		params: "[--backup_timestamp=yyyy-MM-dd.HHmmss] [--restore_to_pos=<pos>] [--dry_run] <tablet alias>",
		help:   "Stops mysqld and restores the data from the latest backup or if a timestamp is specified then the most recent backup at or before that time. If '--restore_to_pos' is given, then a point in time restore based on one full backup followed by zero or more incremental backups. dry-run only validates restore steps without actually restoring data",
	})
}

func commandBackup(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	concurrency := subFlags.Int32("concurrency", 4, "Specifies the number of compression/checksum jobs to run simultaneously")
	allowPrimary := subFlags.Bool("allow_primary", false, "Allows backups to be taken on primary. Warning!! If you are using the builtin backup engine, this will shutdown your primary mysql for as long as it takes to create a backup.")
	incrementalFromPos := subFlags.String("incremental_from_pos", "", "Position, or name of backup from which to create an incremental backup. Default: empty. If given, then this backup becomes an incremental backup from given position or given backup. If value is 'auto', this backup will be taken from the last successful backup position.")
	upgradeSafe := subFlags.Bool("upgrade-safe", false, "Whether to use innodb_fast_shutdown=0 for the backup so it is safe to use for MySQL upgrades.")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the Backup command requires the <tablet alias> argument")
	}

	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}

	return wr.VtctldServer().Backup(&vtctldatapb.BackupRequest{
		TabletAlias:        tabletAlias,
		Concurrency:        *concurrency,
		AllowPrimary:       *allowPrimary,
		IncrementalFromPos: *incrementalFromPos,
		UpgradeSafe:        *upgradeSafe,
	}, &backupEventStreamLogger{logger: wr.Logger(), ctx: ctx})
}

// backupEventStreamLogger takes backup events from the vtctldserver and emits
// them via logutil.LogEvent, preserving legacy behavior.
type backupEventStreamLogger struct {
	grpc.ServerStream
	logger logutil.Logger
	ctx    context.Context
}

func (b *backupEventStreamLogger) Context() context.Context { return b.ctx }

func (b *backupEventStreamLogger) Send(resp *vtctldatapb.BackupResponse) error {
	logutil.LogEvent(b.logger, resp.Event)
	return nil
}

func commandBackupShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	concurrency := subFlags.Int32("concurrency", 4, "Specifies the number of compression/checksum jobs to run simultaneously")
	allowPrimary := subFlags.Bool("allow_primary", false, "Whether to use primary tablet for backup. Warning!! If you are using the builtin backup engine, this will shutdown your primary mysql for as long as it takes to create a backup.")
	incrementalFromPos := subFlags.String("incremental_from_pos", "", "Position, or name of backup from which to create an incremental backup. Default: empty. If given, then this backup becomes an incremental backup from given position or given backup. If value is 'auto', this backup will be taken from the last successful backup position.")
	upgradeSafe := subFlags.Bool("upgrade-safe", false, "Whether to use innodb_fast_shutdown=0 for the backup so it is safe to use for MySQL upgrades.")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action BackupShard requires <keyspace/shard>")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}

	return wr.VtctldServer().BackupShard(&vtctldatapb.BackupShardRequest{
		Keyspace:           keyspace,
		Shard:              shard,
		Concurrency:        *concurrency,
		AllowPrimary:       *allowPrimary,
		IncrementalFromPos: *incrementalFromPos,
		UpgradeSafe:        *upgradeSafe,
	}, &backupEventStreamLogger{logger: wr.Logger(), ctx: ctx})
}

func commandListBackups(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ListBackups requires <keyspace/shard>")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	bucket := fmt.Sprintf("%v/%v", keyspace, shard)

	bs, err := backupstorage.GetBackupStorage()
	if err != nil {
		return err
	}
	defer bs.Close()
	bhs, err := bs.ListBackups(ctx, bucket)
	if err != nil {
		return err
	}
	for _, bh := range bhs {
		wr.Logger().Printf("%v\n", bh.Name())
	}
	return nil
}

func commandRemoveBackup(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action RemoveBackup requires <keyspace/shard> <backup name>")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	name := subFlags.Arg(1)

	_, err = wr.VtctldServer().RemoveBackup(ctx, &vtctldatapb.RemoveBackupRequest{
		Keyspace: keyspace,
		Shard:    shard,
		Name:     name,
	})
	return err
}

// backupRestoreEventStreamLogger takes backup restore events from the
// vtctldserver and emits them via logutil.LogEvent, preserving legacy behavior.
type backupRestoreEventStreamLogger struct {
	grpc.ServerStream
	logger logutil.Logger
	ctx    context.Context
}

func (b *backupRestoreEventStreamLogger) Context() context.Context { return b.ctx }

func (b *backupRestoreEventStreamLogger) Send(resp *vtctldatapb.RestoreFromBackupResponse) error {
	logutil.LogEvent(b.logger, resp.Event)
	return nil
}

func commandRestoreFromBackup(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	backupTimestampStr := subFlags.String("backup_timestamp", "", "Use the backup taken at or before this timestamp rather than using the latest backup.")
	restoreToPos := subFlags.String("restore_to_pos", "", "Run a point in time recovery that ends with the given position. This will attempt to use one full backup followed by zero or more incremental backups")
	restoreToTimestampStr := subFlags.String("restore_to_timestamp", "", "Run a point in time recovery that restores up to, and excluding, given timestamp in RFC3339 format (`2006-01-02T15:04:05Z07:00`). This will attempt to use one full backup followed by zero or more incremental backups")
	dryRun := subFlags.Bool("dry_run", false, "Only validate restore steps, do not actually restore data")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the RestoreFromBackup command requires the <tablet alias> argument")
	}

	// Zero date will cause us to use the latest, which is the default
	backupTime := time.Time{}

	// Or if a backup timestamp was specified then we use the last backup taken at or before that time
	if *backupTimestampStr != "" {
		var err error
		backupTime, err = time.Parse(mysqlctl.BackupTimestampFormat, *backupTimestampStr)
		if err != nil {
			return vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, fmt.Sprintf("unable to parse the backup timestamp value provided of '%s'", *backupTimestampStr))
		}
	}

	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}

	var restoreToTimestamp time.Time
	if *restoreToTimestampStr != "" {
		restoreToTimestamp, err = mysqlctl.ParseRFC3339(*restoreToTimestampStr)
		if err != nil {
			return vterrors.Wrapf(err, "parsing --restore_to_timestamp args")
		}
	}
	req := &vtctldatapb.RestoreFromBackupRequest{
		TabletAlias:        tabletAlias,
		RestoreToPos:       *restoreToPos,
		RestoreToTimestamp: protoutil.TimeToProto(restoreToTimestamp),
		DryRun:             *dryRun,
	}

	if !backupTime.IsZero() {
		req.BackupTime = protoutil.TimeToProto(backupTime)
	}

	return wr.VtctldServer().RestoreFromBackup(req, &backupRestoreEventStreamLogger{logger: wr.Logger(), ctx: ctx})
}
