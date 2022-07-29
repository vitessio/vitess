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
	"flag"
	"fmt"
	"time"

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
		params: "[--concurrency=4] [--allow_primary=false] <tablet alias>",
		help:   "Stops mysqld and uses the BackupStorage service to store a new backup. This function also remembers if the tablet was replicating so that it can restore the same state after the backup completes.",
	})
	addCommand("Tablets", command{
		name:   "RestoreFromBackup",
		method: commandRestoreFromBackup,
		params: "[--backup_timestamp=yyyy-MM-dd.HHmmss] <tablet alias>",
		help:   "Stops mysqld and restores the data from the latest backup or if a timestamp is specified then the most recent backup at or before that time.",
	})
}

func commandBackup(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	concurrency := subFlags.Int("concurrency", 4, "Specifies the number of compression/checksum jobs to run simultaneously")
	allowPrimary := subFlags.Bool("allow_primary", false, "Allows backups to be taken on primary. Warning!! If you are using the builtin backup engine, this will shutdown your primary mysql for as long as it takes to create a backup.")

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
		TabletAlias:  tabletAlias,
		Concurrency:  uint64(*concurrency),
		AllowPrimary: *allowPrimary,
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

func commandBackupShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	concurrency := subFlags.Int("concurrency", 4, "Specifies the number of compression/checksum jobs to run simultaneously")
	allowPrimary := subFlags.Bool("allow_primary", false, "Whether to use primary tablet for backup. Warning!! If you are using the builtin backup engine, this will shutdown your primary mysql for as long as it takes to create a backup.")

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
		Keyspace:     keyspace,
		Shard:        shard,
		Concurrency:  uint64(*concurrency),
		AllowPrimary: *allowPrimary,
	}, &backupEventStreamLogger{logger: wr.Logger(), ctx: ctx})
}

func commandListBackups(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
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

func commandRemoveBackup(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
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

func commandRestoreFromBackup(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	backupTimestampStr := subFlags.String("backup_timestamp", "", "Use the backup taken at or before this timestamp rather than using the latest backup.")
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

	req := &vtctldatapb.RestoreFromBackupRequest{
		TabletAlias: tabletAlias,
	}

	if !backupTime.IsZero() {
		req.BackupTime = protoutil.TimeToProto(backupTime)
	}

	return wr.VtctldServer().RestoreFromBackup(req, &backupRestoreEventStreamLogger{logger: wr.Logger(), ctx: ctx})
}
