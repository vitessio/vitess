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
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
		params: "[-allow_primary=false] <keyspace/shard>",
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
		params: "[-concurrency=4] [-allow_primary=false] <tablet alias>",
		help:   "Stops mysqld and uses the BackupStorage service to store a new backup. This function also remembers if the tablet was replicating so that it can restore the same state after the backup completes.",
	})
	addCommand("Tablets", command{
		name:   "RestoreFromBackup",
		method: commandRestoreFromBackup,
		params: "[-backup_timestamp=yyyy-MM-dd.HHmmss] <tablet alias>",
		help:   "Stops mysqld and restores the data from the latest backup or if a timestamp is specified then the most recent backup at or before that time.",
	})
}

func commandBackup(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	concurrency := subFlags.Int("concurrency", 4, "Specifies the number of compression/checksum jobs to run simultaneously")
	allowPrimary := subFlags.Bool("allow_primary", false, "Allows backups to be taken on primary. Warning!! If you are using the builtin backup engine, this will shutdown your primary mysql for as long as it takes to create a backup.")

	// handle deprecated flags
	// should be deleted in a future release
	deprecatedAllowMaster := subFlags.Bool("allow_master", false, "DEPRECATED. Use -allow_primary instead")

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
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	if *deprecatedAllowMaster {
		*allowPrimary = *deprecatedAllowMaster
	}

	return execBackup(ctx, wr, tabletInfo.Tablet, *concurrency, *allowPrimary)
}

func commandBackupShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	concurrency := subFlags.Int("concurrency", 4, "Specifies the number of compression/checksum jobs to run simultaneously")
	allowPrimary := subFlags.Bool("allow_primary", false, "Whether to use primary tablet for backup. Warning!! If you are using the builtin backup engine, this will shutdown your primary mysql for as long as it takes to create a backup.")

	// handle deprecated flags
	// should be deleted in a future release
	deprecatedAllowMaster := subFlags.Bool("allow_master", false, "DEPRECATED. Use -allow_primary instead")

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

	tablets, stats, err := wr.ShardReplicationStatuses(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	var tabletForBackup *topodatapb.Tablet
	var secondsBehind uint32

	for i := range tablets {
		// find a replica, rdonly or spare tablet type to run the backup on
		switch tablets[i].Type {
		case topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY, topodatapb.TabletType_SPARE:
		default:
			continue
		}
		// choose the first tablet as the baseline
		if tabletForBackup == nil {
			tabletForBackup = tablets[i].Tablet
			secondsBehind = stats[i].ReplicationLagSeconds
			continue
		}

		// choose a new tablet if it is more up to date
		if stats[i].ReplicationLagSeconds < secondsBehind {
			tabletForBackup = tablets[i].Tablet
			secondsBehind = stats[i].ReplicationLagSeconds
		}
	}

	// if no other tablet is available and allowPrimary is set to true
	if tabletForBackup == nil && *allowPrimary {
	ChooseTablet:
		for i := range tablets {
			switch tablets[i].Type {
			case topodatapb.TabletType_PRIMARY:
				tabletForBackup = tablets[i].Tablet
				secondsBehind = 0 //nolint
				break ChooseTablet
			default:
				continue
			}
		}
	}

	if tabletForBackup == nil {
		return errors.New("no tablet available for backup")
	}

	if *deprecatedAllowMaster {
		*allowPrimary = *deprecatedAllowMaster
	}

	return execBackup(ctx, wr, tabletForBackup, *concurrency, *allowPrimary)
}

// execBackup is shared by Backup and BackupShard
func execBackup(ctx context.Context, wr *wrangler.Wrangler, tablet *topodatapb.Tablet, concurrency int, allowPrimary bool) error {
	stream, err := wr.TabletManagerClient().Backup(ctx, tablet, concurrency, allowPrimary)
	if err != nil {
		return err
	}
	for {
		e, err := stream.Recv()
		switch err {
		case nil:
			logutil.LogEvent(wr.Logger(), e)
		case io.EOF:
			return nil
		default:
			return err
		}
	}
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
	bucket := fmt.Sprintf("%v/%v", keyspace, shard)
	name := subFlags.Arg(1)

	bs, err := backupstorage.GetBackupStorage()
	if err != nil {
		return err
	}
	defer bs.Close()
	return bs.RemoveBackup(ctx, bucket, name)
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
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	stream, err := wr.TabletManagerClient().RestoreFromBackup(ctx, tabletInfo.Tablet, backupTime)
	if err != nil {
		return err
	}
	for {
		e, err := stream.Recv()
		switch err {
		case nil:
			logutil.LogEvent(wr.Logger(), e)
		case io.EOF:
			return nil
		default:
			return err
		}
	}
}
