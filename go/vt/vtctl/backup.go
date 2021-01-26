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
	"errors"
	"flag"
	"fmt"
	"io"

	"context"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/wrangler"
)

func init() {
	addCommand("Shards", command{
		"ListBackups",
		commandListBackups,
		"<keyspace/shard>",
		"Lists all the backups for a shard."})
	addCommand("Shards", command{
		"BackupShard",
		commandBackupShard,
		"[-allow_master=false] <keyspace/shard>",
		"Chooses a tablet and creates a backup for a shard."})
	addCommand("Shards", command{
		"RemoveBackup",
		commandRemoveBackup,
		"<keyspace/shard> <backup name>",
		"Removes a backup for the BackupStorage."})

	addCommand("Tablets", command{
		"Backup",
		commandBackup,
		"[-concurrency=4] [-allow_master=false] <tablet alias>",
		"Stops mysqld and uses the BackupStorage service to store a new backup. This function also remembers if the tablet was replicating so that it can restore the same state after the backup completes."})
	addCommand("Tablets", command{
		"RestoreFromBackup",
		commandRestoreFromBackup,
		"<tablet alias>",
		"Stops mysqld and restores the data from the latest backup."})
}

func commandBackup(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	concurrency := subFlags.Int("concurrency", 4, "Specifies the number of compression/checksum jobs to run simultaneously")
	allowMaster := subFlags.Bool("allow_master", false, "Allows backups to be taken on master. Warning!! If you are using the builtin backup engine, this will shutdown your master mysql for as long as it takes to create a backup ")

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

	return execBackup(ctx, wr, tabletInfo.Tablet, *concurrency, *allowMaster)
}

func commandBackupShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	concurrency := subFlags.Int("concurrency", 4, "Specifies the number of compression/checksum jobs to run simultaneously")
	allowMaster := subFlags.Bool("allow_master", false, "Whether to use master tablet for backup. Warning!! If you are using the builtin backup engine, this will shutdown your master mysql for as long as it takes to create a backup ")

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
	if tablets == nil {
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
			secondsBehind = stats[i].SecondsBehindMaster
			continue
		}

		// choose a new tablet if it is more up to date
		if stats[i].SecondsBehindMaster < secondsBehind {
			tabletForBackup = tablets[i].Tablet
			secondsBehind = stats[i].SecondsBehindMaster
		}
	}

	// if no other tablet is available and allowMaster is set to true
	if tabletForBackup == nil && *allowMaster {
	ChooseMaster:
		for i := range tablets {
			switch tablets[i].Type {
			case topodatapb.TabletType_MASTER:
				tabletForBackup = tablets[i].Tablet
				secondsBehind = 0 //nolint
				break ChooseMaster
			default:
				continue
			}
		}
	}

	if tabletForBackup == nil {
		return errors.New("no tablet available for backup")
	}

	return execBackup(ctx, wr, tabletForBackup, *concurrency, *allowMaster)
}

// execBackup is shared by Backup and BackupShard
func execBackup(ctx context.Context, wr *wrangler.Wrangler, tablet *topodatapb.Tablet, concurrency int, allowMaster bool) error {
	stream, err := wr.TabletManagerClient().Backup(ctx, tablet, concurrency, allowMaster)
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
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the RestoreFromBackup command requires the <tablet alias> argument")
	}

	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	stream, err := wr.TabletManagerClient().RestoreFromBackup(ctx, tabletInfo.Tablet)
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
