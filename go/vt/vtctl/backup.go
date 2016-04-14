// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtctl

import (
	"flag"
	"fmt"

	"github.com/youtube/vitess/go/vt/mysqlctl/backupstorage"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
)

func init() {
	addCommand("Shards", command{
		"ListBackups",
		commandListBackups,
		"<keyspace/shard>",
		"Lists all the backups for a shard."})
	addCommand("Shards", command{
		"RemoveBackup",
		commandRemoveBackup,
		"<keyspace/shard> <backup name>",
		"Removes a backup for the BackupStorage."})
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
	bhs, err := bs.ListBackups(bucket)
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
	return bs.RemoveBackup(bucket, name)
}
