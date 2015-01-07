// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtctl

import (
	"flag"
	"fmt"
	"time"

	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
)

func init() {
	addCommand("Tablets", command{
		"DemoteMaster",
		commandDemoteMaster,
		"<tablet alias|zk tablet path>",
		"Demotes a master tablet."})
	addCommand("Tablets", command{
		"ReparentTablet",
		commandReparentTablet,
		"<tablet alias|zk tablet path>",
		"Reparent a tablet to the current master in the shard. This only works if the current slave position matches the last known reparent action."})
	addCommand("Shards", command{
		"ReparentShard",
		commandReparentShard,
		"[-force] [-leave-master-read-only] <keyspace/shard|zk shard path> <tablet alias|zk tablet path>",
		"Specify which shard to reparent and which tablet should be the new master."})
}

func commandDemoteMaster(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action DemoteMaster requires <tablet alias|zk tablet path>")
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletInfo, err := wr.TopoServer().GetTablet(tabletAlias)
	if err != nil {
		return err
	}
	return wr.TabletManagerClient().DemoteMaster(ctx, tabletInfo)
}

func commandReparentTablet(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ReparentTablet requires <tablet alias|zk tablet path>")
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ReparentTablet(ctx, tabletAlias)
}

func commandReparentShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	leaveMasterReadOnly := subFlags.Bool("leave-master-read-only", false, "leaves the master read-only after reparenting")
	force := subFlags.Bool("force", false, "will force the reparent even if the master is already correct")
	waitSlaveTimeout := subFlags.Duration("wait_slave_timeout", 30*time.Second, "time to wait for slaves to catch up in reparenting")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action ReparentShard requires <keyspace/shard|zk shard path> <tablet alias|zk tablet path>")
	}

	keyspace, shard, err := shardParamToKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletAlias, err := tabletParamToTabletAlias(subFlags.Arg(1))
	if err != nil {
		return err
	}
	return wr.ReparentShard(ctx, keyspace, shard, tabletAlias, *leaveMasterReadOnly, *force, *waitSlaveTimeout)
}
