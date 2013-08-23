// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"

	log "github.com/golang/glog"
	_ "github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/wrangler"
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

func commandDemoteMaster(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		log.Fatalf("action DemoteMaster requires <tablet alias|zk tablet path>")
	}
	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	return wr.ActionInitiator().DemoteMaster(tabletAlias)
}

func commandReparentTablet(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		log.Fatalf("action ReparentTablet requires <tablet alias|zk tablet path>")
	}
	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(0))
	return "", wr.ReparentTablet(tabletAlias)
}

func commandReparentShard(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	leaveMasterReadOnly := subFlags.Bool("leave-master-read-only", false, "leaves the master read-only after reparenting")
	force := subFlags.Bool("force", false, "will force the reparent even if the master is already correct")
	subFlags.Parse(args)
	if subFlags.NArg() != 2 {
		log.Fatalf("action ReparentShard requires <keyspace/shard|zk shard path> <tablet alias|zk tablet path>")
	}

	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	tabletAlias := tabletParamToTabletAlias(subFlags.Arg(1))
	return "", wr.ReparentShard(keyspace, shard, tabletAlias, *leaveMasterReadOnly, *force)
}
