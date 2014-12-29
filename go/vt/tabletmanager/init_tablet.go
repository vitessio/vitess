// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

// This file handles the initialization of the tablet at startup time.
// It is only enabled if init_tablet_type or init_keyspace is set.

import (
	"flag"
	"fmt"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/flagutil"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"golang.org/x/net/context"
)

var (
	initDbNameOverride = flag.String("init_db_name_override", "", "(init parameter) override the name of the db used by vttablet")
	initKeyspace       = flag.String("init_keyspace", "", "(init parameter) keyspace to use for this tablet")
	initShard          = flag.String("init_shard", "", "(init parameter) shard to use for this tablet")
	initTags           flagutil.StringMapValue
	initTabletType     = flag.String("init_tablet_type", "", "(init parameter) the tablet type to use for this tablet. Incompatible with target_tablet_type.")
)

func init() {
	flag.Var(&initTags, "init_tags", "(init parameter) comma separated list of key:value pairs used to tag the tablet")
}

// InitTablet initializes the tablet record if necessary.
func (agent *ActionAgent) InitTablet(port, securePort int) error {
	// only enabled if one of init_tablet_type (when healthcheck
	// is disabled) or init_keyspace (when healthcheck is enabled)
	// is passed in, then check other parameters
	if *initTabletType == "" && *initKeyspace == "" {
		return nil
	}

	// figure out our default target type
	var tabletType topo.TabletType
	if *initTabletType != "" {
		if *targetTabletType != "" {
			log.Fatalf("cannot specify both target_tablet_type and init_tablet_type parameters (as they might conflict)")
		}

		// use the type specified on the command line
		tabletType = topo.TabletType(*initTabletType)
		if !topo.IsTypeInList(tabletType, topo.AllTabletTypes) {
			log.Fatalf("InitTablet encountered unknown init_tablet_type '%v'", *initTabletType)
		}
		if tabletType == topo.TYPE_MASTER || tabletType == topo.TYPE_SCRAP {
			// We disallow TYPE_MASTER, so we don't have to change
			// shard.MasterAlias, and deal with the corner cases.
			// We also disallow TYPE_SCRAP, obviously.
			log.Fatalf("init_tablet_type cannot be %v", tabletType)
		}

	} else if *targetTabletType != "" {
		// use spare, the healthcheck will turn us into what
		// we need to be eventually
		tabletType = topo.TYPE_SPARE

	} else {
		log.Fatalf("if init tablet is enabled, one of init_tablet_type or target_tablet_type needs to be specified")
	}

	// if we're assigned to a shard, make sure it exists, see if
	// we are its master, and update its cells list if necessary
	if tabletType != topo.TYPE_IDLE {
		if *initKeyspace == "" || *initShard == "" {
			log.Fatalf("if init tablet is enabled and the target type is not idle, init_keyspace and init_shard also need to be specified")
		}
		shard, _, err := topo.ValidateShardName(*initShard)
		if err != nil {
			log.Fatalf("cannot validate shard name: %v", err)
		}

		log.Infof("Reading shard record %v/%v", *initKeyspace, shard)

		// read the shard, create it if necessary
		si, err := agent.TopoServer.GetShard(*initKeyspace, shard)
		if err == topo.ErrNoNode {
			// create the keyspace, maybe it already exists
			if err := agent.TopoServer.CreateKeyspace(*initKeyspace, &topo.Keyspace{}); err != nil && err != topo.ErrNodeExists {
				return fmt.Errorf("CreateKeyspace(%v) failed: %v", *initKeyspace, err)
			}

			// create the shard
			if err := topo.CreateShard(agent.TopoServer, *initKeyspace, shard); err != nil {
				return fmt.Errorf("CreateShard(%v/%v) failed: %v", *initKeyspace, shard, err)
			}

			// and re-read the shard object
			si, err = agent.TopoServer.GetShard(*initKeyspace, shard)
		}
		if err != nil {
			return fmt.Errorf("InitTablet cannot read shard: %v", err)
		}
		if si.MasterAlias == agent.TabletAlias {
			// we are the current master for this shard (probably
			// means the master tablet process was just restarted),
			// so InitTablet as master.
			tabletType = topo.TYPE_MASTER
		}

		// See if we need to add the tablet's cell to the shard's cell
		// list.  If we do, it has to be under the shard lock.
		if !si.HasCell(agent.TabletAlias.Cell) {
			actionNode := actionnode.UpdateShard()
			ctx, cancel := context.WithTimeout(agent.batchCtx, agent.LockTimeout)
			defer cancel()
			lockPath, err := actionNode.LockShard(ctx, agent.TopoServer, *initKeyspace, shard)
			if err != nil {
				return fmt.Errorf("LockShard(%v/%v) failed: %v", *initKeyspace, shard, err)
			}

			// re-read the shard with the lock
			si, err = agent.TopoServer.GetShard(*initKeyspace, shard)
			if err != nil {
				return actionNode.UnlockShard(ctx, agent.TopoServer, *initKeyspace, shard, lockPath, err)
			}

			// see if we really need to update it now
			if !si.HasCell(agent.TabletAlias.Cell) {
				si.Cells = append(si.Cells, agent.TabletAlias.Cell)

				// write it back
				if err := topo.UpdateShard(ctx, agent.TopoServer, si); err != nil {
					return actionNode.UnlockShard(ctx, agent.TopoServer, *initKeyspace, shard, lockPath, err)
				}
			}

			// and unlock
			if err := actionNode.UnlockShard(ctx, agent.TopoServer, *initKeyspace, shard, lockPath, nil); err != nil {
				return err
			}
		}
	}
	log.Infof("Initializing the tablet for type %v", tabletType)

	// figure out the hostname
	hostname := *tabletHostname
	if hostname == "" {
		var err error
		hostname, err = netutil.FullyQualifiedHostname()
		if err != nil {
			return err
		}
	}

	// create and populate tablet record
	tablet := &topo.Tablet{
		Alias:          agent.TabletAlias,
		Hostname:       hostname,
		Portmap:        make(map[string]int),
		Keyspace:       *initKeyspace,
		Shard:          *initShard,
		Type:           tabletType,
		DbNameOverride: *initDbNameOverride,
		Tags:           initTags,
	}
	if port != 0 {
		tablet.Portmap["vt"] = port
	}
	if securePort != 0 {
		tablet.Portmap["vts"] = securePort
	}
	if err := tablet.Complete(); err != nil {
		return fmt.Errorf("InitTablet tablet.Complete failed: %v", err)
	}

	// now try to create the record
	err := topo.CreateTablet(agent.TopoServer, tablet)
	switch err {
	case nil:
		// it worked, we're good, can update the replication graph
		if tablet.IsInReplicationGraph() {
			if err := topo.UpdateTabletReplicationData(agent.batchCtx, agent.TopoServer, tablet); err != nil {
				return fmt.Errorf("UpdateTabletReplicationData failed: %v", err)
			}
		}

	case topo.ErrNodeExists:
		// The node already exists, will just try to update
		// it. So we read it first.
		oldTablet, err := agent.TopoServer.GetTablet(tablet.Alias)
		if err != nil {
			fmt.Errorf("InitTablet failed to read existing tablet record: %v", err)
		}

		// Sanity check the keyspace and shard
		if oldTablet.Keyspace != tablet.Keyspace || oldTablet.Shard != tablet.Shard {
			return fmt.Errorf("InitTablet failed because existing tablet keyspace and shard %v/%v differ from the provided ones %v/%v", oldTablet.Keyspace, oldTablet.Shard, tablet.Keyspace, tablet.Shard)
		}

		// And overwrite the rest
		*(oldTablet.Tablet) = *tablet
		if err := topo.UpdateTablet(agent.batchCtx, agent.TopoServer, oldTablet); err != nil {
			return fmt.Errorf("UpdateTablet failed: %v", err)
		}

		// Note we don't need to UpdateTabletReplicationData
		// as the tablet already existed with the right data
		// in the replication graph
	default:
		return fmt.Errorf("CreateTablet failed: %v", err)
	}

	// and now rebuild the serving graph. Note we do that in any case,
	// to clean any inaccurate record from any part of the serving graph.
	if tabletType != topo.TYPE_IDLE {
		if _, err := topotools.RebuildShard(agent.batchCtx, logutil.NewConsoleLogger(), agent.TopoServer, tablet.Keyspace, tablet.Shard, []string{tablet.Alias.Cell}, agent.LockTimeout); err != nil {
			return fmt.Errorf("RebuildShard failed: %v", err)
		}
	}

	return nil
}
