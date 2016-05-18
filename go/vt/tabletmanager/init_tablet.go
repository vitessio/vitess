// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

// This file handles the initialization of the tablet at startup time.
// It is only enabled if init_tablet_type or init_keyspace is set.

import (
	"flag"
	"fmt"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/flagutil"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topotools"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	initDbNameOverride = flag.String("init_db_name_override", "", "(init parameter) override the name of the db used by vttablet")
	initKeyspace       = flag.String("init_keyspace", "", "(init parameter) keyspace to use for this tablet")
	initShard          = flag.String("init_shard", "", "(init parameter) shard to use for this tablet")
	initTags           flagutil.StringMapValue
	initTabletType     = flag.String("init_tablet_type", "", "(init parameter) the tablet type to use for this tablet. Incompatible with target_tablet_type.")
	initTimeout        = flag.Duration("init_timeout", 1*time.Minute, "(init parameter) timeout to use for the init phase.")
)

func init() {
	flag.Var(&initTags, "init_tags", "(init parameter) comma separated list of key:value pairs used to tag the tablet")
}

// InitTablet initializes the tablet record if necessary.
func (agent *ActionAgent) InitTablet(port, gRPCPort int32) error {
	// only enabled if one of init_tablet_type (when healthcheck
	// is disabled) or init_keyspace (when healthcheck is enabled)
	// is passed in, then check other parameters
	if *initTabletType == "" && *initKeyspace == "" {
		return nil
	}

	// figure out our default target type
	var tabletType topodatapb.TabletType
	if *initTabletType != "" {
		if *targetTabletType != "" {
			log.Fatalf("cannot specify both target_tablet_type and init_tablet_type parameters (as they might conflict)")
		}

		// use the type specified on the command line
		var err error
		tabletType, err = topoproto.ParseTabletType(*initTabletType)
		if err != nil {
			log.Fatalf("Invalid init_tablet_type %v: %v", *initTabletType, err)
		}

		if tabletType == topodatapb.TabletType_MASTER {
			// We disallow MASTER, so we don't have to change
			// shard.MasterAlias, and deal with the corner cases.
			log.Fatalf("init_tablet_type cannot be master, use replica instead")
		}

	} else if *targetTabletType != "" {
		// use the targetTabletType, check it's not master.
		// FIXME(alainjobart): refactor the flags: we should switch
		// to init_tablet_type, and always enable healthcheck.
		var err error
		tabletType, err = topoproto.ParseTabletType(*targetTabletType)
		if err != nil {
			log.Fatalf("Invalid target_tablet_type %v: %v", *targetTabletType, err)
		}

		if tabletType == topodatapb.TabletType_MASTER {
			// We disallow MASTER, so we don't have to change
			// shard.MasterAlias, and deal with the corner cases.
			log.Fatalf("target_tablet_type cannot be master. Use replica instead.")
		}
	} else {
		log.Fatalf("if init tablet is enabled (by specifying init_keyspace), one of init_tablet_type or target_tablet_type needs to be specified")
	}

	// create a context for this whole operation
	ctx, cancel := context.WithTimeout(agent.batchCtx, *initTimeout)
	defer cancel()

	// since we're assigned to a shard, make sure it exists, see if
	// we are its master, and update its cells list if necessary
	if *initKeyspace == "" || *initShard == "" {
		log.Fatalf("if init tablet is enabled and the target type is not idle, init_keyspace and init_shard also need to be specified")
	}
	shard, _, err := topo.ValidateShardName(*initShard)
	if err != nil {
		log.Fatalf("cannot validate shard name: %v", err)
	}

	log.Infof("Reading shard record %v/%v", *initKeyspace, shard)

	// read the shard, create it if necessary
	si, err := topotools.GetOrCreateShard(ctx, agent.TopoServer, *initKeyspace, shard)
	if err != nil {
		return fmt.Errorf("InitTablet cannot GetOrCreateShard shard: %v", err)
	}
	if si.MasterAlias != nil && topoproto.TabletAliasEqual(si.MasterAlias, agent.TabletAlias) {
		// We're marked as master in the shard record, which could mean the master
		// tablet process was just restarted. However, we need to check if a new
		// master is in the process of taking over. In that case, it will let us
		// know by forcibly updating the old master's tablet record.
		oldTablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
		switch err {
		case topo.ErrNoNode:
			// There's no existing tablet record, so we can assume
			// no one has left us a message to step down.
			tabletType = topodatapb.TabletType_MASTER
		case nil:
			if oldTablet.Type == topodatapb.TabletType_MASTER {
				// We're marked as master in the shard record,
				// and our existing tablet record agrees.
				tabletType = topodatapb.TabletType_MASTER
			}
		default:
			return fmt.Errorf("InitTablet failed to read existing tablet record: %v", err)
		}
	}

	// See if we need to add the tablet's cell to the shard's cell list.
	if !si.HasCell(agent.TabletAlias.Cell) {
		si, err = agent.TopoServer.UpdateShardFields(ctx, *initKeyspace, shard, func(shard *topodatapb.Shard) error {
			if topoproto.ShardHasCell(shard, agent.TabletAlias.Cell) {
				// Someone else already did it.
				return topo.ErrNoUpdateNeeded
			}
			shard.Cells = append(shard.Cells, agent.TabletAlias.Cell)
			return nil
		})
		if err != nil {
			return fmt.Errorf("couldn't add tablet's cell to shard record: %v", err)
		}
	}
	log.Infof("Initializing the tablet for type %v", tabletType)

	// figure out the hostname
	hostname := *tabletHostname
	if hostname != "" {
		log.Infof("Using hostname: %v from -tablet_hostname flag.", hostname)
	} else {
		hostname, err := netutil.FullyQualifiedHostname()
		if err != nil {
			return err
		}
		log.Infof("Using detected machine hostname: %v To change this, fix your machine network configuration or override it with -tablet_hostname.", hostname)
	}

	// create and populate tablet record
	tablet := &topodatapb.Tablet{
		Alias:          agent.TabletAlias,
		Hostname:       hostname,
		PortMap:        make(map[string]int32),
		Keyspace:       *initKeyspace,
		Shard:          *initShard,
		Type:           tabletType,
		DbNameOverride: *initDbNameOverride,
		Tags:           initTags,
	}
	if port != 0 {
		tablet.PortMap["vt"] = port
	}
	if gRPCPort != 0 {
		tablet.PortMap["grpc"] = gRPCPort
	}
	if err := topo.TabletComplete(tablet); err != nil {
		return fmt.Errorf("InitTablet TabletComplete failed: %v", err)
	}

	// now try to create the record
	err = agent.TopoServer.CreateTablet(ctx, tablet)
	switch err {
	case nil:
		// it worked, we're good, can update the replication graph
		if err := topo.UpdateTabletReplicationData(ctx, agent.TopoServer, tablet); err != nil {
			return fmt.Errorf("UpdateTabletReplicationData failed: %v", err)
		}

	case topo.ErrNodeExists:
		// The node already exists, will just try to update it. So we read it first.
		oldTablet, err := agent.TopoServer.GetTablet(ctx, tablet.Alias)
		if err != nil {
			return fmt.Errorf("InitTablet failed to read existing tablet record: %v", err)
		}

		// Sanity check the keyspace and shard
		if oldTablet.Keyspace != tablet.Keyspace || oldTablet.Shard != tablet.Shard {
			return fmt.Errorf("InitTablet failed because existing tablet keyspace and shard %v/%v differ from the provided ones %v/%v", oldTablet.Keyspace, oldTablet.Shard, tablet.Keyspace, tablet.Shard)
		}

		// Then overwrite everything, ignoring version mismatch.
		if err := agent.TopoServer.UpdateTablet(ctx, topo.NewTabletInfo(tablet, -1)); err != nil {
			return fmt.Errorf("UpdateTablet failed: %v", err)
		}

		// Note we don't need to UpdateTabletReplicationData
		// as the tablet already existed with the right data
		// in the replication graph
	default:
		return fmt.Errorf("CreateTablet failed: %v", err)
	}

	return nil
}
