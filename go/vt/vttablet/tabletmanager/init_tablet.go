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
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	initDbNameOverride = flag.String("init_db_name_override", "", "(init parameter) override the name of the db used by vttablet")
	initKeyspace       = flag.String("init_keyspace", "", "(init parameter) keyspace to use for this tablet")
	initShard          = flag.String("init_shard", "", "(init parameter) shard to use for this tablet")
	initTags           flagutil.StringMapValue
	initTabletType     = flag.String("init_tablet_type", "", "(init parameter) the tablet type to use for this tablet.")
	initTimeout        = flag.Duration("init_timeout", 1*time.Minute, "(init parameter) timeout to use for the init phase.")
)

func init() {
	flag.Var(&initTags, "init_tags", "(init parameter) comma separated list of key:value pairs used to tag the tablet")
}

// InitTablet initializes the tablet record if necessary.
func (agent *ActionAgent) InitTablet(port, gRPCPort int32) error {
	// it should be either we have all three of init_keyspace,
	// init_shard and init_tablet_type, or none.
	if *initKeyspace == "" && *initShard == "" && *initTabletType == "" {
		// not initializing the record
		return nil
	}
	if *initKeyspace == "" || *initShard == "" || *initTabletType == "" {
		return fmt.Errorf("either need all of init_keyspace, init_shard and init_tablet_type, or none")
	}

	// parse init_tablet_type
	tabletType, err := topoproto.ParseTabletType(*initTabletType)
	if err != nil {
		return fmt.Errorf("invalid init_tablet_type %v: %v", *initTabletType, err)
	}
	if tabletType == topodatapb.TabletType_MASTER {
		// We disallow MASTER, so we don't have to change
		// shard.MasterAlias, and deal with the corner cases.
		return fmt.Errorf("init_tablet_type cannot be master, use replica instead")
	}

	// parse and validate shard name
	shard, _, err := topo.ValidateShardName(*initShard)
	if err != nil {
		return fmt.Errorf("cannot validate shard name %v: %v", *initShard, err)
	}

	// Create a context for this whole operation.  Note we will
	// retry some actions upon failure up to this context expires.
	ctx, cancel := context.WithTimeout(agent.batchCtx, *initTimeout)
	defer cancel()

	// Read the shard, create it if necessary.
	log.Infof("Reading/creating keyspace and shard records for %v/%v", *initKeyspace, shard)
	var si *topo.ShardInfo
	if err := agent.withRetry(ctx, "creating keyspace and shard", func() error {
		var err error
		si, err = agent.TopoServer.GetOrCreateShard(ctx, *initKeyspace, shard)
		return err
	}); err != nil {
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
		if err := agent.withRetry(ctx, "updating Cells list in Shard if necessary", func() error {
			si, err = agent.TopoServer.UpdateShardFields(ctx, *initKeyspace, shard, func(si *topo.ShardInfo) error {
				if si.HasCell(agent.TabletAlias.Cell) {
					// Someone else already did it.
					return topo.ErrNoUpdateNeeded
				}
				si.Cells = append(si.Cells, agent.TabletAlias.Cell)
				return nil
			})
			return err
		}); err != nil {
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

	// Now try to create the record (it will also fix up the
	// ShardReplication record if necessary).
	err = agent.TopoServer.CreateTablet(ctx, tablet)
	switch err {
	case nil:
		// It worked, we're good.
	case topo.ErrNodeExists:
		// The node already exists, will just try to update
		// it. So we read it first.
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
	default:
		return fmt.Errorf("CreateTablet failed: %v", err)
	}

	return nil
}
