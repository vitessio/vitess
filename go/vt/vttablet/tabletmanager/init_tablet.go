/*
Copyright 2017 Google Inc.

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

package tabletmanager

// This file handles the initialization of the tablet at startup time.
// It is only enabled if init_tablet_type or init_keyspace is set.

import (
	"flag"
	"fmt"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	initDbNameOverride   = flag.String("init_db_name_override", "", "(init parameter) override the name of the db used by vttablet")
	initKeyspace         = flag.String("init_keyspace", "", "(init parameter) keyspace to use for this tablet")
	initShard            = flag.String("init_shard", "", "(init parameter) shard to use for this tablet")
	initTags             flagutil.StringMapValue
	initTabletType       = flag.String("init_tablet_type", "", "(init parameter) the tablet type to use for this tablet.")
	initTimeout          = flag.Duration("init_timeout", 1*time.Minute, "(init parameter) timeout to use for the init phase.")
	initPopulateMetadata = flag.Bool("init_populate_metadata", false, "(init parameter) populate metadata tables")
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
		return vterrors.Wrapf(err, "invalid init_tablet_type %v", *initTabletType)
	}
	if tabletType == topodatapb.TabletType_MASTER {
		// We disallow MASTER, so we don't have to change
		// shard.MasterAlias, and deal with the corner cases.
		return fmt.Errorf("init_tablet_type cannot be master, use replica instead")
	}

	// parse and validate shard name
	shard, keyRange, err := topo.ValidateShardName(*initShard)
	if err != nil {
		return vterrors.Wrapf(err, "cannot validate shard name %v", *initShard)
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
		return vterrors.Wrap(err, "InitTablet cannot GetOrCreateShard shard")
	}
	if si.MasterAlias != nil && topoproto.TabletAliasEqual(si.MasterAlias, agent.TabletAlias) {
		// We're marked as master in the shard record, which could mean the master
		// tablet process was just restarted. However, we need to check if a new
		// master is in the process of taking over. In that case, it will let us
		// know by forcibly updating the old master's tablet record.
		oldTablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
		switch {
		case topo.IsErrType(err, topo.NoNode):
			// There's no existing tablet record, so we can assume
			// no one has left us a message to step down.
			tabletType = topodatapb.TabletType_MASTER
			// Update the TER timestamp (current value is 0) because we
			// assume that we are actually the MASTER and in case of a tiebreak,
			// vtgate should prefer us.
			agent.setExternallyReparentedTime(time.Now())
		case err == nil:
			if oldTablet.Type == topodatapb.TabletType_MASTER {
				// We're marked as master in the shard record,
				// and our existing tablet record agrees.
				tabletType = topodatapb.TabletType_MASTER
				// Same comment as above. Update tiebreaking timestamp to now.
				agent.setExternallyReparentedTime(time.Now())
			}
		default:
			return vterrors.Wrap(err, "InitTablet failed to read existing tablet record")
		}
	}

	// Rebuild keyspace graph if this the first tablet in this keyspace/cell
	_, err = agent.TopoServer.GetSrvKeyspace(ctx, agent.TabletAlias.Cell, *initKeyspace)
	switch {
	case err == nil:
		// NOOP
	case topo.IsErrType(err, topo.NoNode):
		// try to RebuildKeyspace here but ignore errors if it fails
		topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), agent.TopoServer, *initKeyspace, []string{agent.TabletAlias.Cell})
	default:
		return vterrors.Wrap(err, "InitTablet failed to read srvKeyspace")
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

	// if we are recovering from a snapshot we set initDbNameOverride
	// but only if it not already set
	if *initDbNameOverride == "" {
		keyspaceInfo, err := agent.TopoServer.GetKeyspace(ctx, *initKeyspace)
		if err != nil {
			return vterrors.Wrapf(err, "Error getting keyspace: %v", *initKeyspace)
		}
		baseKeyspace := keyspaceInfo.Keyspace.BaseKeyspace
		if baseKeyspace != "" {
			*initDbNameOverride = topoproto.VtDbPrefix + baseKeyspace
		}
	}

	// create and populate tablet record
	tablet := &topodatapb.Tablet{
		Alias:          agent.TabletAlias,
		Hostname:       hostname,
		PortMap:        make(map[string]int32),
		Keyspace:       *initKeyspace,
		Shard:          shard,
		KeyRange:       keyRange,
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

	// Now try to create the record (it will also fix up the
	// ShardReplication record if necessary).
	err = agent.TopoServer.CreateTablet(ctx, tablet)
	switch {
	case err == nil:
		// It worked, we're good.
	case topo.IsErrType(err, topo.NodeExists):
		// The node already exists, will just try to update
		// it. So we read it first.
		oldTablet, err := agent.TopoServer.GetTablet(ctx, tablet.Alias)
		if err != nil {
			return vterrors.Wrap(err, "InitTablet failed to read existing tablet record")
		}

		// Sanity check the keyspace and shard
		if oldTablet.Keyspace != tablet.Keyspace || oldTablet.Shard != tablet.Shard {
			return fmt.Errorf("InitTablet failed because existing tablet keyspace and shard %v/%v differ from the provided ones %v/%v", oldTablet.Keyspace, oldTablet.Shard, tablet.Keyspace, tablet.Shard)
		}

		// Update ShardReplication in any case, to be sure.  This is
		// meant to fix the case when a Tablet record was created, but
		// then the ShardReplication record was not (because for
		// instance of a startup timeout). Upon running this code
		// again, we want to fix ShardReplication.
		if updateErr := topo.UpdateTabletReplicationData(ctx, agent.TopoServer, tablet); updateErr != nil {
			return vterrors.Wrap(updateErr, "UpdateTabletReplicationData failed")
		}

		// Then overwrite everything, ignoring version mismatch.
		if err := agent.TopoServer.UpdateTablet(ctx, topo.NewTabletInfo(tablet, nil)); err != nil {
			return vterrors.Wrap(err, "UpdateTablet failed")
		}
	default:
		return vterrors.Wrap(err, "CreateTablet failed")
	}

	// optionally populate metadata records
	if *initPopulateMetadata {
		agent.setTablet(tablet)
		localMetadata := agent.getLocalMetadataValues(tablet.Type)
		err := mysqlctl.PopulateMetadataTables(agent.MysqlDaemon, localMetadata, topoproto.TabletDbName(tablet))
		if err != nil {
			return vterrors.Wrap(err, "failed to -init_populate_metadata")
		}
	}

	return nil
}
