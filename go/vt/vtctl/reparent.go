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

	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func init() {
	addCommand("Tablets", command{
		name:   "ReparentTablet",
		method: commandReparentTablet,
		params: "<tablet alias>",
		help:   "Reparent a tablet to the current primary in the shard. This only works if the current replication position matches the last known reparent action.",
	})
	addCommand("Shards", command{
		name:   "InitShardPrimary",
		method: commandInitShardPrimary,
		params: "[--force] [--wait_replicas_timeout=<duration>] <keyspace/shard> <tablet alias>",
		help:   "Sets the initial primary for a shard. Will make all other tablets in the shard replicas of the provided tablet. WARNING: this could cause data loss on an already replicating shard. PlannedReparentShard or EmergencyReparentShard should be used instead.",
	})
	addCommand("Shards", command{
		name:   "PlannedReparentShard",
		method: commandPlannedReparentShard,
		params: "--keyspace_shard=<keyspace/shard> [--new_primary=<tablet alias>] [--avoid_tablet=<tablet alias>] [--wait_replicas_timeout=<duration>]",
		help:   "Reparents the shard to the new primary, or away from old primary. Both old and new primary need to be up and running.",
	})
	addCommand("Shards", command{
		name:   "EmergencyReparentShard",
		method: commandEmergencyReparentShard,
		params: "--keyspace_shard=<keyspace/shard> [--new_primary=<tablet alias>] [--wait_replicas_timeout=<duration>] [--ignore_replicas=<tablet alias list>] [--prevent_cross_cell_promotion=<true/false>]",
		help:   "Reparents the shard to the new primary. Assumes the old primary is dead and not responding.",
	})
	addCommand("Shards", command{
		name:   "TabletExternallyReparented",
		method: commandTabletExternallyReparented,
		params: "<tablet alias>",
		help: "Changes metadata in the topology server to acknowledge a shard primary change performed by an external tool. See the Reparenting guide for more information:" +
			"https://vitess.io/docs/user-guides/reparenting/#external-reparenting",
	})
}

func commandReparentTablet(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if *mysqlctl.DisableActiveReparents {
		return fmt.Errorf("active reparent commands disabled (unset the --disable_active_reparents flag to enable)")
	}

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ReparentTablet requires <tablet alias>")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ReparentTablet(ctx, tabletAlias)
}

func commandInitShardPrimary(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if *mysqlctl.DisableActiveReparents {
		return fmt.Errorf("active reparent commands disabled (unset the --disable_active_reparents flag to enable)")
	}

	force := subFlags.Bool("force", false, "will force the reparent even if the provided tablet is not writable or the shard primary")
	waitReplicasTimeout := subFlags.Duration("wait_replicas_timeout", *topo.RemoteOperationTimeout, "time to wait for replicas to catch up in reparenting")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action InitShardPrimary requires <keyspace/shard> <tablet alias>")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(1))
	if err != nil {
		return err
	}
	return wr.InitShardPrimary(ctx, keyspace, shard, tabletAlias, *force, *waitReplicasTimeout)
}

func commandPlannedReparentShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if *mysqlctl.DisableActiveReparents {
		return fmt.Errorf("active reparent commands disabled (unset the --disable_active_reparents flag to enable)")
	}

	waitReplicasTimeout := subFlags.Duration("wait_replicas_timeout", *topo.RemoteOperationTimeout, "time to wait for replicas to catch up on replication before and after reparenting")
	keyspaceShard := subFlags.String("keyspace_shard", "", "keyspace/shard of the shard that needs to be reparented")
	newPrimary := subFlags.String("new_primary", "", "alias of a tablet that should be the new primary")
	avoidTablet := subFlags.String("avoid_tablet", "", "alias of a tablet that should not be the primary, i.e. reparent to any other tablet if this one is the primary")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 2 {
		// Legacy syntax: "<keyspace/shard> <tablet alias>".
		if *keyspaceShard != "" || *newPrimary != "" {
			return fmt.Errorf("cannot use legacy syntax and flags --keyspace_shard and --new_primary for action PlannedReparentShard at the same time")
		}
		*keyspaceShard = subFlags.Arg(0)
		*newPrimary = subFlags.Arg(1)
	} else if subFlags.NArg() != 0 {
		return fmt.Errorf("action PlannedReparentShard requires --keyspace_shard=<keyspace/shard> [--new_primary=<tablet alias>] [--avoid_tablet=<tablet alias>]")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(*keyspaceShard)
	if err != nil {
		return err
	}
	var newPrimaryAlias, avoidTabletAlias *topodatapb.TabletAlias
	if *newPrimary != "" {
		newPrimaryAlias, err = topoproto.ParseTabletAlias(*newPrimary)
		if err != nil {
			return err
		}
	}
	if *avoidTablet != "" {
		avoidTabletAlias, err = topoproto.ParseTabletAlias(*avoidTablet)
		if err != nil {
			return err
		}
	}
	return wr.PlannedReparentShard(ctx, keyspace, shard, newPrimaryAlias, avoidTabletAlias, *waitReplicasTimeout)
}

func commandEmergencyReparentShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if *mysqlctl.DisableActiveReparents {
		return fmt.Errorf("active reparent commands disabled (unset the --disable_active_reparents flag to enable)")
	}

	waitReplicasTimeout := subFlags.Duration("wait_replicas_timeout", *topo.RemoteOperationTimeout, "time to wait for replicas to catch up in reparenting")
	keyspaceShard := subFlags.String("keyspace_shard", "", "keyspace/shard of the shard that needs to be reparented")
	newPrimary := subFlags.String("new_primary", "", "optional alias of a tablet that should be the new primary. If not specified, Vitess will select the best candidate")
	preventCrossCellPromotion := subFlags.Bool("prevent_cross_cell_promotion", false, "only promotes a new primary from the same cell as the previous primary")
	ignoreReplicasList := subFlags.String("ignore_replicas", "", "comma-separated list of replica tablet aliases to ignore during emergency reparent")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 2 {
		// Legacy syntax: "<keyspace/shard> <tablet alias>".
		if *newPrimary != "" {
			return fmt.Errorf("cannot use legacy syntax and flag --new_primary for action EmergencyReparentShard at the same time")
		}
		*keyspaceShard = subFlags.Arg(0)
		*newPrimary = subFlags.Arg(1)
	} else if subFlags.NArg() != 0 {
		return fmt.Errorf("action EmergencyReparentShard requires --keyspace_shard=<keyspace/shard>")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(*keyspaceShard)
	if err != nil {
		return err
	}
	var tabletAlias *topodatapb.TabletAlias
	if *newPrimary != "" {
		tabletAlias, err = topoproto.ParseTabletAlias(*newPrimary)
		if err != nil {
			return err
		}
	}
	unreachableReplicas := topoproto.ParseTabletSet(*ignoreReplicasList)
	return wr.EmergencyReparentShard(ctx, keyspace, shard, tabletAlias, *waitReplicasTimeout, unreachableReplicas, *preventCrossCellPromotion)
}

func commandTabletExternallyReparented(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action TabletExternallyReparented requires <tablet alias>")
	}

	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.TabletExternallyReparented(ctx, tabletAlias)
}
