// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// Imports and register the Zookeeper topo.Server
// Adds the Zookeeper specific commands

import (
	"flag"
	"fmt"
	"path"
	"sort"
	"sync"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/sync2"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/vt/topo"
	"code.google.com/p/vitess/go/vt/wrangler"
	"code.google.com/p/vitess/go/vt/zktopo"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

func init() {
	addCommand("Generic", command{
		"PurgeActions",
		commandPurgeActions,
		"<zk action path> ... (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>/action)",
		"(requires zktopo.Server)\n" +
			"Remove all actions - be careful, this is powerful cleanup magic."})
	addCommand("Generic", command{
		"StaleActions",
		commandStaleActions,
		"[-max-staleness=<duration> -purge] <zk action path> ... (/zk/global/vt/keyspaces/<keyspace>/shards/<shard>/action)",
		"(requires zktopo.Server)\n" +
			"List any queued actions that are considered stale."})
	addCommand("Generic", command{
		"PruneActionLogs",
		commandPruneActionLogs,
		"[-keep-count=<count to keep>] <zk actionlog path> ...",
		"(requires zktopo.Server)\n" +
			"e.g. PruneActionLogs -keep-count=10 /zk/global/vt/keyspaces/my_keyspace/shards/0/actionlog\n" +
			"Removes older actionlog entries until at most <count to keep> are left."})
	addCommand("Generic", command{
		"ExportZkns",
		commandExportZkns,
		"<cell name|zk local vt path>",
		"(requires zktopo.Server)\n" +
			"Export the serving graph entries to the zkns format."})
	addCommand("Generic", command{
		"ExportZknsForKeyspace",
		commandExportZknsForKeyspace,
		"<keyspace|zk global keyspace path>",
		"(requires zktopo.Server)\n" +
			"Export the serving graph entries to the zkns format."})

	addCommand("Shards", command{
		"ListShardActions",
		commandListShardActions,
		"<keyspace/shard|zk shard path>",
		"(requires zktopo.Server)\n" +
			"List all active actions in a given shard."})

	resolveWildcards = zkResolveWildcards
}

func zkResolveWildcards(wr *wrangler.Wrangler, args []string) ([]string, error) {
	zkts, ok := wr.TopoServer().(*zktopo.Server)
	if !ok {
		return args, nil
	}
	return zk.ResolveWildcards(zkts.GetZConn(), args)
}

func commandPurgeActions(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		relog.Fatal("action PurgeActions requires <zk action path> ...")
	}
	zkts, ok := wr.TopoServer().(*zktopo.Server)
	if !ok {
		return "", fmt.Errorf("PurgeActions requires a zktopo.Server")
	}
	zkActionPaths, err := resolveWildcards(wr, subFlags.Args())
	if err != nil {
		return "", err
	}
	for _, zkActionPath := range zkActionPaths {
		err := zkts.PurgeActions(zkActionPath, tm.ActionNodeCanBePurged)
		if err != nil {
			return "", err
		}
	}
	return "", nil
}

func staleActions(zkts *zktopo.Server, zkActionPath string, maxStaleness time.Duration) ([]*tm.ActionNode, error) {
	// get the stale strings
	actionNodes, err := zkts.StaleActions(zkActionPath, maxStaleness, tm.ActionNodeIsStale)
	if err != nil {
		return nil, err
	}

	// convert to ActionNode
	staleActions := make([]*tm.ActionNode, len(actionNodes))
	for i, actionNodeStr := range actionNodes {
		actionNode, err := tm.ActionNodeFromJson(actionNodeStr, "")
		if err != nil {
			return nil, err
		}
		staleActions[i] = actionNode
	}

	return staleActions, nil
}

func commandStaleActions(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	maxStaleness := subFlags.Duration("max-staleness", 5*time.Minute, "how long since the last modification before an action considered stale")
	purge := subFlags.Bool("purge", false, "purge stale actions")
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		relog.Fatal("action StaleActions requires <zk action path>")
	}
	zkts, ok := wr.TopoServer().(*zktopo.Server)
	if !ok {
		return "", fmt.Errorf("StaleActions requires a zktopo.Server")
	}
	zkPaths, err := resolveWildcards(wr, subFlags.Args())
	if err != nil {
		return "", err
	}
	var errCount sync2.AtomicInt32
	wg := sync.WaitGroup{}
	for _, apath := range zkPaths {
		wg.Add(1)
		go func(zkActionPath string) {
			defer wg.Done()
			staleActions, err := staleActions(zkts, zkActionPath, *maxStaleness)
			if err != nil {
				errCount.Add(1)
				relog.Error("can't check stale actions: %v %v", zkActionPath, err)
				return
			}
			for _, action := range staleActions {
				fmt.Println(fmtAction(action))
			}
			if *purge && len(staleActions) > 0 {
				err := zkts.PurgeActions(zkActionPath, tm.ActionNodeCanBePurged)
				if err != nil {
					errCount.Add(1)
					relog.Error("can't purge stale actions: %v %v", zkActionPath, err)
					return
				}
			}
		}(apath)
	}
	wg.Wait()
	if errCount.Get() > 0 {
		return "", fmt.Errorf("some errors occurred, check the log")
	}
	return "", nil
}

func commandPruneActionLogs(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	keepCount := subFlags.Int("keep-count", 10, "count to keep")
	subFlags.Parse(args)

	if subFlags.NArg() == 0 {
		relog.Fatal("action PruneActionLogs requires <zk action log path> ...")
	}

	paths, err := resolveWildcards(wr, subFlags.Args())
	if err != nil {
		return "", err
	}

	zkts, ok := wr.TopoServer().(*zktopo.Server)
	if !ok {
		return "", fmt.Errorf("PruneActionLogs requires a zktopo.Server")
	}

	var errCount sync2.AtomicInt32
	wg := sync.WaitGroup{}
	for _, zkActionLogPath := range paths {
		wg.Add(1)
		go func(zkActionLogPath string) {
			defer wg.Done()
			purgedCount, err := zkts.PruneActionLogs(zkActionLogPath, *keepCount)
			if err == nil {
				relog.Debug("%v pruned %v", zkActionLogPath, purgedCount)
			} else {
				relog.Error("%v pruning failed: %v", zkActionLogPath, err)
				errCount.Add(1)
			}
		}(zkActionLogPath)
	}
	wg.Wait()
	if errCount.Get() > 0 {
		return "", fmt.Errorf("some errors occurred, check the log")
	}
	return "", nil
}

func commandExportZkns(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ExportZkns requires <cell name|zk vt root path>")
	}
	cell := vtPathToCell(subFlags.Arg(0))
	return "", wr.ExportZkns(cell)
}

func commandExportZknsForKeyspace(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ExportZknsForKeyspace requires <keyspace|zk global keyspace path>")
	}
	keyspace := keyspaceParamToKeyspace(subFlags.Arg(0))
	return "", wr.ExportZknsForKeyspace(keyspace)
}

func getActions(zconn zk.Conn, actionPath string) ([]*tm.ActionNode, error) {
	actions, _, err := zconn.Children(actionPath)
	if err != nil {
		return nil, fmt.Errorf("getActions failed: %v %v", actionPath, err)
	}
	sort.Strings(actions)
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	nodes := make([]*tm.ActionNode, 0, len(actions))
	for _, action := range actions {
		wg.Add(1)
		go func(action string) {
			defer wg.Done()
			actionNodePath := path.Join(actionPath, action)
			data, _, err := zconn.Get(actionNodePath)
			if err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
				relog.Warning("getActions: %v %v", actionNodePath, err)
				return
			}
			actionNode, err := tm.ActionNodeFromJson(data, actionNodePath)
			if err != nil {
				relog.Warning("getActions: %v %v", actionNodePath, err)
				return
			}
			mu.Lock()
			nodes = append(nodes, actionNode)
			mu.Unlock()
		}(action)
	}
	wg.Wait()

	return nodes, nil
}

func listActionsByShard(ts topo.Server, keyspace, shard string) error {
	// only works with Server
	zkts, ok := ts.(*zktopo.Server)
	if !ok {
		return fmt.Errorf("listActionsByShard only works with zktopo.Server")
	}

	// print the shard action nodes
	shardActionPath := zkts.ShardActionPath(keyspace, shard)
	shardActionNodes, err := getActions(zkts.GetZConn(), shardActionPath)
	if err != nil {
		return err
	}
	for _, shardAction := range shardActionNodes {
		fmt.Println(fmtAction(shardAction))
	}

	// get and print the tablet action nodes
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	actionMap := make(map[string]*tm.ActionNode)

	f := func(actionPath string) {
		defer wg.Done()
		actionNodes, err := getActions(zkts.GetZConn(), actionPath)
		if err != nil {
			relog.Warning("listActionsByShard %v", err)
			return
		}
		mu.Lock()
		for _, node := range actionNodes {
			actionMap[node.Path()] = node
		}
		mu.Unlock()
	}

	tabletAliases, err := topo.FindAllTabletAliasesInShard(ts, keyspace, shard)
	if err != nil {
		return err
	}
	for _, tabletAlias := range tabletAliases {
		actionPath := zktopo.TabletActionPathForAlias(tabletAlias)
		if err != nil {
			relog.Warning("listActionsByShard %v", err)
		} else {
			wg.Add(1)
			go f(actionPath)
		}
	}

	wg.Wait()
	mu.Lock()
	defer mu.Unlock()

	keys := wrangler.CopyMapKeys(actionMap, []string{}).([]string)
	sort.Strings(keys)
	for _, key := range keys {
		action := actionMap[key]
		if action == nil {
			relog.Warning("nil action: %v", key)
		} else {
			fmt.Println(fmtAction(action))
		}
	}
	return nil
}

func commandListShardActions(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (string, error) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		relog.Fatal("action ListShardActions requires <keyspace/shard|zk shard path>")
	}
	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	return "", listActionsByShard(wr.TopoServer(), keyspace, shard)
}
