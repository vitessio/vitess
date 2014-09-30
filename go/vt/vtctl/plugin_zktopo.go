// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtctl

// Imports and register the Zookeeper topo.Server
// Adds the Zookeeper specific commands

import (
	"flag"
	"fmt"
	"sync"

	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/zktopo"
	"github.com/youtube/vitess/go/zk"
)

func init() {
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

	resolveWildcards = zkResolveWildcards
}

func zkResolveWildcards(wr *wrangler.Wrangler, args []string) ([]string, error) {
	zkts, ok := wr.TopoServer().(*zktopo.Server)
	if !ok {
		return args, nil
	}
	return zk.ResolveWildcards(zkts.GetZConn(), args)
}

func commandPruneActionLogs(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	keepCount := subFlags.Int("keep-count", 10, "count to keep")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() == 0 {
		return fmt.Errorf("action PruneActionLogs requires <zk action log path> ...")
	}

	paths, err := resolveWildcards(wr, subFlags.Args())
	if err != nil {
		return err
	}

	zkts, ok := wr.TopoServer().(*zktopo.Server)
	if !ok {
		return fmt.Errorf("PruneActionLogs requires a zktopo.Server")
	}

	var errCount sync2.AtomicInt32
	wg := sync.WaitGroup{}
	for _, zkActionLogPath := range paths {
		wg.Add(1)
		go func(zkActionLogPath string) {
			defer wg.Done()
			purgedCount, err := zkts.PruneActionLogs(zkActionLogPath, *keepCount)
			if err == nil {
				wr.Logger().Infof("%v pruned %v", zkActionLogPath, purgedCount)
			} else {
				wr.Logger().Errorf("%v pruning failed: %v", zkActionLogPath, err)
				errCount.Add(1)
			}
		}(zkActionLogPath)
	}
	wg.Wait()
	if errCount.Get() > 0 {
		return fmt.Errorf("some errors occurred, check the log")
	}
	return nil
}

func commandExportZkns(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ExportZkns requires <cell name|zk vt root path>")
	}
	cell, err := vtPathToCell(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ExportZkns(cell)
}

func commandExportZknsForKeyspace(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ExportZknsForKeyspace requires <keyspace|zk global keyspace path>")
	}
	keyspace, err := keyspaceParamToKeyspace(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ExportZknsForKeyspace(keyspace)
}
