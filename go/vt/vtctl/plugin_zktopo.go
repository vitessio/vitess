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
	"golang.org/x/net/context"
)

func init() {
	addCommand("Generic", command{
		"PruneActionLogs",
		commandPruneActionLogs,
		"[-keep-count=<count to keep>] <zk actionlog path> ...",
		"(requires zktopo.Server)\n" +
			"e.g. PruneActionLogs -keep-count=10 /zk/global/vt/keyspaces/my_keyspace/shards/0/actionlog\n" +
			"Removes older actionlog entries until at most <count to keep> are left."})
}

func zkResolveWildcards(wr *wrangler.Wrangler, args []string) ([]string, error) {
	zkts, ok := wr.TopoServer().Impl.(*zktopo.Server)
	if !ok {
		return args, nil
	}
	return zk.ResolveWildcards(zkts.GetZConn(), args)
}

func commandPruneActionLogs(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	keepCount := subFlags.Int("keep-count", 10, "count to keep")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	if subFlags.NArg() == 0 {
		return fmt.Errorf("action PruneActionLogs requires <zk action log path> [...]")
	}

	paths, err := zkResolveWildcards(wr, subFlags.Args())
	if err != nil {
		return err
	}

	zkts, ok := wr.TopoServer().Impl.(*zktopo.Server)
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
