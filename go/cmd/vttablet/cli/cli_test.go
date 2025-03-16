/*
Copyright 2024 The Vitess Authors.

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

package cli

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/utils"
)

// TestRunFailsToStartTabletManager tests the code path in 'run' where we fail to start the TabletManager
// this is done by starting vttablet without a cnf file but requesting it to restore from backup.
// When starting, the TabletManager checks if it needs to restore, in tm.handleRestore but this step will
// fail if we do not provide a cnf file and if the flag --restore_from_backup is provided.
func TestRunFailsToStartTabletManager(t *testing.T) {
	ts, factory := memorytopo.NewServerAndFactory(context.Background(), "cell")
	topo.RegisterFactory("test", factory)

	args := append([]string{}, os.Args...)
	t.Cleanup(func() {
		ts.Close()
		tabletPath = ""
		os.Args = append([]string{}, args...)
	})

	flags := make(map[string]string)
	utils.SetFlagVariantsForTests(flags, "topo-implementation", "test")
	utils.SetFlagVariantsForTests(flags, "topo-global-server-address", "localhost")
	utils.SetFlagVariantsForTests(flags, "topo-global-root", "cell")

	var flagArgs []string
	for k, v := range flags {
		flagArgs = append(flagArgs, fmt.Sprintf("--%s=%s", k, v))
	}

	flagArgs = append(flagArgs,
		"--db_host", "localhost", "--db_port", "3306",
		"--tablet-path", "cell-1", "--init_keyspace", "ks",
		"--init_shard", "0", "--init_tablet_type", "replica",
		"--restore_from_backup",
	)

	os.Args = append([]string{"vttablet"}, flagArgs...)

	// Creating and canceling the context so that pending tasks in tm_init gets canceled before we close the topo server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := Main.ExecuteContext(ctx)
	require.ErrorContains(t, err, "you cannot enable --restore_from_backup without a my.cnf file")
}
