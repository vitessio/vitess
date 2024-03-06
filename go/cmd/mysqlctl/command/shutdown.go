/*
Copyright 2023 The Vitess Authors.

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

package command

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/vt/mysqlctl"
)

var Shutdown = &cobra.Command{
	Use:   "shutdown",
	Short: "Shuts down mysqld, without removing any files.",
	Long: "Stop a `mysqld` instance that was previously started with `init` or `start`.\n\n" +
		"For large `mysqld` instances, you may need to extend the `wait_time` to shutdown cleanly.",
	Example: `mysqlctl --tablet_uid 101 --alsologtostderr shutdown`,
	Args:    cobra.NoArgs,
	RunE:    commandShutdown,
}

var shutdownArgs = struct {
	WaitTime time.Duration
}{
	WaitTime: 5 * time.Minute,
}

func commandShutdown(cmd *cobra.Command, args []string) error {
	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(tabletUID, collationEnv)
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), shutdownArgs.WaitTime+10*time.Second)
	defer cancel()
	if err := mysqld.Shutdown(ctx, cnf, true, shutdownArgs.WaitTime); err != nil {
		return fmt.Errorf("failed shutdown mysql: %v", err)
	}
	return nil
}

func init() {
	Shutdown.Flags().DurationVar(&shutdownArgs.WaitTime, "wait_time", shutdownArgs.WaitTime, "How long to wait for mysqld shutdown.")

	Root.AddCommand(Shutdown)
}
