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

var Teardown = &cobra.Command{
	Use:   "teardown",
	Short: "Shuts mysqld down and removes the directory.",
	Long: "{{< warning >}}\n" +
		"This is a destructive operation.\n" +
		"{{</ warning >}}\n\n" +
		"Shuts down a `mysqld` instance and removes its data directory.",
	Example: `mysqlctl --tablet_uid 101 --alsologtostderr teardown`,
	Args:    cobra.NoArgs,
	RunE:    commandTeardown,
}

var teardownArgs = struct {
	WaitTime time.Duration
	Force    bool
}{
	WaitTime: 5 * time.Minute,
}

func commandTeardown(cmd *cobra.Command, args []string) error {
	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(tabletUID, collationEnv)
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), teardownArgs.WaitTime+10*time.Second)
	defer cancel()
	if err := mysqld.Teardown(ctx, cnf, teardownArgs.Force, teardownArgs.WaitTime); err != nil {
		return fmt.Errorf("failed teardown mysql (forced? %v): %v", teardownArgs.Force, err)
	}
	return nil
}

func init() {
	Teardown.Flags().DurationVar(&teardownArgs.WaitTime, "wait_time", teardownArgs.WaitTime, "How long to wait for mysqld shutdown.")
	Teardown.Flags().BoolVarP(&teardownArgs.Force, "force", "f", teardownArgs.Force, "Remove the root directory even if mysqld shutdown fails.")

	Root.AddCommand(Teardown)
}
