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

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/vt/mysqlctl"
)

var Start = &cobra.Command{
	Use:     "start",
	Short:   "Starts mysqld on an already 'init'-ed directory.",
	Long:    "Resume an existing `mysqld` instance that was previously bootstrapped with `init` or `init_config`",
	Example: `mysqlctl --tablet_uid 101 --alsologtostderr start`,
	Args:    cobra.NoArgs,
	RunE:    commandStart,
}

var startArgs = struct {
	WaitTime   time.Duration
	MySQLdArgs flagutil.StringListValue
}{
	WaitTime: 5 * time.Minute,
}

func commandStart(cmd *cobra.Command, args []string) error {
	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(tabletUID, collationEnv)
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), startArgs.WaitTime)
	defer cancel()
	if err := mysqld.Start(ctx, cnf, startArgs.MySQLdArgs...); err != nil {
		return fmt.Errorf("failed start mysql: %v", err)
	}
	return nil
}

func init() {
	Start.Flags().DurationVar(&startArgs.WaitTime, "wait_time", startArgs.WaitTime, "How long to wait for mysqld startup.")
	Start.Flags().Var(&startArgs.MySQLdArgs, "mysqld_args", "List of comma-separated flags to pass additionally to mysqld.")

	Root.AddCommand(Start)
}
