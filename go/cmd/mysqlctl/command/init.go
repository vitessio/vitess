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
	"vitess.io/vitess/go/vt/utils"
)

var Init = &cobra.Command{
	Use:   "init",
	Short: "Initializes the directory structure and starts mysqld.",
	Long: "Bootstraps a new `mysqld` instance, initializes its data directory, and starts the instance.\n" +
		"The MySQL version and flavor will be auto-detected, with a minimal configuration file applied.",
	Example: `mysqlctl \
	--alsologtostderr \
	--tablet-uid 101 \
	--mysql-port 12345 \
	init`,
	Args: cobra.NoArgs,
	RunE: commandInit,
}

var initArgs = struct {
	WaitTime      time.Duration
	InitDbSQLFile string
}{
	WaitTime: 5 * time.Minute,
}

func commandInit(cmd *cobra.Command, args []string) error {
	// Generate my.cnf from scratch and use it to find mysqld.
	mysqld, cnf, err := mysqlctl.CreateMysqldAndMycnf(tabletUID, mysqlSocket, mysqlPort, collationEnv)
	if err != nil {
		return fmt.Errorf("failed to initialize mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(cmd.Context(), initArgs.WaitTime)
	defer cancel()
	if err := mysqld.Init(ctx, cnf, initArgs.InitDbSQLFile); err != nil {
		return fmt.Errorf("failed init mysql: %v", err)
	}
	return nil
}

func init() {
	utils.SetFlagDurationVar(Init.Flags(), &initArgs.WaitTime, "wait-time", initArgs.WaitTime, "How long to wait for mysqld startup.")
	utils.SetFlagStringVar(Init.Flags(), &initArgs.InitDbSQLFile, "init-db-sql-file", initArgs.InitDbSQLFile, "Path to .sql file to run after mysqld initiliaztion.")

	Root.AddCommand(Init)
}
