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
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/vt/mysqlctl"
)

var InitConfig = &cobra.Command{
	Use:   "init_config",
	Short: "Initializes the directory structure, creates my.cnf file, but does not start mysqld.",
	Long: "Bootstraps the configuration for a new `mysqld` instance and initializes its data directory.\n" +
		"This command is the same as `init` except the `mysqld` server will not be started.",
	Example: `mysqlctl \
	--alsologtostderr \
	--tablet_uid 101 \
	--mysql_port 12345 \
	init_config`,
	Args: cobra.NoArgs,
	RunE: commandInitConfig,
}

func commandInitConfig(cmd *cobra.Command, args []string) error {
	// Generate my.cnf from scratch and use it to find mysqld.
	mysqld, cnf, err := mysqlctl.CreateMysqldAndMycnf(tabletUID, mysqlSocket, mysqlPort, collationEnv)
	if err != nil {
		return fmt.Errorf("failed to initialize mysql config: %v", err)
	}
	defer mysqld.Close()
	if err := mysqld.InitConfig(cnf); err != nil {
		return fmt.Errorf("failed to init mysql config: %v", err)
	}

	return nil
}

func init() {
	Root.AddCommand(InitConfig)
}
