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
	"errors"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/acl"
	vtcmd "vitess.io/vitess/go/cmd"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
)

var (
	mysqlPort    = 3306
	tabletUID    = uint32(41983)
	mysqlSocket  string
	collationEnv *collations.Environment

	Root = &cobra.Command{
		Use:   "mysqlctl",
		Short: "mysqlctl initializes and controls mysqld with Vitess-specific configuration.",
		Long: "`mysqlctl` is a command-line client used for managing `mysqld` instances.\n\n" +
			"It is responsible for bootstrapping tasks such as generating a configuration file for `mysqld` and initializing the instance and its data directory.\n" +
			"The `mysqld_safe` watchdog is utilized when present.\n" +
			"This helps ensure that `mysqld` is automatically restarted after failures.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := servenv.CobraPreRunE(cmd, args); err != nil {
				return nil
			}

			if vtcmd.IsRunningAsRoot() {
				return errors.New("mysqlctl cannot be run as root. Please run as a different user")
			}

			return nil
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			logutil.Flush()
		},
		Version: servenv.AppVersion.String(),
	}
)

func init() {
	servenv.RegisterDefaultSocketFileFlags()
	servenv.RegisterFlags()
	servenv.RegisterServiceMapFlag()

	// mysqlctl only starts and stops mysql, only needs dba.
	dbconfigs.RegisterFlags(dbconfigs.Dba)

	servenv.MovePersistentFlagsToCobraCommand(Root)

	utils.SetFlagIntVar(Root.PersistentFlags(), &mysqlPort, "mysql-port", mysqlPort, "MySQL port.")
	utils.SetFlagUint32Var(Root.PersistentFlags(), &tabletUID, "tablet-uid", tabletUID, "Tablet UID.")
	utils.SetFlagStringVar(Root.PersistentFlags(), &mysqlSocket, "mysql-socket", mysqlSocket, "Path to the mysqld socket file.")

	acl.RegisterFlags(Root.PersistentFlags())

	collationEnv = collations.NewEnvironment(servenv.MySQLServerVersion())
}
