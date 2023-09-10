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

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/vt/mysqlctl"
)

var ReinitConfig = &cobra.Command{
	Use:   "reinit_config",
	Short: "Reinitializes my.cnf file with new server_id.",
	Args:  cobra.NoArgs,
	RunE:  commandReinitConfig,
}

func commandReinitConfig(cmd *cobra.Command, args []string) error {
	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(tabletUID)
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	if err := mysqld.ReinitConfig(context.TODO(), cnf); err != nil {
		return fmt.Errorf("failed to reinit mysql config: %v", err)
	}
	return nil
}

func init() {
	Root.AddCommand(ReinitConfig)
}
