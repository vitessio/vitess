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

	"vitess.io/vitess/go/mysql/replication"
)

var Position = &cobra.Command{
	Use:   "position <operation> <pos1> <pos2 | gtid>",
	Short: "Compute operations on replication positions",
	Args: cobra.MatchAll(cobra.ExactArgs(3), func(cmd *cobra.Command, args []string) error {
		switch args[0] {
		case "equal", "at_least", "append":
		default:
			return fmt.Errorf("invalid operation %s (choices are 'equal', 'at_least', 'append')", args[0])
		}

		return nil
	}),
	RunE: commandPosition,
}

func commandPosition(cmd *cobra.Command, args []string) error {
	pos1, err := replication.DecodePosition(args[1])
	if err != nil {
		return err
	}

	switch args[0] {
	case "equal":
		pos2, err := replication.DecodePosition(args[2])
		if err != nil {
			return err
		}
		fmt.Println(pos1.Equal(pos2))
	case "at_least":
		pos2, err := replication.DecodePosition(args[2])
		if err != nil {
			return err
		}
		fmt.Println(pos1.AtLeast(pos2))
	case "append":
		gtid, err := replication.DecodeGTID(args[2])
		if err != nil {
			return err
		}
		fmt.Println(replication.AppendGTID(pos1, gtid))
	}

	return nil
}

func init() {
	Root.AddCommand(Position)
}
