/*
Copyright 2026 The Vitess Authors.

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

package materialize

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestCancelKeepDataFlag(t *testing.T) {
	root := &cobra.Command{Use: "test"}
	registerCommands(root)

	cancelCmd, _, err := root.Find([]string{"Materialize", "cancel"})
	require.NoError(t, err)

	keepDataFlag := cancelCmd.Flags().Lookup("keep-data")
	require.NotNil(t, keepDataFlag)
	require.Contains(t, keepDataFlag.Usage, "kept by default")
	require.Contains(t, keepDataFlag.Usage, "--keep-data=false")
}
