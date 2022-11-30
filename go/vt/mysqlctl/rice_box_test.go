/*
Copyright 2022 The Vitess Authors.

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

package mysqlctl

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	rice "github.com/GeertJohan/go.rice"
	"github.com/stretchr/testify/require"
)

const (
	mySQLCtlRiceBoxLocation string = "../../../config"
)

// TestMySQLCtlRiceBox validates that this rice box is fresh: that `make embed_config` has been run after
// any changes to the embedded files
func TestMySQLCtlRiceBox(t *testing.T) {
	var riceBoxFiles []string

	riceBox := rice.MustFindBox(mySQLCtlRiceBoxLocation)

	err := filepath.Walk(mySQLCtlRiceBoxLocation,
		func(path string, info os.FileInfo, err error) error {
			require.NoError(t, err)
			if !info.IsDir() {
				name := strings.Replace(path, mySQLCtlRiceBoxLocation+"/", "", 1)
				riceBoxFiles = append(riceBoxFiles, name)
			}
			return nil
		})
	require.NoError(t, err)
	for _, fileName := range riceBoxFiles {
		t.Run(fileName, func(t *testing.T) {
			riceContents, err := riceBox.String(fileName)
			require.NoError(t, err)
			require.NotNil(t, riceContents)

			diskBytes, err := os.ReadFile(fmt.Sprintf("%s/%s", mySQLCtlRiceBoxLocation, fileName))
			diskContents := string(diskBytes)
			require.NoError(t, err)
			require.Equal(t, riceContents, diskContents)
		})
	}
}
