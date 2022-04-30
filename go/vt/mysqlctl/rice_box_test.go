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
	"io/ioutil"
	"testing"

	rice "github.com/GeertJohan/go.rice"
	"github.com/stretchr/testify/require"
)

const (
	mysqlCtlRiceBoxLocation string = "../../../config"
)

var riceBoxFiles = []string{
	"init_db.sql",
}

// TestMySQLCtlRiceBox validates that this rice box is fresh: that `make embed_config` has been run after
// any changes to the embedded files
func TestMySQLCtlRiceBox(t *testing.T) {
	riceBox := rice.MustFindBox(mysqlCtlRiceBoxLocation)

	for _, fileName := range riceBoxFiles {
		t.Run(fileName, func(t *testing.T) {
			riceContents, err := riceBox.String(fileName)
			require.NoError(t, err)
			require.NotNil(t, riceContents)

			diskBytes, err := ioutil.ReadFile(fmt.Sprintf("%s/%s", mysqlCtlRiceBoxLocation, fileName))
			diskContents := string(diskBytes)
			require.NoError(t, err)
			require.Equal(t, riceContents, diskContents)
		})
	}
}
