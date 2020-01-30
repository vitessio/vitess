/*
Copyright 2019 The Vitess Authors.

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

package cluster

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Vttablet stores the properties needed to start a vttablet process
type Vttablet struct {
	Type      string
	TabletUID int
	HTTPPort  int
	GrpcPort  int
	MySQLPort int
	Alias     string
	Cell      string

	// background executable processes
	MysqlctlProcess  MysqlctlProcess
	MysqlctldProcess MysqlctldProcess
	VttabletProcess  *VttabletProcess
}

// Restart restarts vttablet and mysql.
func (tablet *Vttablet) Restart() error {
	if tablet.MysqlctlProcess.TabletUID|tablet.MysqlctldProcess.TabletUID == 0 {
		return fmt.Errorf("no mysql process is running")
	}

	if tablet.MysqlctlProcess.TabletUID > 0 {
		tablet.MysqlctlProcess.Stop()
		tablet.VttabletProcess.TearDown()
		os.RemoveAll(tablet.VttabletProcess.Directory)

		return tablet.MysqlctlProcess.Start()
	}

	tablet.MysqlctldProcess.Stop()
	tablet.VttabletProcess.TearDown()
	os.RemoveAll(tablet.VttabletProcess.Directory)

	return tablet.MysqlctldProcess.Start()
}

// ValidateTabletRestart restarts the tablet and validate error if there is any.
func (tablet *Vttablet) ValidateTabletRestart(t *testing.T) {
	require.Nilf(t, tablet.Restart(), "tablet restart failed")
}
