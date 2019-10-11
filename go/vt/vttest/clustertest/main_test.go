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

package clustertest

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/vt/vttest"
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		cluster := vttest.EtcdProcessInstance()
		if err := cluster.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			cluster.TearDown()
			return 1
		}
		defer cluster.TearDown()
		return m.Run()
	}()
	os.Exit(exitCode)
}
