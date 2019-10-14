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
	"net/http"
	"os"
	"testing"

	"vitess.io/vitess/go/vt/vttest"
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		etcdProcess := vttest.EtcdProcessInstance()
		if err := etcdProcess.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			etcdProcess.TearDown()
			return 1
		}

		vtcltlProcess := vttest.VtctlProcessInstance()
		vtcltlProcess.AddCellInfo()

		vtctldProcess := vttest.VtctldProcessInstance()
		if err := vtctldProcess.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			etcdProcess.TearDown()
			vtctldProcess.TearDown()
			return 1
		}

		defer etcdProcess.TearDown()
		defer vtctldProcess.TearDown()
		return m.Run()
	}()
	os.Exit(exitCode)
}

func testURL(t *testing.T, url string, testCaseName string) {
	statusCode := getStatusForURL(url)
	if got, want := statusCode, 200; got != want {
		t.Errorf("select:\n%v want\n%v for %s", got, want, testCaseName)
	}
}

// getStatusForUrl returns the status code for the URL
func getStatusForURL(url string) int {
	resp, _ := http.Get(url)
	if resp != nil {
		return resp.StatusCode
	}
	return 0
}
