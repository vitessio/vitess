/*
Copyright 2017 Google Inc.

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

package zkctl

import (
	"testing"
)

// This test depend on starting and stopping a ZK instance,
// but may leave files/processes behind if they don't succeed,
// so some manual cleanup may be required.

func TestLifeCycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}

	config := "255@voltron:2888:3888:2181"
	myID := 255

	zkConf := MakeZkConfigFromString(config, uint32(myID))
	zkd := NewZkd(zkConf)
	if err := zkd.Init(); err != nil {
		t.Fatalf("Init() err: %v", err)
	}

	if err := zkd.Shutdown(); err != nil {
		t.Fatalf("Shutdown() err: %v", err)
	}

	if err := zkd.Start(); err != nil {
		t.Fatalf("Start() err: %v", err)
	}

	if err := zkd.Teardown(); err != nil {
		t.Fatalf("Teardown() err: %v", err)
	}
}
