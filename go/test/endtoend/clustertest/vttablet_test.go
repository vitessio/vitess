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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
)

func TestVttabletProcess(t *testing.T) {
	firstTabletPort := clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].HTTPPort
	testURL(t, fmt.Sprintf("http://localhost:%d/debug/vars/", firstTabletPort), "tablet debug var url")
	resp, _ := http.Get(fmt.Sprintf("http://localhost:%d/debug/vars", firstTabletPort))
	resultMap := make(map[string]interface{})
	respByte, _ := ioutil.ReadAll(resp.Body)
	err := json.Unmarshal(respByte, &resultMap)
	if err != nil {
		panic(err)
	}
	if got, want := resultMap["TabletKeyspace"], "commerce"; got != want {
		t.Errorf("select:\n%v want\n%v for %s", got, want, "Keyspace of tablet should match")
	}
}
