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
	"fmt"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestEtcdServer(t *testing.T) {
	defer cluster.PanicHandler(t)
	etcdURL := fmt.Sprintf("http://%s:%d/v2/keys", clusterInstance.Hostname, clusterInstance.TopoPort)
	testURL(t, etcdURL, "generic etcd url")
	testURL(t, etcdURL+"/vitess/global", "vitess global key")
	testURL(t, etcdURL+"/vitess/zone1", "vitess zone1 key")
}
