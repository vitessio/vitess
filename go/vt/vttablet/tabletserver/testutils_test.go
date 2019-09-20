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

package tabletserver

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

var errRejected = errors.New("rejected")

type dummyChecker struct {
}

func (dummyChecker) CheckMySQL() {}

var DummyChecker = dummyChecker{}

type testUtils struct{}

func newTestUtils() *testUtils {
	return &testUtils{}
}

func (util *testUtils) checkEqual(t *testing.T, expected interface{}, result interface{}) {
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("expect to get: %v, but got: %v", expected, result)
	}
}

func (util *testUtils) newDBConfigs(db *fakesqldb.DB) *dbconfigs.DBConfigs {
	return dbconfigs.NewTestDBConfigs(*db.ConnParams(), *db.ConnParams(), "")
}

func (util *testUtils) newQueryServiceConfig() tabletenv.TabletConfig {
	randID := rand.Int63()
	config := tabletenv.DefaultQsConfig
	config.PoolNamePrefix = fmt.Sprintf("Pool-%d-", randID)
	return config
}
