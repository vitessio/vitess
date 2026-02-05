/*
Copyright 2020 The Vitess Authors.

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

package xtrabackup

import (
	"testing"

	"vitess.io/vitess/go/test/endtoend/recovery"
	"vitess.io/vitess/go/test/endtoend/recovery/unshardedrecovery"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

func TestMain(m *testing.M) {
	recovery.UseXb = true
	unshardedrecovery.TestMainImpl(m)
}

func TestRecovery(t *testing.T) {
	unshardedrecovery.TestRecoveryImpl(t)
}
