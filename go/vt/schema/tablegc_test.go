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

package schema

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIsGCTableName(t *testing.T) {
	tm := time.Now()
	hints := []TableGCHint{HoldTableGCHint, PurgeTableGCHint, DropTableGCHint}
	for _, hint := range hints {
		for i := 0; i < 10; i++ {
			for _, at := range []bool{false, true} {
				tableName, err := generateGCTableName(hint, at, tm)
				assert.NoError(t, err)
				assert.True(t, IsGCTableName(tableName))
			}
		}
	}
	names := []string{
		"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_202009151204100",
		"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410 ",
		"__vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
		"_vt_DROP_6ace8bcef73211ea87e9f875a4d2_20200915120410",
		"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915",
		"_vt_OTHER_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
		"_vt_OTHER_6ace8bcef73211ea87e9f875a4d24e90_zz20200915120410",
	}
	for _, tableName := range names {
		assert.False(t, IsGCTableName(tableName))
	}
}
