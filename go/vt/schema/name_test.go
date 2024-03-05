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
	"github.com/stretchr/testify/require"
)

func TestNameIsGCTableName(t *testing.T) {
	irrelevantNames := []string{
		"t",
		"_table_new",
		"__table_new",
		"_table_gho",
		"_table_ghc",
		"_table_del",
		"table_old",
		"vt_onlineddl_test_02",
		"_4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114014_gho",
		"_4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114014_ghc",
		"_4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114014_del",
		"_4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114013_new",
		"_table_old",
		"__table_old",
	}
	for _, tableName := range irrelevantNames {
		t.Run(tableName, func(t *testing.T) {
			assert.False(t, IsGCTableName(tableName))
		})
	}
	relevantNames := []string{
		"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
		"_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
		"_vt_EVAC_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
		"_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
	}
	for _, tableName := range relevantNames {
		t.Run(tableName, func(t *testing.T) {
			assert.True(t, IsGCTableName(tableName))
		})
	}
}

func TestIsInternalOperationTableName(t *testing.T) {
	names := []string{
		"_4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114014_gho",
		"_4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114014_ghc",
		"_4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114014_del",
		"_4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114013_new",
		"_table_old",
		"__table_old",
		"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
		"_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
		"_vt_EVAC_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
		"_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
		"_vt_drp_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
		"_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
		"_vt_prg_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
		"_vt_evc_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
		"_vt_vrp_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
		"_vt_gho_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
		"_vt_ghc_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
		"_vt_xyz_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
	}
	for _, tableName := range names {
		assert.True(t, IsInternalOperationTableName(tableName))
	}
	irrelevantNames := []string{
		"t",
		"_table_new",
		"__table_new",
		"_table_gho",
		"_table_ghc",
		"_table_del",
		"table_old",
		"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_202009151204100",
		"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410 ",
		"__vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
		"_vt_DROP_6ace8bcef73211ea87e9f875a4d2_20200915120410",
		"_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915",
		"_vt_OTHER_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
		"_vt_OTHER_6ace8bcef73211ea87e9f875a4d24e90_zz20200915120410",
	}
	for _, tableName := range irrelevantNames {
		assert.False(t, IsInternalOperationTableName(tableName))
	}
}

func TestAnalyzeInternalTableName(t *testing.T) {
	baseTime, err := time.Parse(time.RFC1123, "Tue, 15 Sep 2020 12:04:10 UTC")
	assert.NoError(t, err)
	tt := []struct {
		tableName  string
		hint       string
		t          time.Time
		isInternal bool
	}{
		{
			tableName:  "_84371a37_6153_11eb_9917_f875a4d24e90_20210128122816_vrepl",
			isInternal: false,
		},
		{
			tableName:  "_vt_DROP_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			isInternal: false,
		},
		{
			tableName:  "_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			isInternal: false,
		},
		{
			tableName:  "_vt_EVAC_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			isInternal: false,
		},
		{
			tableName:  "_vt_PURGE_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			isInternal: false,
		},
		{
			tableName:  "_vt_drop_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			isInternal: false,
		},
		{
			tableName:  "_vt_drp_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			hint:       "drp",
			t:          baseTime,
			isInternal: true,
		},
		{
			tableName:  "_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			hint:       "hld",
			t:          baseTime,
			isInternal: true,
		},
		{
			tableName:  "_vt_xyz_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_",
			hint:       "xyz",
			t:          baseTime,
			isInternal: true,
		},
		{
			tableName:  "_vt_xyz_6ace8bcef73211ea87e9f875a4d24e90_20200915129999_",
			isInternal: false,
		},
	}
	for _, ts := range tt {
		t.Run(ts.tableName, func(t *testing.T) {
			isInternal, hint, uuid, tm, err := AnalyzeInternalTableName(ts.tableName)
			assert.Equal(t, ts.isInternal, isInternal)
			if ts.isInternal {
				assert.NoError(t, err)
				assert.True(t, isCondensedUUID(uuid))
				assert.Equal(t, ts.hint, hint)
				assert.Equal(t, ts.t, tm)
			}
		})
	}
}

func TestToReadableTimestamp(t *testing.T) {
	ti, err := time.Parse(time.UnixDate, "Wed Feb 25 11:06:39 PST 2015")
	assert.NoError(t, err)

	readableTimestamp := ToReadableTimestamp(ti)
	assert.Equal(t, "20150225110639", readableTimestamp)
}

func TestGenerateInternalTableName(t *testing.T) {
	ti, err := time.Parse(time.UnixDate, "Wed Feb 25 11:06:39 PST 2015")
	assert.NoError(t, err)

	{
		uuid := "6ace8bcef73211ea87e9f875a4d24e90"
		tableName, err := GenerateInternalTableName(InternalTableGCPurgeHint.String(), uuid, ti)
		require.NoError(t, err)
		assert.Equal(t, "_vt_prg_6ace8bcef73211ea87e9f875a4d24e90_20150225110639_", tableName)
		assert.True(t, IsInternalOperationTableName(tableName))
	}
	{
		uuid := "4e5dcf80_354b_11eb_82cd_f875a4d24e90"
		tableName, err := GenerateInternalTableName(InternalTableGCPurgeHint.String(), uuid, ti)
		require.NoError(t, err)
		assert.Equal(t, "_vt_prg_4e5dcf80354b11eb82cdf875a4d24e90_20150225110639_", tableName)
		assert.True(t, IsInternalOperationTableName(tableName))
	}
	{
		uuid := "4e5dcf80-354b-11eb-82cd-f875a4d24e90"
		tableName, err := GenerateInternalTableName(InternalTableGCPurgeHint.String(), uuid, ti)
		require.NoError(t, err)
		assert.Equal(t, "_vt_prg_4e5dcf80354b11eb82cdf875a4d24e90_20150225110639_", tableName)
		assert.True(t, IsInternalOperationTableName(tableName))
	}
	{
		uuid := ""
		tableName, err := GenerateInternalTableName(InternalTableGCPurgeHint.String(), uuid, ti)
		require.NoError(t, err)
		assert.True(t, IsInternalOperationTableName(tableName))
	}
	{
		uuid := "4e5dcf80_354b_11eb_82cd_f875a4d24e90_00001111"
		_, err := GenerateInternalTableName(InternalTableGCPurgeHint.String(), uuid, ti)
		require.ErrorContains(t, err, "Invalid UUID")
	}
	{
		uuid := "6ace8bcef73211ea87e9f875a4d24e90"
		_, err := GenerateInternalTableName("abcdefg", uuid, ti)
		require.ErrorContains(t, err, "Invalid hint")
	}
}
