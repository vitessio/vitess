package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
