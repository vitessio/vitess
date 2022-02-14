package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseAlterTableOptions(t *testing.T) {
	type expect struct {
		schema, table, options string
	}
	tests := map[string]expect{
		"add column i int, drop column d":                               {schema: "", table: "", options: "add column i int, drop column d"},
		"  add column i int, drop column d  ":                           {schema: "", table: "", options: "add column i int, drop column d"},
		"alter table t add column i int, drop column d":                 {schema: "", table: "t", options: "add column i int, drop column d"},
		"alter    table   t      add column i int, drop column d":       {schema: "", table: "t", options: "add column i int, drop column d"},
		"alter table `t` add column i int, drop column d":               {schema: "", table: "t", options: "add column i int, drop column d"},
		"alter table `scm`.`t` add column i int, drop column d":         {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"alter table `scm`.t add column i int, drop column d":           {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"alter table scm.`t` add column i int, drop column d":           {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"alter table scm.t add column i int, drop column d":             {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"  alter       table   scm.`t` add column i int, drop column d": {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"ALTER  table scm.t ADD COLUMN i int, DROP COLUMN d":            {schema: "scm", table: "t", options: "ADD COLUMN i int, DROP COLUMN d"},
		"ALTER TABLE scm.t ADD COLUMN i int, DROP COLUMN d":             {schema: "scm", table: "t", options: "ADD COLUMN i int, DROP COLUMN d"},
	}
	for query, expect := range tests {
		schema, table, options := ParseAlterTableOptions(query)
		assert.Equal(t, expect.schema, schema)
		assert.Equal(t, expect.table, table)
		assert.Equal(t, expect.options, options)
	}
}

func TestReplaceTableNameInCreateTableStatement(t *testing.T) {
	replacementTableName := `my_table`
	tt := []struct {
		stmt    string
		expect  string
		isError bool
	}{
		{
			stmt:    "CREATE TABLE tbl (id int)",
			isError: true,
		},
		{
			stmt:   "CREATE TABLE `tbl` (id int)",
			expect: "CREATE TABLE `my_table` (id int)",
		},
		{
			stmt:   "CREATE     TABLE     `tbl`    (id int)",
			expect: "CREATE     TABLE     `my_table`    (id int)",
		},
		{
			stmt:   "create table `tbl` (id int)",
			expect: "create table `my_table` (id int)",
		},
		{
			stmt:    "CREATE TABLE `schema`.`tbl` (id int)",
			isError: true,
		},
		{
			stmt:    "CREATE TABLE IF NOT EXISTS `tbl` (id int)",
			isError: true,
		},
	}
	for _, ts := range tt {
		t.Run(ts.stmt, func(*testing.T) {
			result, err := ReplaceTableNameInCreateTableStatement(ts.stmt, replacementTableName)
			if ts.isError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, ts.expect, result)
			}
		})
	}
}

func TestReplaceViewNameInCreateViewStatement(t *testing.T) {
	replacementViewName := `my_view`
	tt := []struct {
		stmt    string
		expect  string
		isError bool
	}{
		{
			stmt:    "CREATE VIEW vw AS SELECT * FROM tbl",
			isError: true,
		},
		{
			stmt:   "CREATE VIEW `vw` AS SELECT * FROM tbl",
			expect: "CREATE VIEW `my_view` AS SELECT * FROM tbl",
		},
		{
			stmt:   "CREATE     VIEW    `vw`   AS      SELECT    *  FROM     tbl",
			expect: "CREATE     VIEW    `my_view`   AS      SELECT    *  FROM     tbl",
		},
		{
			stmt:   "create view `vw` as select * from tbl",
			expect: "create view `my_view` as select * from tbl",
		},
		{
			stmt:   "create or replace view `vw` as select * from tbl",
			expect: "create or replace view `my_view` as select * from tbl",
		},
		{
			stmt:   "create ALGORITHM =TEMPTABLE view `vw` as select * from tbl",
			expect: "create ALGORITHM =TEMPTABLE view `my_view` as select * from tbl",
		},
		{
			stmt:    "CREATE VIEW `schema`.`vw` AS SELECT * FROM tbl",
			isError: true,
		},
	}
	for _, ts := range tt {
		t.Run(ts.stmt, func(*testing.T) {
			result, err := ReplaceViewNameInCreateViewStatement(ts.stmt, replacementViewName)
			if ts.isError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, ts.expect, result)
			}
		})
	}
}

func TestLegacyParseRevertUUID(t *testing.T) {

	{
		uuid, err := legacyParseRevertUUID("revert 4e5dcf80_354b_11eb_82cd_f875a4d24e90")
		assert.NoError(t, err)
		assert.Equal(t, "4e5dcf80_354b_11eb_82cd_f875a4d24e90", uuid)
	}
	{
		_, err := legacyParseRevertUUID("revert 4e5dcf80_354b_11eb_82cd_f875a4")
		assert.Error(t, err)
	}
	{
		_, err := legacyParseRevertUUID("revert vitess_migration '4e5dcf80_354b_11eb_82cd_f875a4d24e90'")
		assert.Error(t, err)
	}
}

func TestParseEnumValues(t *testing.T) {
	{
		inputs := []string{
			`enum('x-small','small','medium','large','x-large')`,
			`ENUM('x-small','small','medium','large','x-large')`,
			`'x-small','small','medium','large','x-large'`,
		}
		for _, input := range inputs {
			enumValues := ParseEnumValues(input)
			assert.Equal(t, `'x-small','small','medium','large','x-large'`, enumValues)
		}
	}
	{
		inputs := []string{
			``,
			`abc`,
			`func('x-small','small','medium','large','x-large')`,
			`set('x-small','small','medium','large','x-large')`,
		}
		for _, input := range inputs {
			enumValues := ParseEnumValues(input)
			assert.Equal(t, input, enumValues)
		}
	}
}

func TestParseSetValues(t *testing.T) {
	{
		inputs := []string{
			`set('x-small','small','medium','large','x-large')`,
			`SET('x-small','small','medium','large','x-large')`,
			`'x-small','small','medium','large','x-large'`,
		}
		for _, input := range inputs {
			setValues := ParseSetValues(input)
			assert.Equal(t, `'x-small','small','medium','large','x-large'`, setValues)
		}
	}
	{
		inputs := []string{
			``,
			`abc`,
			`func('x-small','small','medium','large','x-large')`,
			`enum('x-small','small','medium','large','x-large')`,
			`ENUM('x-small','small','medium','large','x-large')`,
		}
		for _, input := range inputs {
			setValues := ParseSetValues(input)
			assert.Equal(t, input, setValues)
		}
	}
}

func TestParseEnumTokens(t *testing.T) {
	{
		input := `'x-small','small','medium','large','x-large'`
		enumTokens := parseEnumOrSetTokens(input)
		expect := []string{"x-small", "small", "medium", "large", "x-large"}
		assert.Equal(t, expect, enumTokens)
	}
	{
		input := `enum('x-small','small','medium','large','x-large')`
		enumTokens := parseEnumOrSetTokens(input)
		assert.Nil(t, enumTokens)
	}
	{
		input := `set('x-small','small','medium','large','x-large')`
		enumTokens := parseEnumOrSetTokens(input)
		assert.Nil(t, enumTokens)
	}
}

func TestParseEnumTokensMap(t *testing.T) {
	{
		input := `'x-small','small','medium','large','x-large'`

		enumTokensMap := ParseEnumOrSetTokensMap(input)
		expect := map[string]string{
			"1": "x-small",
			"2": "small",
			"3": "medium",
			"4": "large",
			"5": "x-large",
		}
		assert.Equal(t, expect, enumTokensMap)
	}
	{
		inputs := []string{
			`enum('x-small','small','medium','large','x-large')`,
			`set('x-small','small','medium','large','x-large')`,
		}
		for _, input := range inputs {
			enumTokensMap := ParseEnumOrSetTokensMap(input)
			expect := map[string]string{}
			assert.Equal(t, expect, enumTokensMap)
		}
	}
}
