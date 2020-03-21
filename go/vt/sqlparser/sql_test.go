package sqlparser

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAlterCreateIndex(t *testing.T) {
	tests := []testIndexStruct{
		{
			"ALTER TABLE tbl_abc ADD INDEX idx_v (col1)",
			TableName{
				Name: TableIdent{"tbl_abc"},
			},
			&IndexSpec{
				Action: CreateStr,
				ToName: ColIdent{val: "idx_v"},
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "col1"}},
				},
			},
		},
		{
			"ALTER TABLE asdf ADD UNIQUE KEY wxyzIndex (w,X, y , z)",
			TableName{
				Name: TableIdent{"asdf"},
			},
			&IndexSpec{
				Action: CreateStr,
				ToName: ColIdent{val: "wxyzIndex"},
				Type: "unique",
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "w"}},
					{Column: ColIdent{val: "X"}},
					{Column: ColIdent{val: "y"}},
					{Column: ColIdent{val: "z"}},
				},
			},
		},
		{
			"ALTER TABLE besttable ADD SPATIAL INDEX bestindex (bestcol) COMMENT 'hello world'",
			TableName{
				Name: TableIdent{"besttable"},
			},
			&IndexSpec{
				Action: CreateStr,
				ToName: ColIdent{val: "bestindex"},
				Type: "spatial",
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "bestcol"}},
				},
				Options: []*IndexOption{{Name: "comment", Value: &SQLVal{Type:StrVal, Val: []byte("hello world")}}},
			},
		},
		{
			"ALTER TABLE tableBOI ADD KEY sOmEiNdEx USING BTREE (ye, ne) USING HASH", // doesn't make sense but valid
			TableName{
				Name: TableIdent{"tableBOI"},
			},
			&IndexSpec{
				Action: CreateStr,
				ToName: ColIdent{val: "sOmEiNdEx"},
				Using: ColIdent{val: "BTREE"},
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "ye"}},
					{Column: ColIdent{val: "ne"}},
				},
				Options: []*IndexOption{{Name: "using", Using: "HASH"}},
			},
		},
	}
	testIndex(t, tests)
}

func TestAlterDropIndex(t *testing.T) {
	tests := []testIndexStruct{
		{
			"ALTER TABLE tbl_abc DROP INDEX idx_v",
			TableName{
				Name: TableIdent{"tbl_abc"},
			},
			&IndexSpec{
				Action: DropStr,
				ToName: ColIdent{val: "idx_v"},
			},
		},
		{
			"ALTER TABLE tableYeah DROP KEY otherName",
			TableName{
				Name: TableIdent{"tableYeah"},
			},
			&IndexSpec{
				Action: DropStr,
				ToName: ColIdent{val: "otherName"},
			},
		},
	}
	testIndex(t, tests)
}

func TestAlterRenameIndex(t *testing.T) {
	tests := []testIndexStruct{
		{
			"ALTER TABLE tbl_abc RENAME INDEX idx_w TO idx_v",
			TableName{
				Name: TableIdent{"tbl_abc"},
			},
			&IndexSpec{
				Action: RenameStr,
				FromName: ColIdent{val: "idx_w"},
				ToName: ColIdent{val: "idx_v"},
			},
		},
		{
			"ALTER TABLE asdf RENAME KEY wxyzIndex TO indexWXYZ",
			TableName{
				Name: TableIdent{"asdf"},
			},
			&IndexSpec{
				Action: RenameStr,
				FromName: ColIdent{val: "wxyzIndex"},
				ToName: ColIdent{val: "indexWXYZ"},
			},
		},
	}
	testIndex(t, tests)
}

func TestCreateIndex(t *testing.T) {
	tests := []testIndexStruct{
		{
			"CREATE INDEX idx_v ON tbl_abc (col1)",
			TableName{
				Name: TableIdent{"tbl_abc"},
			},
			&IndexSpec{
				Action: CreateStr,
				ToName: ColIdent{val: "idx_v"},
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "col1"}},
				},
			},
		},
		{
			"CREATE UNIQUE INDEX wxyzIndex ON asdf (w,X, y , z)",
			TableName{
				Name: TableIdent{"asdf"},
			},
			&IndexSpec{
				Action: CreateStr,
				ToName: ColIdent{val: "wxyzIndex"},
				Type: "unique",
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "w"}},
					{Column: ColIdent{val: "X"}},
					{Column: ColIdent{val: "y"}},
					{Column: ColIdent{val: "z"}},
				},
			},
		},
		{
			"CREATE SPATIAL INDEX bestindex ON besttable (bestcol) COMMENT 'hello world'",
			TableName{
				Name: TableIdent{"besttable"},
			},
			&IndexSpec{
				Action: CreateStr,
				ToName: ColIdent{val: "bestindex"},
				Type: "spatial",
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "bestcol"}},
				},
				Options: []*IndexOption{{Name: "comment", Value: &SQLVal{Type:StrVal, Val: []byte("hello world")}}},
			},
		},
		{
			"CREATE INDEX sOmEiNdEx USING BTREE ON tableBOI (ye, ne) USING HASH", // doesn't make sense but valid
			TableName{
				Name: TableIdent{"tableBOI"},
			},
			&IndexSpec{
				Action: CreateStr,
				ToName: ColIdent{val: "sOmEiNdEx"},
				Using: ColIdent{val: "BTREE"},
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "ye"}},
					{Column: ColIdent{val: "ne"}},
				},
				Options: []*IndexOption{{Name: "using", Using: "HASH"}},
			},
		},
	}
	testIndex(t, tests)
}

func TestDropIndex(t *testing.T) {
	tests := []testIndexStruct{
		{
			"DROP INDEX idx_v ON tbl_abc",
			TableName{
				Name: TableIdent{"tbl_abc"},
				Qualifier: TableIdent{},
			},
			&IndexSpec{
				Action: DropStr,
				ToName: ColIdent{val: "idx_v"},
			},
		},
		{
			"DROP INDEX otherName ON tableYeah",
			TableName{
				Name: TableIdent{"tableYeah"},
				Qualifier: TableIdent{},
			},
			&IndexSpec{
				Action: DropStr,
				ToName: ColIdent{val: "otherName"},
			},
		},
	}
	testIndex(t, tests)
}

func TestShowIndex(t *testing.T) {
	tests := []struct {
		statement string
		res       *Show
	}{
		{
			"SHOW INDEX FROM tbl_abc",
			&Show{
				Type: IndexStr,
				Table: TableName{
					Name: TableIdent{"tbl_abc"},
				},
			},
		},
		{
			"SHOW INDEXES IN tableYeah FROM dbYeah",
			&Show{
				Type: IndexStr,
				Table: TableName{
					Name: TableIdent{"tableYeah"},
				},
				Database: "dbYeah",
			},
		},
		{
			"SHOW KEYS FROM tabe IN dabe WHERE Key_name = 'idx_v'",
			&Show{
				Type: IndexStr,
				Table: TableName{
					Name: TableIdent{"tabe"},
				},
				Database: "dabe",
				ShowIndexFilterOpt: &ComparisonExpr{
					Operator: "=",
					Left: &ColName{
						Name: ColIdent{val: "Key_name"},
					},
					Right: &SQLVal{
						Type: StrVal,
						Val:  []byte("idx_v"),
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.statement), func(t *testing.T) {
			res, err := Parse(test.statement)
			require.NoError(t, err)
			showRes, ok := res.(*Show)
			require.True(t, ok)
			assert.Equal(t, test.res, showRes)
		})
	}
}

type testIndexStruct struct {
	statement string
	resTable  TableName
	res       *IndexSpec
}
func testIndex(t *testing.T, tests []testIndexStruct) {
	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.statement), func(t *testing.T) {
			res, err := Parse(test.statement)
			require.NoError(t, err)
			ddlRes, ok := res.(*DDL)
			require.True(t, ok)
			assert.Equal(t, AlterStr, ddlRes.Action)
			assert.Equal(t, test.resTable, ddlRes.Table)
			assert.Equal(t, test.res, ddlRes.IndexSpec)
		})
	}
}