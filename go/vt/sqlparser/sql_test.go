package sqlparser

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
					{Column: ColIdent{val: "col1"}, Order: AscScr},
				},
			},
		},
		{
			"ALTER TABLE tbl_abc ADD INDEX (col1)",
			TableName{
				Name: TableIdent{"tbl_abc"},
			},
			&IndexSpec{
				Action: CreateStr,
				ToName: ColIdent{val: ""},
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "col1"}, Order: AscScr},
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
				Type:   "unique",
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "w"}, Order: AscScr},
					{Column: ColIdent{val: "X"}, Order: AscScr},
					{Column: ColIdent{val: "y"}, Order: AscScr},
					{Column: ColIdent{val: "z"}, Order: AscScr},
				},
			},
		},
		{
			"ALTER TABLE asdf ADD UNIQUE (w,X, y , z)",
			TableName{
				Name: TableIdent{"asdf"},
			},
			&IndexSpec{
				Action: CreateStr,
				ToName: ColIdent{},
				Type:   "unique",
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "w"}, Order: AscScr},
					{Column: ColIdent{val: "X"}, Order: AscScr},
					{Column: ColIdent{val: "y"}, Order: AscScr},
					{Column: ColIdent{val: "z"}, Order: AscScr},
				},
			},
		},
		{
			"ALTER TABLE asdf ADD CONSTRAINT abc UNIQUE (w,X, y , z)",
			TableName{
				Name: TableIdent{"asdf"},
			},
			&IndexSpec{
				Action: CreateStr,
				ToName: ColIdent{},
				Type:   "unique",
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "w"}, Order: AscScr},
					{Column: ColIdent{val: "X"}, Order: AscScr},
					{Column: ColIdent{val: "y"}, Order: AscScr},
					{Column: ColIdent{val: "z"}, Order: AscScr},
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
				Type:   "spatial",
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "bestcol"}, Order: AscScr},
				},
				Options: []*IndexOption{{Name: "COMMENT", Value: &SQLVal{Type: StrVal, Val: []byte("hello world")}}},
			},
		},
		{
			"ALTER TABLE tableBOI ADD KEY sOmEiNdEx USING BTREE (ye DESC, ne) USING HASH", // doesn't make sense but valid
			TableName{
				Name: TableIdent{"tableBOI"},
			},
			&IndexSpec{
				Action: CreateStr,
				ToName: ColIdent{val: "sOmEiNdEx"},
				Using:  ColIdent{val: "BTREE"},
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "ye"}, Order: DescScr},
					{Column: ColIdent{val: "ne"}, Order: AscScr},
				},
				Options: []*IndexOption{{Name: "USING", Using: "HASH"}},
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
				Action:   RenameStr,
				FromName: ColIdent{val: "idx_w"},
				ToName:   ColIdent{val: "idx_v"},
			},
		},
		{
			"ALTER TABLE asdf RENAME KEY wxyzIndex TO indexWXYZ",
			TableName{
				Name: TableIdent{"asdf"},
			},
			&IndexSpec{
				Action:   RenameStr,
				FromName: ColIdent{val: "wxyzIndex"},
				ToName:   ColIdent{val: "indexWXYZ"},
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
					{Column: ColIdent{val: "col1"}, Order: AscScr},
				},
			},
		},
		{
			"CREATE UNIQUE INDEX wxyzIndex ON asdf (w,X DESC, y , z)",
			TableName{
				Name: TableIdent{"asdf"},
			},
			&IndexSpec{
				Action: CreateStr,
				ToName: ColIdent{val: "wxyzIndex"},
				Type:   "unique",
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "w"}, Order: AscScr},
					{Column: ColIdent{val: "X"}, Order: DescScr},
					{Column: ColIdent{val: "y"}, Order: AscScr},
					{Column: ColIdent{val: "z"}, Order: AscScr},
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
				Type:   "spatial",
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "bestcol"}, Order: AscScr},
				},
				Options: []*IndexOption{{Name: "COMMENT", Value: &SQLVal{Type: StrVal, Val: []byte("hello world")}}},
			},
		},
		{
			"CREATE INDEX sOmEiNdEx USING BTREE ON tableBOI (ye, ne DESC) USING HASH", // doesn't make sense but valid
			TableName{
				Name: TableIdent{"tableBOI"},
			},
			&IndexSpec{
				Action: CreateStr,
				ToName: ColIdent{val: "sOmEiNdEx"},
				Using:  ColIdent{val: "BTREE"},
				Columns: []*IndexColumn{
					{Column: ColIdent{val: "ye"}, Order: AscScr},
					{Column: ColIdent{val: "ne"}, Order: DescScr},
				},
				Options: []*IndexOption{{Name: "USING", Using: "HASH"}},
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
				Name:      TableIdent{"tbl_abc"},
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
				Name:      TableIdent{"tableYeah"},
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

func TestCreateForeignKey(t *testing.T) {
	tests := []testForeignKeyStruct{
		{
			`CREATE TABLE child (
				id INT,
				parent_id INT,
				FOREIGN KEY (parent_id)
					REFERENCES parent(id)
			)`,
			[]*ConstraintDefinition{
				{
					Name: "",
					Details: &ForeignKeyDefinition{
						Source:            Columns{{val: "parent_id"}},
						ReferencedTable:   TableName{Name: TableIdent{v: "parent"}},
						ReferencedColumns: Columns{{val: "id"}},
						OnUpdate:          DefaultAction,
						OnDelete:          DefaultAction,
					},
				},
			},
		},
		{
			`CREATE TABLE child (
				id INT,
				parent_id INT,
				INDEX par_ind (parent_id),
				FOREIGN KEY (parent_id)
					REFERENCES parent(id)
					ON UPDATE RESTRICT ON DELETE CASCADE
			)`,
			[]*ConstraintDefinition{
				{
					Name: "",
					Details: &ForeignKeyDefinition{
						Source:            Columns{{val: "parent_id"}},
						ReferencedTable:   TableName{Name: TableIdent{v: "parent"}},
						ReferencedColumns: Columns{{val: "id"}},
						OnUpdate:          Restrict,
						OnDelete:          Cascade,
					},
				},
			},
		},
		{
			`CREATE TABLE child (
				id INT,
				parent_id INT,
				INDEX par_ind (parent_id),
				FOREIGN KEY (parent_id)
					REFERENCES parent(id)
					ON DELETE NO ACTION ON UPDATE SET DEFAULT
			)`,
			[]*ConstraintDefinition{
				{
					Name: "",
					Details: &ForeignKeyDefinition{
						Source:            Columns{{val: "parent_id"}},
						ReferencedTable:   TableName{Name: TableIdent{v: "parent"}},
						ReferencedColumns: Columns{{val: "id"}},
						OnUpdate:          SetDefault,
						OnDelete:          NoAction,
					},
				},
			},
		},
		{
			`CREATE TABLE child (
				id INT,
				parent_id INT,
				INDEX par_ind (parent_id),
				FOREIGN KEY (parent_id)
					REFERENCES parent(id)
					ON DELETE CASCADE
			)`,
			[]*ConstraintDefinition{
				{
					Name: "",
					Details: &ForeignKeyDefinition{
						Source:            Columns{{val: "parent_id"}},
						ReferencedTable:   TableName{Name: TableIdent{v: "parent"}},
						ReferencedColumns: Columns{{val: "id"}},
						OnUpdate:          DefaultAction,
						OnDelete:          Cascade,
					},
				},
			},
		},
		{
			`CREATE TABLE child (
				id INT,
				parent_id INT,
				INDEX par_ind (parent_id),
				FOREIGN KEY (parent_id)
					REFERENCES parent(id)
					ON UPDATE SET NULL
			)`,
			[]*ConstraintDefinition{
				{
					Name: "",
					Details: &ForeignKeyDefinition{
						Source:            Columns{{val: "parent_id"}},
						ReferencedTable:   TableName{Name: TableIdent{v: "parent"}},
						ReferencedColumns: Columns{{val: "id"}},
						OnUpdate:          SetNull,
						OnDelete:          DefaultAction,
					},
				},
			},
		},
		{
			`CREATE TABLE child (
				id INT,
				parents_id INT,
				parents_sub_id INT,
				INDEX par_ind (parents_id, parents_sub_id),
				CONSTRAINT fk_parents FOREIGN KEY (parents_id, parents_sub_id)
					REFERENCES parents(id, sub_id)
			)`,
			[]*ConstraintDefinition{
				{
					Name: "fk_parents",
					Details: &ForeignKeyDefinition{
						Source:            Columns{{val: "parents_id"}, {val: "parents_sub_id"}},
						ReferencedTable:   TableName{Name: TableIdent{v: "parents"}},
						ReferencedColumns: Columns{{val: "id"}, {val: "sub_id"}},
						OnUpdate:          DefaultAction,
						OnDelete:          DefaultAction,
					},
				},
			},
		},
	}

	testForeignKey(t, tests, AddStr)
}

func TestAlterAddForeignKey(t *testing.T) {
	tests := []testForeignKeyStruct{
		{
			`ALTER TABLE child ADD
				FOREIGN KEY (parent_id)
					REFERENCES parent(id)`,
			[]*ConstraintDefinition{
				{
					Name: "",
					Details: &ForeignKeyDefinition{
						Source:            Columns{{val: "parent_id"}},
						ReferencedTable:   TableName{Name: TableIdent{v: "parent"}},
						ReferencedColumns: Columns{{val: "id"}},
						OnUpdate:          DefaultAction,
						OnDelete:          DefaultAction,
					},
				},
			},
		},
		{
			`ALTER TABLE child ADD
				FOREIGN KEY (parent_id)
					REFERENCES parent(id)
					ON UPDATE RESTRICT ON DELETE CASCADE`,
			[]*ConstraintDefinition{
				{
					Name: "",
					Details: &ForeignKeyDefinition{
						Source:            Columns{{val: "parent_id"}},
						ReferencedTable:   TableName{Name: TableIdent{v: "parent"}},
						ReferencedColumns: Columns{{val: "id"}},
						OnUpdate:          Restrict,
						OnDelete:          Cascade,
					},
				},
			},
		},
		{
			`ALTER TABLE child ADD
				FOREIGN KEY (parent_id)
					REFERENCES parent(id)
					ON DELETE NO ACTION ON UPDATE SET DEFAULT`,
			[]*ConstraintDefinition{
				{
					Name: "",
					Details: &ForeignKeyDefinition{
						Source:            Columns{{val: "parent_id"}},
						ReferencedTable:   TableName{Name: TableIdent{v: "parent"}},
						ReferencedColumns: Columns{{val: "id"}},
						OnUpdate:          SetDefault,
						OnDelete:          NoAction,
					},
				},
			},
		},
		{
			`ALTER TABLE child ADD
				FOREIGN KEY (parent_id)
					REFERENCES parent(id)
					ON DELETE CASCADE`,
			[]*ConstraintDefinition{
				{
					Name: "",
					Details: &ForeignKeyDefinition{
						Source:            Columns{{val: "parent_id"}},
						ReferencedTable:   TableName{Name: TableIdent{v: "parent"}},
						ReferencedColumns: Columns{{val: "id"}},
						OnUpdate:          DefaultAction,
						OnDelete:          Cascade,
					},
				},
			},
		},
		{
			`ALTER TABLE child ADD
				FOREIGN KEY (parent_id)
					REFERENCES parent(id)
					ON UPDATE SET NULL`,
			[]*ConstraintDefinition{
				{
					Name: "",
					Details: &ForeignKeyDefinition{
						Source:            Columns{{val: "parent_id"}},
						ReferencedTable:   TableName{Name: TableIdent{v: "parent"}},
						ReferencedColumns: Columns{{val: "id"}},
						OnUpdate:          SetNull,
						OnDelete:          DefaultAction,
					},
				},
			},
		},
		{
			`ALTER TABLE child ADD
				CONSTRAINT fk_parents FOREIGN KEY (parents_id, parents_sub_id)
					REFERENCES parents(id, sub_id)`,
			[]*ConstraintDefinition{
				{
					Name: "fk_parents",
					Details: &ForeignKeyDefinition{
						Source:            Columns{{val: "parents_id"}, {val: "parents_sub_id"}},
						ReferencedTable:   TableName{Name: TableIdent{v: "parents"}},
						ReferencedColumns: Columns{{val: "id"}, {val: "sub_id"}},
						OnUpdate:          DefaultAction,
						OnDelete:          DefaultAction,
					},
				},
			},
		},
	}

	testForeignKey(t, tests, AddStr)
}

func TestAlterDropForeignKey(t *testing.T) {
	tests := []testForeignKeyStruct{
		{
			`ALTER TABLE child DROP FOREIGN KEY fk_parent_id`,
			[]*ConstraintDefinition{{
				Name: "fk_parent_id",
				Details: &ForeignKeyDefinition{
					Source:            Columns{},
					ReferencedTable:   TableName{},
					ReferencedColumns: Columns{},
					OnUpdate:          DefaultAction,
					OnDelete:          DefaultAction,
				},
			}},
		},
		{
			`ALTER TABLE child DROP FOREIGN KEY random_foreign_key_name`,
			[]*ConstraintDefinition{{
				Name: "random_foreign_key_name",
				Details: &ForeignKeyDefinition{
					Source:            Columns{},
					ReferencedTable:   TableName{},
					ReferencedColumns: Columns{},
					OnUpdate:          DefaultAction,
					OnDelete:          DefaultAction,
				},
			}},
		},
	}

	testForeignKey(t, tests, DropStr)
}

func TestAlterDropConstraint(t *testing.T) {
	tests := []testForeignKeyStruct{
		{
			`ALTER TABLE child DROP CONSTRAINT random_foreign_key_name`,
			[]*ConstraintDefinition{{
				Name: "random_foreign_key_name",
			}},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.statement), func(t *testing.T) {
			res, err := Parse(test.statement)
			require.NoError(t, err)
			ddlRes, ok := res.(*DDL)
			if !ok {
				mAlterDDL, ok := res.(*MultiAlterDDL)
				require.True(t, ok)
				require.Len(t, mAlterDDL.Statements, 1)
				ddlRes = mAlterDDL.Statements[0]
			}
			require.NotNil(t, ddlRes.TableSpec)
			require.Equal(t, len(test.res), len(ddlRes.TableSpec.Constraints))
			require.Equal(t, DropStr, ddlRes.ConstraintAction)
			for i := range test.res {
				require.NotNil(t, ddlRes.TableSpec.Constraints[i])
				assert.Equal(t, test.res[i].Name, ddlRes.TableSpec.Constraints[i].Name)
				require.Nil(t, ddlRes.TableSpec.Constraints[i].Details)
			}
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
			if !ok {
				mAlterDDL, ok := res.(*MultiAlterDDL)
				require.True(t, ok)
				require.Len(t, mAlterDDL.Statements, 1)
				ddlRes = mAlterDDL.Statements[0]
			}
			assert.Equal(t, AlterStr, ddlRes.Action)
			assert.Equal(t, test.resTable, ddlRes.Table)
			assert.Equal(t, test.res, ddlRes.IndexSpec)
		})
	}
}

type testForeignKeyStruct struct {
	statement string
	res       []*ConstraintDefinition
}

func testForeignKey(t *testing.T, tests []testForeignKeyStruct, expectedConstraintAction string) {
	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.statement), func(t *testing.T) {
			res, err := Parse(test.statement)
			require.NoError(t, err)
			ddlRes, ok := res.(*DDL)
			if !ok {
				mAlterDDL, ok := res.(*MultiAlterDDL)
				require.True(t, ok)
				require.Len(t, mAlterDDL.Statements, 1)
				ddlRes = mAlterDDL.Statements[0]
			}
			require.NotNil(t, ddlRes.TableSpec)
			require.Equal(t, len(test.res), len(ddlRes.TableSpec.Constraints))
			require.Equal(t, expectedConstraintAction, ddlRes.ConstraintAction)
			for i := range test.res {
				require.NotNil(t, ddlRes.TableSpec.Constraints[i])
				require.NotNil(t, ddlRes.TableSpec.Constraints[i].Details)
				assert.Equal(t, test.res[i].Name, ddlRes.TableSpec.Constraints[i].Name)
				fkDefinition, ok := ddlRes.TableSpec.Constraints[i].Details.(*ForeignKeyDefinition)
				require.True(t, ok)
				testFkDefinition := test.res[i].Details.(*ForeignKeyDefinition)
				if assert.Equal(t, len(testFkDefinition.Source), len(fkDefinition.Source)) {
					for i := range testFkDefinition.Source {
						assert.Equal(t, testFkDefinition.Source[i].val, fkDefinition.Source[i].val)
					}
				}
				assert.Equal(t, testFkDefinition.ReferencedTable.Name.v, fkDefinition.ReferencedTable.Name.v)
				if assert.Equal(t, len(testFkDefinition.ReferencedColumns), len(fkDefinition.ReferencedColumns)) {
					for i := range testFkDefinition.ReferencedColumns {
						assert.Equal(t, testFkDefinition.ReferencedColumns[i].val, fkDefinition.ReferencedColumns[i].val)
					}
				}
				assert.Equal(t, testFkDefinition.OnUpdate, fkDefinition.OnUpdate)
				assert.Equal(t, testFkDefinition.OnDelete, fkDefinition.OnDelete)
			}
		})
	}
}
