package vtgate

import (
	"fmt"

	"reflect"
	"testing"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestLoadDataInfo_GetRowFromLine(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{
			`"1","a string","100.20"`,
			[]string{"1", "a string", "100.20"},
		},
		{
			`"2","a string containing a , comma","102.20"`,
			[]string{"2", "a string containing a , comma", "102.20"},
		},
		{
			`"3","a string containing a \" quote","102.20"`,
			[]string{"3", "a string containing a \" quote", "102.20"},
		},

		{
			`"4","a string containing a \", quote and comma","102.20"`,
			[]string{"4", "a string containing a \", quote and comma", "102.20"},
		},
		// Test some escape char.
		{
			`"\0\b\n\r\t\Z\\\  \c\'\""`,
			[]string{string([]byte{0, '\b', '\n', '\r', '\t', 26, '\\', ' ', ' ', 'c', '\'', '"'})},
		},
		{
			`"5","a string containing a ',single quote and comma","102.20"`,
			[]string{"5", "a string containing a ',single quote and comma", "102.20"},
		},
		{
			`"6","a string containing a " quote","102.20"`,
			[]string{"6", "a string containing a \" quote", "102.20"},
		},

		//test the null(\N or \N\r)
		{
			`"7","a string containing a " quote","\N"`,
			[]string{"7", "a string containing a \" quote", "NULL"},
		},
	}
	e := &LoadDataInfo{
		FieldsInfo: &sqlparser.FieldsClause{
			Terminated: ",",
			Enclosed:   '"',
		},
	}

	for _, test := range tests {
		if got, err := e.GetRowFromLine([]byte(test.input)); err != nil {
			t.Errorf("GetRowFromLine error: %v", err)
		} else {
			if !reflect.DeepEqual(got, test.expected) {
				t.Errorf("got: %v  but: %v", got, test.expected)
			} else {
				fmt.Printf("GOT: %v   EXPECTED: %v\n", got, test.expected)
			}
		}

	}
}

type LoadParam struct {
	rows           [][]string
	tb             *vindexes.Table
	fields         []*querypb.Field
	sql            string
	maxRowsInBatch int
}

func TestLoadDataInfo_MakeInsert(t *testing.T) {
	tests := []struct {
		input    LoadParam
		expected string
	}{
		{
			LoadParam{
				sql:  "LOAD DATA LOCAL INFILE '/export/hello.csv' INTO TABLE message FIELDS TERMINATED BY '\t'  LINES  TERMINATED BY '\n' ( id,user_id,message,s1,s2,s3,s4)",
				rows: [][]string{{"1", "1", "message1", "s1", "s2", "s3", "s4"}, {"2", "1", "message1", "s1", "s2", "s3", "s4"}, {"3", "1", "message1", "s1", "s2", "s3", "s4"}},
				fields: []*querypb.Field{
					{Name: "id", Type: 263},
					{Name: "user_id", Type: 263},
					{Name: "message", Type: 6165},
					{Name: "s1", Type: 6165},
					{Name: "s2", Type: 6165},
					{Name: "s3", Type: 6165},
					{Name: "s4", Type: 6165},
				},
			},
			"INSERT  IGNORE  INTO message(id,user_id,message,s1,s2,s3,s4) values(1,1,'message1','s1','s2','s3','s4'),(2,1,'message1','s1','s2','s3','s4'),(3,1,'message1','s1','s2','s3','s4')",
		},
		{
			LoadParam{
				sql:  "LOAD DATA LOCAL INFILE '/export/hello.csv' INTO TABLE message FIELDS TERMINATED BY '\t'  LINES  TERMINATED BY '\n' ( id,user_id,message,s1,s2,s3,s4)",
				rows: [][]string{{"1", "1", "message1", "s1", "s2", "s3", "s4"}, {"2", "1", "message1", "s1", "s2", "s3", "s4"}, {"3", "1", "message1", "s1", "s2", "s3", "s4"}},
				fields: []*querypb.Field{
					{Name: "id", Type: 263},
					{Name: "user_id", Type: 263},
					{Name: "message", Type: 6165},
					{Name: "s1", Type: 2061},
					{Name: "s2", Type: 6165},
					{Name: "s3", Type: 6165},
					{Name: "s4", Type: 6165},
				},
			},
			"INSERT  IGNORE  INTO message(id,user_id,message,s1,s2,s3,s4) values(1,1,'message1','s1','s2','s3','s4'),(2,1,'message1','s1','s2','s3','s4'),(3,1,'message1','s1','s2','s3','s4')",
		},
		{
			LoadParam{
				sql:  "LOAD DATA LOCAL INFILE '/export/hello.csv' INTO TABLE message FIELDS TERMINATED BY '\t'  LINES  TERMINATED BY '\n' ( id,user_id,message,s1,s2,s3,s4)",
				rows: [][]string{{"1", "1.0", "message1", "s1\"", "s2", "s3", "s4"}, {"2", "1.0", "message1", "s1\"", "s2", "s3", "s4"}, {"3", "1.0", "message1", "s1\"", "s2", "s3", "s4"}},
				fields: []*querypb.Field{
					{Name: "id", Type: 263},
					{Name: "user_id", Type: 1035},
					{Name: "message", Type: 6165},
					{Name: "s1", Type: 6165},
					{Name: "s2", Type: 6165},
					{Name: "s3", Type: 6165},
					{Name: "s4", Type: 6165},
				},
			},
			"INSERT  IGNORE  INTO message(id,user_id,message,s1,s2,s3,s4) values(1,1.0,'message1','s1\"','s2','s3','s4'),(2,1.0,'message1','s1\"','s2','s3','s4'),(3,1.0,'message1','s1\"','s2','s3','s4')",
		},
		{
			LoadParam{
				sql:  "LOAD DATA LOCAL INFILE '/export/hello.csv' INTO TABLE message FIELDS TERMINATED BY '\t'  LINES  TERMINATED BY '\n'",
				rows: [][]string{{"1", "1.0", "message1", "s1", "s2", "s3"}, {"2", "1.0", "message1", "s1", "s2"}, {"3", "1.0", "message1", "s1", "", "s3", "s4"}},
				fields: []*querypb.Field{
					{Name: "id", Type: 263},
					{Name: "user_id", Type: 1036},
					{Name: "message", Type: 6165},
					{Name: "s1", Type: 6165},
					{Name: "s2", Type: 6165},
					{Name: "s3", Type: 6165},
					{Name: "s4", Type: 6165},
				},
			},
			"INSERT  IGNORE  INTO message(id,user_id,message,s1,s2,s3,s4) values(1,1.0,'message1','s1','s2','s3',''),(2,1.0,'message1','s1','s2','',''),(3,1.0,'message1','s1','','s3','s4')",
		},
	}

	for _, test := range tests {
		stmt, err := sqlparser.Parse(test.input.sql)
		if err != nil {
			t.Errorf("sql: %s parse error: %v", test.input.sql, err)
			return
		}
		loadStmt := stmt.(*sqlparser.LoadDataStmt)
		loadData := NewLoadData()
		loadData.LoadDataInfo.ParseLoadDataPram(loadStmt)
		got, _ := loadData.LoadDataInfo.MakeInsert(test.input.rows, nil, test.input.fields)
		if got != test.expected {
			t.Errorf("\ngot: %v  \nbut: %v", got, test.expected)
		}

	}

}
