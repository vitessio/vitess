package datapump

import (
	"fmt"

	"vitess.io/vitess/go/mysql"
)

var (
	createStatement = `
		CREATE TABLE %s.%s (
			id bigint(20) auto_increment,
			vc varchar(32) default "default",
			vb varbinary(32) default '123' ,
			j json  not null,
			e enum('individual','soho','enterprise') default 'individual',
			s set('football','cricket','baseball') default 'cricket',
			t timestamp not null default current_timestamp,
			i int unsigned not null default 0,
			PRIMARY KEY (id)
		) ENGINE=InnoDB
`
	insertStatement = "insert into %s.%s(j) values('{}')"
	updateStatement = "update %s.%s set i = i + 1, j = '{}' order by rand() limit 1"
	deleteStatement = "delete from %s.%s order by rand() limit 1"
)

type StatementType int

const (
	InsertStatement StatementType = iota
	UpdateStatement
	DeleteStatement
)

// for now just a single table

type TestTable struct {
	tableName, create, insert, update, delete string
	dbConn                                    *mysql.Conn
}

func (tt *TestTable) Insert() error {
	_, err := tt.dbConn.ExecuteFetch(tt.insert, 1, false)
	return err
}

func (tt *TestTable) Update() error {
	_, err := tt.dbConn.ExecuteFetch(tt.update, 1, false)
	return err
}

func (tt *TestTable) Delete() error {
	_, err := tt.dbConn.ExecuteFetch(tt.delete, 1, false)
	return err
}

func NewTestTable(tableName, keyspace string, dbConn *mysql.Conn) *TestTable {
	tt := &TestTable{
		tableName: tableName,
		dbConn:    dbConn,
		create:    fmt.Sprintf(createStatement, keyspace, tableName),
		insert:    fmt.Sprintf(insertStatement, keyspace, tableName),
		update:    fmt.Sprintf(updateStatement, keyspace, tableName),
		delete:    fmt.Sprintf(deleteStatement, keyspace, tableName),
	}
	return tt
}

type ITestTable interface {
	Insert() error
	Update() error
	Delete() error
}

var _ ITestTable = (*TestTable)(nil)
