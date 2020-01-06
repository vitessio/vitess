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

package preparestmt

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"github.com/stretchr/testify/assert"
)

// DBInfo information about the database.
type DBInfo struct {
	Username     string
	Password     string
	Host         string
	Port         uint
	KeyspaceName string
	Params       []string
}

// ConnectionString generates the connection string using dbinfo.
func (db DBInfo) ConnectionString(params ...string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", db.Username, db.Password, db.Host,
		db.Port, db.KeyspaceName, strings.Join(append(db.Params, params...), "&"))
}

// createConfig create file in to Tmp dir in vtdataroot and write the given data.
func createConfig(name, data string) error {
	// creating new file
	f, err := os.Create(clusterInstance.TmpDirectory + name)
	if err != nil {
		return err
	}

	if data == "" {
		return nil
	}

	// write the given data
	_, err = fmt.Fprint(f, data)
	return err
}

// GetORM will connect the vtgate through mysql protocol.
func GetORM(t *testing.T, params ...string) *gorm.DB {

	dbo := GetORMByConnectionString(t, dbInfo.ConnectionString(params...))
	if sqlDebug {
		dbo.Debug()
	}
	return dbo
}

// GetORMByConnectionString connect database using connection string.
func GetORMByConnectionString(t *testing.T, str string) *gorm.DB {
	dbo, err := gorm.Open("mysql", str)
	assert.Nil(t, err)
	return dbo
}

// execWithIgnore executes the prepared query, and ignore the given error codes.
func execWithIgnore(t *testing.T, dbo *gorm.DB, errorCodes []uint16, stmt string, params ...interface{}) {
	if err := dbo.Exec(stmt, params...).Error; err != nil {
		assert.Contains(t, errorCodes, err.(*mysql.MySQLError).Number)

		fmt.Printf("error ignored, %v", err)
	}
}

// exec executes the query using the params.
func exec(t *testing.T, dbo *gorm.DB, stmt string, params ...interface{}) {
	assert.Nil(t, execErr(dbo, stmt, params...))
}

// execErr execute the query and return error.
func execErr(dbo *gorm.DB, stmt string, params ...interface{}) *mysql.MySQLError {
	if err := dbo.Exec(stmt, params...).Error; err != nil {
		return err.(*mysql.MySQLError)
	}
	return nil
}

// selectWhere select the row corresponds to the where condition.
func selectWhere(t *testing.T, dbo *gorm.DB, where string, params ...interface{}) []tableData {
	var out []tableData
	err := dbo.Table(tableName).Where(where, params...).Scan(&out).Error
	assert.Nil(t, err)
	return out
}
