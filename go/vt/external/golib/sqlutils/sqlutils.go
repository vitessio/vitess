/*
   Copyright 2014 Outbrain Inc.

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

/*
	This file has been copied over from VTOrc package
*/

package sqlutils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
)

const DateTimeFormat = "2006-01-02 15:04:05.999999"

// RowMap represents one row in a result set. Its objective is to allow
// for easy, typed getters by column name.
type RowMap map[string]CellData

// CellData is the result of a single (atomic) column in a single row
type CellData sql.NullString

func (this *CellData) MarshalJSON() ([]byte, error) {
	if this.Valid {
		return json.Marshal(this.String)
	} else {
		return json.Marshal(nil)
	}
}

// UnmarshalJSON reds this object from JSON
func (this *CellData) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	(*this).String = s
	(*this).Valid = true

	return nil
}

func (this *CellData) NullString() *sql.NullString {
	return (*sql.NullString)(this)
}

// RowData is the result of a single row, in positioned array format
type RowData []CellData

// MarshalJSON will marshal this map as JSON
func (this *RowData) MarshalJSON() ([]byte, error) {
	cells := make([](*CellData), len(*this))
	for i, val := range *this {
		d := CellData(val)
		cells[i] = &d
	}
	return json.Marshal(cells)
}

func (this *RowData) Args() []any {
	result := make([]any, len(*this))
	for i := range *this {
		result[i] = (*(*this)[i].NullString())
	}
	return result
}

// ResultData is an ordered row set of RowData
type ResultData []RowData
type NamedResultData struct {
	Columns []string
	Data    ResultData
}

var EmptyResultData = ResultData{}

func (this *RowMap) GetString(key string) string {
	return (*this)[key].String
}

// GetStringD returns a string from the map, or a default value if the key does not exist
func (this *RowMap) GetStringD(key string, def string) string {
	if cell, ok := (*this)[key]; ok {
		return cell.String
	}
	return def
}

func (this *RowMap) GetInt64(key string) int64 {
	res, _ := strconv.ParseInt(this.GetString(key), 10, 64)
	return res
}

func (this *RowMap) GetInt32(key string) int32 {
	res, _ := strconv.ParseInt(this.GetString(key), 10, 32)
	return int32(res)
}

func (this *RowMap) GetNullInt64(key string) sql.NullInt64 {
	i, err := strconv.ParseInt(this.GetString(key), 10, 64)
	if err == nil {
		return sql.NullInt64{Int64: i, Valid: true}
	} else {
		return sql.NullInt64{Valid: false}
	}
}

func (this *RowMap) GetInt(key string) int {
	res, _ := strconv.Atoi(this.GetString(key))
	return res
}

func (this *RowMap) GetIntD(key string, def int) int {
	res, err := strconv.Atoi(this.GetString(key))
	if err != nil {
		return def
	}
	return res
}

func (this *RowMap) GetUint(key string) uint {
	res, _ := strconv.ParseUint(this.GetString(key), 10, 0)
	return uint(res)
}

func (this *RowMap) GetUintD(key string, def uint) uint {
	res, err := strconv.ParseUint(this.GetString(key), 10, 0)
	if err != nil {
		return def
	}
	return uint(res)
}

func (this *RowMap) GetUint64(key string) uint64 {
	res, _ := strconv.ParseUint(this.GetString(key), 10, 64)
	return res
}

func (this *RowMap) GetUint64D(key string, def uint64) uint64 {
	res, err := strconv.ParseUint(this.GetString(key), 10, 64)
	if err != nil {
		return def
	}
	return res
}

func (this *RowMap) GetUint32(key string) uint32 {
	res, _ := strconv.ParseUint(this.GetString(key), 10, 32)
	return uint32(res)
}

func (this *RowMap) GetBool(key string) bool {
	return this.GetInt(key) != 0
}

func (this *RowMap) GetTime(key string) time.Time {
	if t, err := time.Parse(DateTimeFormat, this.GetString(key)); err == nil {
		return t
	}
	return time.Time{}
}

// knownDBs is a DB cache by uri
var knownDBs map[string]*sql.DB = make(map[string]*sql.DB)
var knownDBsMutex = &sync.Mutex{}

// GetDB returns a DB instance based on uri.
// bool result indicates whether the DB was returned from cache; err
func GetGenericDB(driverName, dataSourceName string) (*sql.DB, bool, error) {
	knownDBsMutex.Lock()
	defer func() {
		knownDBsMutex.Unlock()
	}()

	var exists bool
	if _, exists = knownDBs[dataSourceName]; !exists {
		if db, err := sql.Open(driverName, dataSourceName); err == nil {
			knownDBs[dataSourceName] = db
		} else {
			return db, exists, err
		}
	}
	return knownDBs[dataSourceName], exists, nil
}

// GetDB returns a MySQL DB instance based on uri.
// bool result indicates whether the DB was returned from cache; err
func GetDB(mysql_uri string) (*sql.DB, bool, error) {
	return GetGenericDB("mysql", mysql_uri)
}

// GetSQLiteDB returns a SQLite DB instance based on DB file name.
// bool result indicates whether the DB was returned from cache; err
func GetSQLiteDB(dbFile string) (*sql.DB, bool, error) {
	return GetGenericDB("sqlite", dbFile)
}

// RowToArray is a convenience function, typically not called directly, which maps a
// single read database row into a NullString
func RowToArray(rows *sql.Rows, columns []string) ([]CellData, error) {
	buff := make([]any, len(columns))
	data := make([]CellData, len(columns))
	for i := range buff {
		buff[i] = data[i].NullString()
	}
	err := rows.Scan(buff...)
	return data, err
}

// ScanRowsToArrays is a convenience function, typically not called directly, which maps rows
// already read from the databse into arrays of NullString
func ScanRowsToArrays(rows *sql.Rows, on_row func([]CellData) error) error {
	columns, _ := rows.Columns()
	for rows.Next() {
		arr, err := RowToArray(rows, columns)
		if err != nil {
			return err
		}
		err = on_row(arr)
		if err != nil {
			return err
		}
	}
	return nil
}

func rowToMap(row []CellData, columns []string) map[string]CellData {
	m := make(map[string]CellData)
	for k, data_col := range row {
		m[columns[k]] = data_col
	}
	return m
}

// ScanRowsToMaps is a convenience function, typically not called directly, which maps rows
// already read from the databse into RowMap entries.
func ScanRowsToMaps(rows *sql.Rows, on_row func(RowMap) error) error {
	columns, _ := rows.Columns()
	err := ScanRowsToArrays(rows, func(arr []CellData) error {
		m := rowToMap(arr, columns)
		err := on_row(m)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

// QueryRowsMap is a convenience function allowing querying a result set while poviding a callback
// function activated per read row.
func QueryRowsMap(db *sql.DB, query string, on_row func(RowMap) error, args ...any) (err error) {
	defer func() {
		if derr := recover(); derr != nil {
			err = fmt.Errorf("QueryRowsMap unexpected error: %+v", derr)
		}
	}()

	var rows *sql.Rows
	rows, err = db.Query(query, args...)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil && err != sql.ErrNoRows {
		log.Error(err)
		return err
	}
	err = ScanRowsToMaps(rows, on_row)
	return
}

// ExecNoPrepare executes given query using given args on given DB, without using prepared statements.
func ExecNoPrepare(db *sql.DB, query string, args ...any) (res sql.Result, err error) {
	defer func() {
		if derr := recover(); derr != nil {
			err = fmt.Errorf("ExecNoPrepare unexpected error: %+v", derr)
		}
	}()

	res, err = db.Exec(query, args...)
	if err != nil {
		log.Error(err)
	}
	return res, err
}

// Convert variable length arguments into arguments array
func Args(args ...any) []any {
	return args
}

func NilIfZero(i int64) any {
	if i == 0 {
		return nil
	}
	return i
}
