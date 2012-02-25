/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package tabletserver

import (
	"vitess/mysql"
	"vitess/relog"
	"vitess/stats"
	"time"
)

var mysqlStats *stats.Timings
var QueryLogger *relog.Logger

func init() {
	mysqlStats = stats.NewTimings("MySQL")
}

type PoolConnection interface {
	// Smart has to return a physical struct so there's no ambiguity about the object type
	Smart() *SmartConnection
	Recycle()
}

type CreateConnectionFunc func() (connection *SmartConnection, err error)

type SmartConnection struct {
	Conn     *mysql.Connection
	IsClosed bool
}

func (self *SmartConnection) ExecuteFetch(query []byte, maxrows int) (*QueryResult, error) {
	start := time.Now()
	if QueryLogger != nil {
		QueryLogger.Info("%s", query)
	}
	mqr, err := self.Conn.ExecuteFetch(query, maxrows)
	if err != nil {
		mysqlStats.Record("Exec", start)
		if sqlErr, ok := err.(*mysql.SqlError); ok {
			if sqlErr.Number() >= 2000 && sqlErr.Number() <= 2018 { // mysql connection errors
				self.Close()
			}
			if sqlErr.Number() == 1317 { // Query was interrupted
				self.Close()
			}
		}
		return nil, err
	}
	mysqlStats.Record("Exec", start)
	qr := QueryResult(*mqr)
	return &qr, nil
}

func (self *SmartConnection) Id() int64 {
	return self.Conn.Id()
}

func (self *SmartConnection) Close() {
	if self.IsClosed {
		return
	}
	self.IsClosed = true
	self.Conn.Close()
}

// CreateConnection returns a connection for running user queries. No DDL.
func CreateConnection(socketPath, dbName string) (*SmartConnection, error) {
	info := map[string]interface{}{
		"host":        "localhost",
		"port":        0,
		"unix_socket": socketPath,
		"uname":       "vt_app",
		"pass":        "",
		"dbname":      dbName,
		"charset":     "utf8",
	}
	c, err := mysql.Connect(info)
	return &SmartConnection{Conn: c}, err
}

// ConnectionCreator creates a closure that wraps CreateConnection
func ConnectionCreator(socketPath, dbName string) CreateConnectionFunc {
	return func() (connection *SmartConnection, err error) {
		return CreateConnection(socketPath, dbName)
	}
}

/* CreateSuperConnection retuns a connection for doing DDLs and maintenence operations
where you need full control over mysql.
*/
func CreateSuperConnection(socketPath, dbName string) (*SmartConnection, error) {
	info := map[string]interface{}{
		"host":        "localhost",
		"port":        0,
		"unix_socket": socketPath,
		"uname":       "vt_dba",
		"pass":        "",
		"dbname":      dbName,
		"charset":     "utf8",
	}
	c, err := mysql.Connect(info)
	return &SmartConnection{Conn: c}, err
}

// SuperConnectionCreator is a closure that wraps CreateSuperConnection
func SuperConnectionCreator(socketPath, dbName string) CreateConnectionFunc {
	return func() (connection *SmartConnection, err error) {
		return CreateSuperConnection(socketPath, dbName)
	}
}

func CreateGenericConnection(info map[string]interface{}) (*SmartConnection, error) {
	c, err := mysql.Connect(info)
	return &SmartConnection{Conn: c}, err
}

func GenericConnectionCreator(info map[string]interface{}) CreateConnectionFunc {
	return func() (connection *SmartConnection, err error) {
		return CreateGenericConnection(info)
	}
}
