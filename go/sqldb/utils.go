/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqldb

import (
	"fmt"
	"strconv"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
)

// This file contains utility methods for Conn objects.

// ExecuteFetchMap returns a map from column names to cell data for a query
// that should return exactly 1 row.
func ExecuteFetchMap(conn Conn, query string) (map[string]string, error) {
	qr, err := conn.ExecuteFetch(query, 1, true)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 {
		return nil, fmt.Errorf("query %#v returned %d rows, expected 1", query, len(qr.Rows))
	}
	if len(qr.Fields) != len(qr.Rows[0]) {
		return nil, fmt.Errorf("query %#v returned %d column names, expected %d", query, len(qr.Fields), len(qr.Rows[0]))
	}

	rowMap := make(map[string]string)
	for i, value := range qr.Rows[0] {
		rowMap[qr.Fields[i].Name] = value.String()
	}
	return rowMap, nil
}

// GetCharset returns the current numerical values of the per-session character
// set variables.
func GetCharset(conn Conn) (*binlogdatapb.Charset, error) {
	// character_set_client
	row, err := ExecuteFetchMap(conn, "SHOW COLLATION WHERE `charset`=@@session.character_set_client AND `default`='Yes'")
	if err != nil {
		return nil, err
	}
	client, err := strconv.ParseInt(row["Id"], 10, 16)
	if err != nil {
		return nil, err
	}

	// collation_connection
	row, err = ExecuteFetchMap(conn, "SHOW COLLATION WHERE `collation`=@@session.collation_connection")
	if err != nil {
		return nil, err
	}
	connection, err := strconv.ParseInt(row["Id"], 10, 16)
	if err != nil {
		return nil, err
	}

	// collation_server
	row, err = ExecuteFetchMap(conn, "SHOW COLLATION WHERE `collation`=@@session.collation_server")
	if err != nil {
		return nil, err
	}
	server, err := strconv.ParseInt(row["Id"], 10, 16)
	if err != nil {
		return nil, err
	}

	return &binlogdatapb.Charset{
		Client: int32(client),
		Conn:   int32(connection),
		Server: int32(server),
	}, nil
}

// SetCharset changes the per-session character set variables.
func SetCharset(conn Conn, cs *binlogdatapb.Charset) error {
	sql := fmt.Sprintf(
		"SET @@session.character_set_client=%d, @@session.collation_connection=%d, @@session.collation_server=%d",
		cs.Client, cs.Conn, cs.Server)
	_, err := conn.ExecuteFetch(sql, 1, false)
	return err
}
