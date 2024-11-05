/*
Copyright 2024 The Vitess Authors.

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

package mysql

import (
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// ParseResult converts the raw packets in a QueryResult to a sqltypes.Result.
func ParseResult(qr *querypb.QueryResult, wantfields bool) (*sqltypes.Result, error) {
	if qr.RawPackets == nil {
		return sqltypes.Proto3ToResult(qr), nil
	}

	var colcount int
	for i, p := range qr.RawPackets {
		if len(p) == 0 {
			colcount = i
			break
		}
	}

	var err error
	fieldArray := make([]querypb.Field, colcount)
	fieldPackets := qr.RawPackets[:colcount]
	rowPackets := qr.RawPackets[colcount+1:]

	result := &sqltypes.Result{
		RowsAffected:        qr.RowsAffected,
		InsertID:            qr.InsertId,
		SessionStateChanges: qr.SessionStateChanges,
		Info:                qr.Info,
		Fields:              make([]*querypb.Field, len(fieldPackets)),
		Rows:                make([]sqltypes.Row, 0, len(rowPackets)),
	}

	for i, fieldpkt := range fieldPackets {
		result.Fields[i] = &fieldArray[i]
		if wantfields {
			err = parseColumnDefinition(fieldpkt, result.Fields[i], i)
		} else {
			err = parseColumnDefinitionType(fieldpkt, result.Fields[i], i)
		}
		if err != nil {
			return nil, err
		}
	}

	for _, rowpkt := range rowPackets {
		r, err := parseRow(rowpkt, result.Fields, readLenEncStringAsBytes, nil)
		if err != nil {
			return nil, err
		}
		result.Rows = append(result.Rows, r)
	}

	return result, nil
}
