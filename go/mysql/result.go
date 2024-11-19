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
	"fmt"
	"strconv"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// ParseResult parses a query result from the raw MySQL packets received in the query.ExecuteResponse.
func ParseResult(er *querypb.ExecuteResponse, wantfields bool) (*sqltypes.Result, error) {
	if len(er.RawPackets) == 0 {
		return sqltypes.Proto3ToResult(er.Result), nil
	}

	result := &sqltypes.Result{}
	return updateResFromRaw(er, wantfields, result)
}

func updateResFromRaw(er *querypb.ExecuteResponse, wantfields bool, result *sqltypes.Result) (*sqltypes.Result, error) {
	// log.Errorf("interpreting raw packets")

	colcount, _, ok := readLenEncInt(er.RawPackets[0], 0)
	if !ok {
		return nil, sqlerror.NewSQLError(sqlerror.CRMalformedPacket, sqlerror.SSUnknownSQLState, "cannot get column number")
	}

	// log.Errorf("col count: %d", colcount)
	// 0  1  2  3  4  5  6
	// 2 c1 c2 r1 r2 r3 ok

	var err error
	fieldArray := make([]querypb.Field, colcount)
	fieldPackets := er.RawPackets[1 : colcount+1]
	var rowPackets [][]byte
	if len(er.RawPackets) > int(colcount)+2 {
		rowPackets = er.RawPackets[colcount+1 : len(er.RawPackets)-1]
	}
	okPacket := er.RawPackets[len(er.RawPackets)-1]

	result.Fields = make([]*querypb.Field, len(fieldPackets))
	result.Rows = make([]sqltypes.Row, 0, len(rowPackets))

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

	// log.Errorf("fields: %+v", result.Fields)

	for _, rowpkt := range rowPackets {
		r, err := parseRow(rowpkt, result.Fields, readLenEncStringAsBytes, nil)
		if err != nil {
			return nil, err
		}
		// log.Errorf("row %d: %+v", x, r)
		result.Rows = append(result.Rows, r)
	}

	var packetOK PacketOK
	if err = parseOKPacket(&packetOK, okPacket, true, false); err != nil {
		return nil, err
	}
	result.RowsAffected = packetOK.affectedRows
	result.InsertID = packetOK.lastInsertID
	result.SessionStateChanges = packetOK.sessionStateData
	result.StatusFlags = packetOK.statusFlags
	result.Info = packetOK.info

	// log.Errorf("final result: %+v", result)

	return result, nil
}

func formatPbError(err error, buf []mem.Buffer) error {
	var q []byte

	for _, b := range buf {
		q = strconv.AppendQuoteToASCII(q, string(b.ReadOnlyData()))
	}
	return fmt.Errorf("failed to serialize PB: %w\nraw data: %s", err, q)
}

// ParseResultFromRawForTest is used in test that unmarshal the raw packets in sqltypes.Result and parses the packets to provide query result in the respective fields of sqltypes.Result.
func ParseResultFromRawForTest(result *sqltypes.Result) (*sqltypes.Result, error) {
	if len(result.RawPackets) == 0 {
		return result, nil
	}
	defer result.RawPackets.Free()
	er := &querypb.ExecuteResponse{}
	c := encoding.GetCodecV2("proto")
	err := c.Unmarshal(result.RawPackets, er)
	if err != nil {
		return nil, formatPbError(err, result.RawPackets)
	}
	return updateResFromRaw(er, true, result)
}
