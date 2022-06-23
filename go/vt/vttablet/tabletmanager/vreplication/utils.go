/*
Copyright 2021 The Vitess Authors.

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

package vreplication

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

const (
	vreplicationLogTableName   = "_vt.vreplication_log"
	createVReplicationLogTable = `CREATE TABLE IF NOT EXISTS _vt.vreplication_log (
		id BIGINT(20) AUTO_INCREMENT,
		vrepl_id INT NOT NULL,
		type VARBINARY(256) NOT NULL,
		state VARBINARY(100) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		message text NOT NULL,
		count BIGINT(20) NOT NULL DEFAULT 1,
		PRIMARY KEY (id))`
)

const (
	// Enum values for type column of _vt.vreplication_log

	// LogStreamCreate is used when a row in _vt.vreplication is inserted via VReplicationExec
	LogStreamCreate = "Stream Created"
	// LogStreamUpdate is used when a row in _vt.vreplication is updated via VReplicationExec
	LogStreamUpdate = "Stream Updated"
	// LogStreamDelete is used when a row in _vt.vreplication is deleted via VReplicationExec
	LogStreamDelete = "Stream Deleted"
	// LogMessage is used for generic log messages
	LogMessage = "Message"
	// LogCopyStart is used when the copy phase is started
	LogCopyStart = "Started Copy Phase"
	// LogCopyEnd is used when the copy phase is done
	LogCopyEnd = "Ended Copy Phase"
	// LogStateChange is used when the state of the stream changes
	LogStateChange = "State Changed"

	// TODO: LogError is not used atm. Currently irrecoverable errors, resumable errors and informational messages
	//  are all treated the same: the message column is updated and state left as Running.
	//  Every five seconds we reset the message and retry streaming so that we can automatically resume from a temporary
	//  loss of connectivity or a reparent. We need to detect if errors are not recoverable and set an Error status.
	//  Since this device (of overloading the message) is strewn across the code, and incorrectly flagging resumable
	//  errors can stall workflows, we have deferred implementing it.

	// LogError indicates that there is an error from which we cannot recover and the operator needs to intervene.
	LogError = "Error"
)

func getLastLog(dbClient *vdbClient, vreplID uint32) (id int64, typ, state, message string, err error) {
	var qr *sqltypes.Result
	query := fmt.Sprintf("select id, type, state, message from _vt.vreplication_log where vrepl_id = %d order by id desc limit 1", vreplID)
	if qr, err = withDDL.Exec(context.Background(), query, dbClient.ExecuteFetch, dbClient.ExecuteFetch); err != nil {
		return 0, "", "", "", err
	}
	if len(qr.Rows) != 1 {
		return 0, "", "", "", nil
	}
	row := qr.Rows[0]
	id, _ = evalengine.ToInt64(row[0])
	typ = row[1].ToString()
	state = row[2].ToString()
	message = row[3].ToString()
	return id, typ, state, message, nil
}

func insertLog(dbClient *vdbClient, typ string, vreplID uint32, state, message string) error {
	// getLastLog returns the last log for a stream. During insertion, if the type/state/message match we do not insert
	// a new log but increment the count. This prevents spamming of the log table in case the same message is logged continuously.
	id, _, lastLogState, lastLogMessage, err := getLastLog(dbClient, vreplID)
	if err != nil {
		return err
	}
	if typ == LogStateChange && state == lastLogState {
		// handles case where current state is Running, controller restarts after an error and initializes the state Running
		return nil
	}
	var query string
	if id > 0 && message == lastLogMessage {
		query = fmt.Sprintf("update _vt.vreplication_log set count = count + 1 where id = %d", id)
	} else {
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("insert into _vt.vreplication_log(vrepl_id, type, state, message) values(%s, %s, %s, %s)",
			strconv.Itoa(int(vreplID)), encodeString(typ), encodeString(state), encodeString(message))
		query = buf.ParsedQuery().Query
	}
	if _, err = withDDL.Exec(context.Background(), query, dbClient.ExecuteFetch, dbClient.ExecuteFetch); err != nil {
		return fmt.Errorf("could not insert into log table: %v: %v", query, err)
	}
	return nil
}

// insertLogWithParams is called when a stream is created. The attributes of the stream are stored as a json string
func insertLogWithParams(dbClient *vdbClient, action string, vreplID uint32, params map[string]string) error {
	var message string
	if params != nil {
		obj, _ := json.Marshal(params)
		message = string(obj)
	}
	if err := insertLog(dbClient, action, vreplID, params["state"], message); err != nil {
		return err
	}
	return nil
}

// isUnrecoverableError returns true if vreplication cannot recover from the given error and should completely terminate
func isUnrecoverableError(err error) bool {
	if err == nil {
		return false
	}
	sqlErr, isSQLErr := err.(*mysql.SQLError)
	if !isSQLErr {
		return false
	}
	switch sqlErr.Num {
	case
		mysql.ERWarnDataOutOfRange,
		mysql.ERDataTooLong,
		mysql.ERWarnDataTruncated,
		mysql.ERTruncatedWrongValue,
		mysql.ERTruncatedWrongValueForField,
		mysql.ERIllegalValueForType,
		mysql.ErrWrongValueForType,
		mysql.ErrCantCreateGeometryObject,
		mysql.ErrGISDataWrongEndianess,
		mysql.ErrNotImplementedForCartesianSRS,
		mysql.ErrNotImplementedForProjectedSRS,
		mysql.ErrNonPositiveRadius,
		mysql.ERBadNullError,
		mysql.ERDupEntry,
		mysql.ERNoDefaultForField,
		mysql.ERInvalidJSONText,
		mysql.ERInvalidJSONTextInParams,
		mysql.ERInvalidJSONBinaryData,
		mysql.ERInvalidJSONCharset,
		mysql.ERInvalidCastToJSON,
		mysql.ERJSONValueTooBig,
		mysql.ERJSONDocumentTooDeep:
		log.Errorf("Got unrecoverable error: %v", sqlErr)
		return true
	}
	return false
}
