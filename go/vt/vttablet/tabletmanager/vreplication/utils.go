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
	"encoding/json"
	"fmt"
	"strconv"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	vreplicationLogTableName = "vreplication_log"
	// This comes from the fact that the message column in the vreplication_log table is of type TEXT.
	maxVReplicationLogMessageLen = 65535
)

const (
	// Enum values for type column in the vreplication_log table.

	// LogStreamCreate is used when a row in the vreplication table is inserted via VReplicationExec.
	LogStreamCreate = "Stream Created"
	// LogStreamUpdate is used when a row in the vreplication table is updated via VReplicationExec.
	LogStreamUpdate = "Stream Updated"
	// LogStreamDelete is used when a row in the vreplication table is deleted via VReplicationExec.
	LogStreamDelete = "Stream Deleted"
	// LogMessage is used for generic log messages.
	LogMessage = "Message"
	// LogCopyStart is used when the copy phase is started.
	LogCopyStart = "Started Copy Phase"
	// LogCopyEnd is used when the copy phase is done.
	LogCopyEnd = "Ended Copy Phase"
	// LogStateChange is used when the state of the stream changes.
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

func getLastLog(dbClient *vdbClient, vreplID int32) (id int64, typ, state, message string, err error) {
	var qr *sqltypes.Result
	query := fmt.Sprintf("select id, type, state, message from %s.vreplication_log where vrepl_id = %d order by id desc limit 1",
		sidecar.GetIdentifier(), vreplID)
	if qr, err = dbClient.Execute(query); err != nil {
		return 0, "", "", "", err
	}
	if len(qr.Rows) != 1 {
		return 0, "", "", "", nil
	}
	row := qr.Rows[0]
	id, _ = row[0].ToCastInt64()
	typ = row[1].ToString()
	state = row[2].ToString()
	message = row[3].ToString()
	return id, typ, state, message, nil
}

func insertLog(dbClient *vdbClient, typ string, vreplID int32, state, message string) {
	// getLastLog returns the last log for a stream. During insertion, if the type/state/message match we do not insert
	// a new log but increment the count. This prevents spamming of the log table in case the same message is logged continuously.
	id, _, lastLogState, lastLogMessage, err := getLastLog(dbClient, vreplID)
	if err != nil {
		log.Errorf("Could not insert vreplication_log record because we failed to get the last log record: %v", err)
		return
	}
	if typ == LogStateChange && state == lastLogState {
		// handles case where current state is Running, controller restarts after an error and initializes the state Running
		return
	}
	var query string
	if id > 0 && message == lastLogMessage {
		query = fmt.Sprintf("update %s.vreplication_log set count = count + 1 where id = %d", sidecar.GetIdentifier(), id)
	} else {
		buf := sqlparser.NewTrackedBuffer(nil)
		if len(message) > maxVReplicationLogMessageLen {
			message, err = textutil.TruncateText(message, maxVReplicationLogMessageLen, binlogplayer.TruncationLocation, binlogplayer.TruncationIndicator)
			if err != nil {
				log.Errorf("Could not insert vreplication_log record because we failed to truncate the message: %v", err)
				return
			}
		}
		buf.Myprintf("insert into %s.vreplication_log(vrepl_id, type, state, message) values(%s, %s, %s, %s)",
			sidecar.GetIdentifier(), strconv.Itoa(int(vreplID)), encodeString(typ), encodeString(state), encodeString(message))
		query = buf.ParsedQuery().Query
	}
	if _, err = dbClient.ExecuteFetch(query, 10000); err != nil {
		log.Errorf("Could not insert into vreplication_log table: %v: %v", query, err)
	}
}

// insertLogWithParams is called when a stream is created. The attributes of the stream are stored as a json string.
func insertLogWithParams(dbClient *vdbClient, action string, vreplID int32, params map[string]string) {
	var message string
	if params != nil {
		obj, _ := json.Marshal(params)
		message = string(obj)
	}
	insertLog(dbClient, action, vreplID, params["state"], message)
}

// isUnrecoverableError returns true if vreplication cannot recover from the given error and should completely terminate.
func isUnrecoverableError(err error) bool {
	if err == nil {
		return false
	}
	if vterrors.Code(err) == vtrpcpb.Code_FAILED_PRECONDITION {
		return true
	}
	sqlErr, isSQLErr := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
	if !isSQLErr {
		return false
	}
	if sqlErr.Num == sqlerror.ERUnknownError {
		return false
	}
	switch sqlErr.Num {
	case
		// in case-insensitive alphabetical order
		sqlerror.ERAccessDeniedError,
		sqlerror.ERBadFieldError,
		sqlerror.ERBadNullError,
		sqlerror.ERCantDropFieldOrKey,
		sqlerror.ERDataOutOfRange,
		sqlerror.ERDataTooLong,
		sqlerror.ERDBAccessDenied,
		sqlerror.ERDupEntry,
		sqlerror.ERDupFieldName,
		sqlerror.ERDupKeyName,
		sqlerror.ERDupUnique,
		sqlerror.ERFeatureDisabled,
		sqlerror.ERFunctionNotDefined,
		sqlerror.ERIllegalValueForType,
		sqlerror.ERInvalidCastToJSON,
		sqlerror.ERInvalidJSONBinaryData,
		sqlerror.ERInvalidJSONCharset,
		sqlerror.ERInvalidJSONText,
		sqlerror.ERInvalidJSONTextInParams,
		sqlerror.ERJSONDocumentTooDeep,
		sqlerror.ERJSONValueTooBig,
		sqlerror.ERRegexpError,
		sqlerror.ERRegexpStringNotTerminated,
		sqlerror.ERRegexpIllegalArgument,
		sqlerror.ERRegexpIndexOutOfBounds,
		sqlerror.ERRegexpInternal,
		sqlerror.ERRegexpRuleSyntax,
		sqlerror.ERRegexpBadEscapeSequence,
		sqlerror.ERRegexpUnimplemented,
		sqlerror.ERRegexpMismatchParen,
		sqlerror.ERRegexpBadInterval,
		sqlerror.ERRRegexpMaxLtMin,
		sqlerror.ERRegexpInvalidBackRef,
		sqlerror.ERRegexpLookBehindLimit,
		sqlerror.ERRegexpMissingCloseBracket,
		sqlerror.ERRegexpInvalidRange,
		sqlerror.ERRegexpStackOverflow,
		sqlerror.ERRegexpTimeOut,
		sqlerror.ERRegexpPatternTooBig,
		sqlerror.ERRegexpInvalidCaptureGroup,
		sqlerror.ERRegexpInvalidFlag,
		sqlerror.ERNoDefault,
		sqlerror.ERNoDefaultForField,
		sqlerror.ERNonUniq,
		sqlerror.ERNonUpdateableTable,
		sqlerror.ERNoSuchTable,
		sqlerror.ERNotAllowedCommand,
		sqlerror.ERNotSupportedYet,
		sqlerror.EROptionPreventsStatement,
		sqlerror.ERParseError,
		sqlerror.ERPrimaryCantHaveNull,
		sqlerror.ErrCantCreateGeometryObject,
		sqlerror.ErrGISDataWrongEndianess,
		sqlerror.ErrNonPositiveRadius,
		sqlerror.ErrNotImplementedForCartesianSRS,
		sqlerror.ErrNotImplementedForProjectedSRS,
		sqlerror.ErrWrongValueForType,
		sqlerror.ERSPDoesNotExist,
		sqlerror.ERSpecifiedAccessDenied,
		sqlerror.ERSyntaxError,
		sqlerror.ERTooBigRowSize,
		sqlerror.ERTooBigSet,
		sqlerror.ERTruncatedWrongValue,
		sqlerror.ERTruncatedWrongValueForField,
		sqlerror.ERUnknownCollation,
		sqlerror.ERUnknownProcedure,
		sqlerror.ERUnknownTable,
		sqlerror.ERWarnDataOutOfRange,
		sqlerror.ERWarnDataTruncated,
		sqlerror.ERWrongFKDef,
		sqlerror.ERWrongFieldSpec,
		sqlerror.ERWrongParamCountToProcedure,
		sqlerror.ERWrongParametersToProcedure,
		sqlerror.ERWrongUsage,
		sqlerror.ERWrongValue,
		sqlerror.ERWrongValueCountOnRow:
		log.Errorf("Got unrecoverable error: %v", sqlErr)
		return true
	}
	return false
}
