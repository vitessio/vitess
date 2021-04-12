package vreplication

import (
	"encoding/json"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

const (
	createVReplicationLog = `CREATE TABLE IF NOT EXISTS _vt.vreplication_log (
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
	// LogError is used when there is an error from which we cannot recover and the operator needs to fix something
	LogError = "Error"
)

func getLastLog(dbClient *vdbClient, vreplID uint32) (int64, string, string, string, error) {
	var qr *sqltypes.Result
	var err error
	query := fmt.Sprintf("select id, type, state, message from _vt.vreplication_log where vrepl_id = %d order by id desc limit 1", vreplID)
	if qr, err = dbClient.ExecuteFetch(query, 1); err != nil {
		return 0, "", "", "", err
	}
	if len(qr.Rows) != 1 {
		return 0, "", "", "", nil
	}
	row := qr.Rows[0]
	if len(row) != 4 {
		return 0, "", "", "", nil
	}
	id, _ := evalengine.ToInt64(row[0])
	typ := row[1].String()
	state := row[2].String()
	message := row[3].String()
	return id, typ, state, message, nil
}

func insertLog(dbClient *vdbClient, typ string, vreplID uint32, state, message string) error {
	var query string

	id, currentType, currentState, currentMessage, err := getLastLog(dbClient, vreplID)
	if err != nil {
		return err
	}
	if id > 0 && typ == currentType && state == currentState && message == currentMessage {
		query = fmt.Sprintf("update _vt.vreplication_log set count = count + 1 where id = %d", id)
	} else {
		query = `insert into _vt.vreplication_log(vrepl_id, type, state, message) values(%d, '%s', '%s', %s)`
		query = fmt.Sprintf(query, vreplID, typ, state, encodeString(message))
	}
	if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
		return fmt.Errorf("could not insert into log table: %v: %v", query, err)
	}
	return nil
}

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
