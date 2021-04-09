package vreplication

import (
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

const (
	createVReplicationLog = `CREATE TABLE IF NOT EXISTS _vt.vreplication_log (
		id BIGINT(20) AUTO_INCREMENT,
		vrepl_id INT NOT NULL,
		state VARBINARY(100) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		message text NOT NULL,
		count BIGINT(20) NOT NULL DEFAULT 1,
		PRIMARY KEY (id))`
)

func getLastLog(dbClient *vdbClient, vreplID uint32) (int64, string, string, error) {
	var qr *sqltypes.Result
	var err error
	query := fmt.Sprintf("select id, state, message from _vt.vreplication_log where vrepl_id = %d order by id desc limit 1", vreplID)
	if qr, err = dbClient.ExecuteFetch(query, 1); err != nil {
		return 0, "", "", err
	}
	if len(qr.Rows) != 1 {
		return 0, "", "", nil
	}
	row := qr.Rows[0]
	id, _ := evalengine.ToInt64(row[0])
	state := row[1].String()
	message := row[2].String()
	return id, state, message, nil
}

func insertLog(dbClient *vdbClient, vreplID uint32, state, message string) error {
	var query string

	id, currentState, currentMessage, err := getLastLog(dbClient, vreplID)
	if err != nil {
		return err
	}
	if id > 0 && state == currentState && message == currentMessage {
		query = fmt.Sprintf("update _vt.vreplication_log set count = count + 1 where id = %d", id)
	} else {
		query = `insert into _vt.vreplication_log(vrepl_id, state, message) values(%d, '%s', %s)`
		query = fmt.Sprintf(query, vreplID, state, encodeString(message))
	}
	if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
		return fmt.Errorf("could not insert into log table: %v: %v", query, err)
	}
	return nil
}
