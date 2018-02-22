package messages

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
)

// Execer lets functions accept a DB or a Tx without knowing the difference
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// A Queue represents a Vitess message queue
type Queue struct {
	name           string
	userFieldNames []string
	newFieldsFunc  func() []interface{}
	maxConcurrent  int

	// predefine these sql strings
	insertSQL       string
	insertFutureSQL string

	s *subscription
}

// A QueueOption lets users customize the queue object
type QueueOption func(q *Queue) error

// NewQueue returns a queue definition
func NewQueue(ctx context.Context, name string, maxConcurrent int, fieldNames []string, newFieldsFunc func() []interface{}) (*Queue, error) {
	if maxConcurrent < 1 {
		return nil, errors.New("maxConcurrent must be greater than 0")
	}

	q := &Queue{
		name:           name,
		maxConcurrent:  maxConcurrent,
		userFieldNames: fieldNames,
		newFieldsFunc:  newFieldsFunc,
	}

	args := newFieldsFunc()
	if len(args) != len(fieldNames) {
		return nil, errors.New("user fields mismatch")
	}

	// only do this string manipulation once
	q.insertSQL = q.generateInsertSQL()
	q.insertFutureSQL = q.generateInsertFutureSQL()

	return q, nil
}

// generateInsertSQL does the string manipulation to generate the insert statement
func (q *Queue) generateInsertSQL() string {
	buf := bytes.Buffer{}

	// generate default insert into queue with required fields
	buf.WriteString("INSERT INTO `")
	buf.WriteString(q.name)
	buf.WriteString("` (id")

	// add quoted user fields to the insert statement
	for _, f := range q.userFieldNames {
		buf.WriteString(", `")
		buf.WriteString(f)
		buf.WriteString("`")
	}
	buf.WriteString(") VALUES (?")

	// add params representing user data
	buf.WriteString(strings.Repeat(",?", len(q.userFieldNames)))

	// close VALUES
	buf.WriteString(")")

	return buf.String()
}

// generateInsertFutureSQL does the string manipulation to generate the insertFuture statement
func (q *Queue) generateInsertFutureSQL() string {
	buf := bytes.Buffer{}

	// generate default insert into queue with required fields
	buf.WriteString("INSERT INTO `")
	buf.WriteString(q.name)
	buf.WriteString("` (time_scheduled, id")

	// add quoted user fields to the insert statement
	for _, f := range q.userFieldNames {
		buf.WriteString(", `")
		buf.WriteString(f)
		buf.WriteString("`")
	}
	buf.WriteString(") VALUES (?,?")

	// add params representing user data
	buf.WriteString(strings.Repeat(",?", len(q.userFieldNames)))

	// close VALUES
	buf.WriteString(")")

	return buf.String()
}
