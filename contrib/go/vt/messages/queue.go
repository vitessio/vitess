package messages

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"

	"github.com/youtube/vitess/go/vt/vitessdriver"
)

// Queryer lets most functions accept a DB or a Tx without knowing the difference
type Queryer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// A Queue represents a Vitess message queue
type Queue struct {
	name           string
	userFieldNames []string

	// -------------------------------------------
	// AddMessage only fields
	// -------------------------------------------

	// to reduce GC pressure, reuse messages after inserting them
	addPool sync.Pool

	// insert params are calculated and the string is
	insertSQL string

	s *Subscription
}

// A Message stores information about a message
type Message struct {
	s *Subscription

	// preconfigured scan targets that point at the below standard fields,
	// along with pointers to the customFieldData slice
	scanFields []interface{}

	TimeScheduled   int64
	ID              int64
	TimeNext        int64
	Epoch           int64
	TimeCreated     *int64
	TimeAcked       *int64
	Message         interface{}
	customFieldData []interface{}

	// Err is only set if there is a scan error in Subscribe
	Err error
}

// A QueueOption lets users customize the queue object
type QueueOption func(q *Queue) error

// CustomFields defines user fields and a generator function to create new arg types
func CustomFields(fieldNames []string, newFieldsFunc func() []interface{}) QueueOption {
	return func(q *Queue) error {
		q.userFieldNames = fieldNames
		q.s.newFieldsFunc = newFieldsFunc

		args := newFieldsFunc()
		if len(args) != len(fieldNames) {
			return errors.New("user fields mismatch")
		}

		return nil
	}
}

// SubscribeConfig stores db connection details. This option doesn't open the db,
// so a connection is only made lazily when Subscribe is called
func SubscribeConfig(address, target string) QueueOption {
	return func(q *Queue) error {
		q.s.dbConfig = vitessdriver.Configuration{
			Address:   address,
			Target:    target,
			Streaming: true,
		}
		return nil
	}
}

// NewQueue returns a queue definition
func NewQueue(ctx context.Context, name string, maxConcurrent int, opts ...QueueOption) (*Queue, error) {
	if maxConcurrent < 1 {
		return nil, errors.New("maxConcurrent must be greater than 0")
	}

	q := &Queue{
		name: name,
		addPool: sync.Pool{
			New: func() interface{} { return &Message{} },
		},
	}

	// execute all the queue options
	for _, opt := range opts {
		if err := opt(q); err != nil {
			return nil, err
		}
	}

	// only do this string manipulation once
	q.insertSQL = q.generateInsertSQL()

	// generate the raw subscription
	q.s = q.newSubscription(maxConcurrent)

	return q, nil
}

// generateInsertSQL does the string manipulation to generate the insert statement
func (q *Queue) generateInsertSQL() string {
	buf := bytes.Buffer{}

	// generate default insert into queue with required fields
	buf.WriteString("INSERT INTO `")
	buf.WriteString(q.name)
	buf.WriteString("` (time_scheduled, id, epoch, message")

	// add quoted user fields to the insert statement
	for _, f := range q.userFieldNames {
		buf.WriteString(", `")
		buf.WriteString(f)
		buf.WriteString("`")
	}
	buf.WriteString(") VALUES (?,?,?,?")

	// add params representing user data
	buf.WriteString(strings.Repeat(",?", len(q.userFieldNames)))

	// close VALUES
	buf.WriteString(")")

	return buf.String()
}
