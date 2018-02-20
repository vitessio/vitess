package messages

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/vitessdriver"
)

// Subscription allows users to interact with the queue
type Subscription struct {
	q *Queue

	open   bool
	openMu sync.Mutex

	// Subscribe requires unique connection string properties,
	// so we will manage that on behalf of the user
	db *sql.DB

	// store db connection details
	dbConfig vitessdriver.Configuration

	newFieldsFunc func() []interface{}

	// this stores all of the in flight messages
	msgBuf []Message

	// these buffered channels manage max message processing concurrency

	// these messages are ready to be filled with data from the db
	waitingForDataChan chan *Message

	// these messages are waiting for a subscriber to process them
	readyForProcessingChan chan *Message

	streamSQL string
	ackSQL    string
	failSQL   string
}

func (q *Queue) newSubscription(maxConcurrent int) *Subscription {
	s := &Subscription{
		q:                      q,
		open:                   false,
		openMu:                 sync.Mutex{},
		msgBuf:                 make([]Message, maxConcurrent),
		waitingForDataChan:     make(chan *Message, maxConcurrent),
		readyForProcessingChan: make(chan *Message, maxConcurrent),
		streamSQL:              fmt.Sprintf("stream * from `%s`", q.name),
		ackSQL:                 fmt.Sprintf("UPDATE `%s` SET time_acked=?, time_next=null WHERE time_scheduled=? AND id=? AND time_acked is null", q.name),
		failSQL:                fmt.Sprintf("UPDATE `%s` SET time_next=%d WHERE time_scheduled=? AND id=? AND time_acked is null", q.name, math.MaxInt64),
	}

	// initialize all the individual messages
	// each message will be reset as it is reused
	for i := range s.msgBuf {
		// create a pointer to the message for convenience
		m := &s.msgBuf[i]

		// add a reference to the original queue
		m.s = s

		// create a permanent set of scan fields
		m.scanFields = []interface{}{
			&m.timeScheduled,
			&m.ID,
		}

		// if the user has set up custom fields, initialize them
		if q.userFieldNames != nil {
			m.customFieldData = s.newFieldsFunc()

			// add a pointer to each of the individual user fields to scanFields
			for j := range m.customFieldData {
				m.scanFields = append(m.scanFields, &m.customFieldData[j])
			}
		}
	}

	return s
}

// Get returns the next available message. It blocks until either a message
// is available or the context is cancelled
func (s *Subscription) Get(ctx context.Context) (*Message, error) {
	select {
	case m := <-s.readyForProcessingChan:
		if m.err != nil {
			return nil, m.err
		}
		return m, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Ack marks a message as successfully completed
func (m *Message) Ack(ctx context.Context) error {
	defer m.s.putMessage(m)
	_, err := m.s.db.ExecContext(ctx, m.s.ackSQL, time.Now().UTC().UnixNano(), m.timeScheduled, m.ID)
	return err
}

// Fail marks a task as failed, and it will not be queued again until manual action is taken
func (m *Message) Fail(ctx context.Context) error {
	defer m.s.putMessage(m)
	_, err := m.s.db.ExecContext(ctx, m.s.failSQL, m.timeScheduled, m.ID)
	return err
}

// Subscribe returns a subscription
// Context cancellation is respected
func (q *Queue) Subscribe(ctx context.Context) (*Subscription, error) {
	q.s.openMu.Lock()
	defer q.s.openMu.Unlock()

	if q.s.open {
		return q.s, nil
	}

	// open a direct database connection if needed
	if q.s.db == nil {
		if err := q.s.openDB(); err != nil {
			return nil, err
		}
	}

	// start a streaming query that will run indefinitely
	rows, err := q.s.db.QueryContext(ctx, q.s.streamSQL)
	if err != nil {
		return nil, err
	}

	// this goroutine will be waiting for rows and is the only writer to the channel
	go func() {
		defer rows.Close()

		// we don't need to check for context cancellation, because that is already
		// happening behind the scenes in the Vitess database/sql driver
		for rows.Next() {
			m := <-q.s.waitingForDataChan
			m.err = rows.Scan(m.scanFields...)

			// send the message through the channel
			q.s.readyForProcessingChan <- m
		}

		// close the channel before exiting
		close(q.s.readyForProcessingChan)
	}()

	return q.s, nil
}

// putMessage pushes a message back into the queue for resue
func (s *Subscription) putMessage(m *Message) {
	s.waitingForDataChan <- m
}

func (s *Subscription) openDB() error {
	var err error
	s.db, err = vitessdriver.OpenWithConfiguration(s.dbConfig)
	if err != nil {
		return err
	}
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1)

	return nil
}
