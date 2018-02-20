package messages

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/youtube/vitess/go/vt/vitessdriver"
)

// Ack marks a message as successfully completed
func (m *Message) Ack(ctx context.Context, conn Queryer) error {
	defer m.q.putSubscribeMessage(m)

	query := fmt.Sprintf("UPDATE `%s` SET time_acked=?, time_next=null WHERE id IN(?) AND time_acked is null", m.q.name)
	_, err := conn.ExecContext(ctx, query, time.Now().UTC().UnixNano(), m.ID)
	return err
}

// Fail marks a task as failed, and it will not be queued again until manual action is taken
func (m *Message) Fail(ctx context.Context, conn Queryer) error {
	defer m.q.putSubscribeMessage(m)

	query := fmt.Sprintf("UPDATE `%s` SET time_next=%d WHERE id IN(?) AND time_acked is null", m.q.name, math.MaxInt64)
	_, err := conn.ExecContext(ctx, query, time.Now().UTC().UnixNano(), m.ID)
	return err
}

// Subscribe returns a channel of tasks
// Context cancellation is respected
func (q *Queue) Subscribe(ctx context.Context) (<-chan *Message, error) {
	// if the channel has already been created, return it
	if q.readyForProcessingChan != nil {
		return q.readyForProcessingChan, nil
	}

	// open a direct database connection if needed
	if q.db == nil {
		if err := q.openDB(); err != nil {
			return nil, err
		}
	}

	// start a streaming query that will run indefinitely
	query := fmt.Sprintf("stream * from `%s`", q.name)
	rows, err := q.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	// define the message channel to communicate
	q.readyForProcessingChan = make(chan *Message, len(q.waitingForDataChan))

	// this goroutine will be waiting for rows and is the only writer to the channel
	go func() {
		defer rows.Close()

		// we don't need to check for context cancellation, because that is already
		// happening behind the scenes in the Vitess database/sql driver
		for rows.Next() {
			m := q.getSubscribeMessage()
			m.Err = rows.Scan(m.scanFields...)

			// send the message through the channel
			q.readyForProcessingChan <- m
		}

		// close the channel before exiting
		close(q.readyForProcessingChan)
	}()

	return q.readyForProcessingChan, nil
}

// getSubscribeMessage blocks until a message is available for reuse
func (q *Queue) getSubscribeMessage() *Message {
	return <-q.waitingForDataChan
}

// putSubscribeMessage pushes a message back into the queue for resue
func (q *Queue) putSubscribeMessage(m *Message) {
	q.waitingForDataChan <- m
}

func (q *Queue) openDB() error {
	var err error
	q.db, err = vitessdriver.OpenWithConfiguration(q.dbConfig)
	if err != nil {
		return err
	}
	q.db.SetMaxOpenConns(1)
	q.db.SetMaxIdleConns(1)

	return nil
}
