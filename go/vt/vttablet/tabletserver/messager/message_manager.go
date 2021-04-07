/*
Copyright 2019 The Vitess Authors.

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

package messager

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	// MessageStats tracks stats for messages.
	MessageStats = stats.NewGaugesWithMultiLabels(
		"Messages",
		"Stats for messages",
		[]string{"TableName", "Metric"})
)

type messageReceiver struct {
	ctx     context.Context
	errChan chan error
	send    func(*sqltypes.Result) error
	cancel  context.CancelFunc
}

func newMessageReceiver(ctx context.Context, send func(*sqltypes.Result) error) (*messageReceiver, <-chan struct{}) {
	ctx, cancel := context.WithCancel(ctx)
	rcv := &messageReceiver{
		ctx:     ctx,
		errChan: make(chan error, 1),
		send:    send,
		cancel:  cancel,
	}
	return rcv, ctx.Done()
}

func (rcv *messageReceiver) Send(qr *sqltypes.Result) error {
	// We have to use a channel so we can also
	// monitor the context.
	go func() {
		rcv.errChan <- rcv.send(qr)
	}()
	select {
	case <-rcv.ctx.Done():
		return io.EOF
	case err := <-rcv.errChan:
		if err == io.EOF {
			// This is only a failsafe. If we received an EOF,
			// grpc would have already canceled the context.
			rcv.cancel()
		}
		return err
	}
}

// receiverWithStatus is a separate struct to signify
// that the busy flag is controlled by the messageManager
// mutex.
type receiverWithStatus struct {
	receiver *messageReceiver
	busy     bool
}

// messageManager manages messages for a message table.
//
// messageManager has three core components that interact with each other.
// 1. The cache: this is essentially the send queue. Its size is limited by the
// number of rows.
// 2. The send loop: this loop pulls items out of the cache and sends them to the
// various clients.
// 3. The poller: this wakes up periodically to fill the cache with values by
// reading the message table from the database.
// The message manager operates in three modes:
//
// Idle mode
// This mode is entered if there is no client connected or if there are
// no outstanding messages to be sent. In this mode:
// The cache is empty.
// The send loop is in a cond.Wait state doing nothing.
// The poller wakes up periodically, but terminates immediately.
// Idle mode is exited when there is at least one client connected
// and there are pending messages to be sent.
//
// Steady state mode
// In this mode, there are connected clients and there is a continuous
// stream of messages being sent. The cache is not full, and there are
// occasional resends.
// Message creation inserts rows to the database and adds them to the cache.
// Each addition makes sure the send loop is awakened if it's waiting.
// The send loop continuously pulls items out of the cache and sends them.
// After every successful send, the messages are postponed, and are
// also removed from the cache.
// The poller wakes up periodically, loads messages that are due and adds them
// to the cache. Most of these items are likely to be those that did not
// receive a timely ack.
//
// messagesPending mode
// This mode is a variation of the steady state mode. This mode is
// entered when there are outstanding items in the database that need to be sent
// but are not present in the cache. This state can be entered in one
// of two ways:
// 1. The poller read returns as many rows as the cache size
// 2. The Add of a message fails (cache full). This is invoked from the vstream.
// In any of the above cases, the messagesPending flag gets turned on.
// In this phase, the send loop proactively wakes up the poller every time
// it clears the cache.
// The system exits the messagesPending state if the number of items the poller
// loads are less than the cache size and all cache adds are successful.
// If so, the system reverts to the steady state mode.
//
// Rate limiting
// There are two ways for the system to rate-limit:
// 1. Client ingestion rate. If clients ingest messages slowly,
// that makes the senders wait on them to send more messages.
// 2. Postpone rate limiting: A client is considered to be non-busy only
// after it has postponed the message it has sent. This way, if postpones
// are too slow, the clients become less available and essentially
// limit the send rate to how fast messages can be postponed.
// The postpone functions also needs to obtain a semaphore that limits
// the number of tx pool connections they can occupy.
//
// Client load balancing
// The messages are sent to the clients in a round-robin fashion.
// If, for some reason, a client is closed, the load balancer resets
// by starting with the first non-busy client.
//
// The Purge thread
// This thread is mostly independent. It wakes up periodically
// to delete old rows that were successfully acked.
type messageManager struct {
	tsv TabletService
	vs  VStreamer

	name         sqlparser.TableIdent
	fieldResult  *sqltypes.Result
	ackWaitTime  time.Duration
	purgeAfter   time.Duration
	minBackoff   time.Duration
	maxBackoff   time.Duration
	batchSize    int
	pollerTicks  *timer.Timer
	purgeTicks   *timer.Timer
	postponeSema *sync2.Semaphore

	mu     sync.Mutex
	isOpen bool
	// cond waits on curReceiver == -1 || cache.IsEmpty():
	// No current receivers available or cache is empty.
	cond            sync.Cond
	cache           *cache
	receivers       []*receiverWithStatus
	curReceiver     int
	messagesPending bool

	// streamMu keeps the cache and database consistent with each other.
	// Specifically:
	// It prevents items from being removed from cache while the poller
	// reads from the db and adds items to it. Otherwise, the poller
	// might add an older snapshot of a row that was just postponed.
	// It blocks vstream from receiving messages while the poller
	// reads a snapshot and updates lastPollPosition. Any events older than
	// lastPollPosition must be ignored by the vstream. It consequently
	// also blocks vstream from updating the cache while the poller is
	// active.
	streamMu sync.Mutex
	// streamCancel is set when a vstream is running, and is reset
	// to nil after a cancel. This allows for startVStream and stopVstream
	// to be idempotent.
	streamCancel     func()
	lastPollPosition *mysql.Position

	// wg is for ensuring all running goroutines have returned
	// before we can close the manager. You need to Add before
	// launching any gorooutine while holding a lock on mu.
	// The goroutine must in turn defer on Done.
	wg sync.WaitGroup

	vsFilter                  *binlogdatapb.Filter
	readByPriorityAndTimeNext *sqlparser.ParsedQuery
	ackQuery                  *sqlparser.ParsedQuery
	postponeQuery             *sqlparser.ParsedQuery
	purgeQuery                *sqlparser.ParsedQuery
}

// newMessageManager creates a new message manager.
// Calls into tsv have to be made asynchronously. Otherwise,
// it can lead to deadlocks.
func newMessageManager(tsv TabletService, vs VStreamer, table *schema.Table, postponeSema *sync2.Semaphore) *messageManager {
	mm := &messageManager{
		tsv:  tsv,
		vs:   vs,
		name: table.Name,
		fieldResult: &sqltypes.Result{
			Fields: table.MessageInfo.Fields,
		},
		ackWaitTime:     table.MessageInfo.AckWaitDuration,
		purgeAfter:      table.MessageInfo.PurgeAfterDuration,
		minBackoff:      table.MessageInfo.MinBackoff,
		maxBackoff:      table.MessageInfo.MaxBackoff,
		batchSize:       table.MessageInfo.BatchSize,
		cache:           newCache(table.MessageInfo.CacheSize),
		pollerTicks:     timer.NewTimer(table.MessageInfo.PollInterval),
		purgeTicks:      timer.NewTimer(table.MessageInfo.PollInterval),
		postponeSema:    postponeSema,
		messagesPending: true,
	}
	mm.cond.L = &mm.mu

	columnList := buildSelectColumnList(table)
	vsQuery := fmt.Sprintf("select priority, time_next, epoch, time_acked, %s from %v", columnList, mm.name)
	mm.vsFilter = &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  table.Name.String(),
			Filter: vsQuery,
		}},
	}
	mm.readByPriorityAndTimeNext = sqlparser.BuildParsedQuery(
		"select priority, time_next, epoch, time_acked, %s from %v where time_next < %a order by priority, time_next desc limit %a",
		columnList, mm.name, ":time_next", ":max")
	mm.ackQuery = sqlparser.BuildParsedQuery(
		"update %v set time_acked = %a, time_next = null where id in %a and time_acked is null",
		mm.name, ":time_acked", "::ids")
	mm.purgeQuery = sqlparser.BuildParsedQuery(
		"delete from %v where time_acked < %a limit 500", mm.name, ":time_acked")

	mm.postponeQuery = buildPostponeQuery(mm.name, mm.minBackoff, mm.maxBackoff)

	return mm
}

func buildPostponeQuery(name sqlparser.TableIdent, minBackoff, maxBackoff time.Duration) *sqlparser.ParsedQuery {
	var args []interface{}

	// since messages are immediately postponed upon sending, we need to add exponential backoff on top
	// of the ackWaitTime, otherwise messages will be resent too quickly.
	buf := bytes.NewBufferString("update %v set time_next = %a + %a + ")
	args = append(args, name, ":time_now", ":wait_time")

	// have backoff be +/- 33%, whenever this is injected, append (:min_backoff, :jitter)
	jitteredBackoff := "FLOOR((%a<<ifnull(epoch, 0)) * %a)"

	//
	// if the jittered backoff is less than min_backoff, just set it to :min_backoff
	//
	buf.WriteString(fmt.Sprintf("IF(%s < %%a, %%a, ", jitteredBackoff))
	// jitteredBackoff < :min_backoff
	args = append(args, ":min_backoff", ":jitter", ":min_backoff")
	// if it is less, then use :min_backoff
	args = append(args, ":min_backoff")

	// now we are setting the false case on the above IF statement
	if maxBackoff == 0 {
		// if there is no max_backoff, just use jitteredBackoff
		buf.WriteString(jitteredBackoff)
		args = append(args, ":min_backoff", ":jitter")
	} else {
		// make sure that it doesn't exceed max_backoff
		buf.WriteString(fmt.Sprintf("IF(%s > %%a, %%a, %s)", jitteredBackoff, jitteredBackoff))
		// jitteredBackoff > :max_backoff
		args = append(args, ":min_backoff", ":jitter", ":max_backoff")
		// if it is greater, then use :max_backoff
		args = append(args, ":max_backoff")
		// otherwise just use jitteredBackoff
		args = append(args, ":min_backoff", ":jitter")
	}

	// close the if statement
	buf.WriteString(")")

	// now that we've identified time_next, finish the statement
	buf.WriteString(", epoch = ifnull(epoch, 0)+1 where id in %a and time_acked is null")
	args = append(args, "::ids")

	return sqlparser.BuildParsedQuery(buf.String(), args...)
}

// buildSelectColumnList is a convenience function that
// builds a 'select' list for the user-defined columns.
func buildSelectColumnList(t *schema.Table) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	for i, c := range t.MessageInfo.Fields {
		// Column names may have to be escaped.
		if i == 0 {
			buf.Myprintf("%v", sqlparser.NewColIdent(c.Name))
		} else {
			buf.Myprintf(", %v", sqlparser.NewColIdent(c.Name))
		}
	}
	return buf.String()
}

// Open starts the messageManager service.
func (mm *messageManager) Open() {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	if mm.isOpen {
		return
	}
	mm.isOpen = true
	mm.wg.Add(1)
	mm.curReceiver = -1

	go mm.runSend()
	// TODO(sougou): improve ticks to add randomness.
	mm.pollerTicks.Start(mm.runPoller)
	mm.purgeTicks.Start(mm.runPurge)
}

// Close stops the messageManager service.
func (mm *messageManager) Close() {
	mm.pollerTicks.Stop()
	mm.purgeTicks.Stop()

	mm.mu.Lock()
	if !mm.isOpen {
		mm.mu.Unlock()
		return
	}
	mm.isOpen = false
	for _, rcvr := range mm.receivers {
		rcvr.receiver.cancel()
	}
	mm.receivers = nil
	MessageStats.Set([]string{mm.name.String(), "ClientCount"}, 0)
	mm.cache.Clear()
	// This broadcast will cause runSend to exit.
	mm.cond.Broadcast()
	mm.mu.Unlock()

	mm.stopVStream()

	mm.wg.Wait()
}

// Subscribe registers the send function as a receiver of messages
// and returns a 'done' channel that will be closed when the subscription
// ends. There are many reasons for a subscription to end: a grpc context
// cancel or timeout, or tabletserver shutdown, etc.
func (mm *messageManager) Subscribe(ctx context.Context, send func(*sqltypes.Result) error) <-chan struct{} {
	receiver, done := newMessageReceiver(ctx, send)

	mm.mu.Lock()
	defer mm.mu.Unlock()
	if !mm.isOpen {
		receiver.cancel()
		return done
	}

	if err := receiver.Send(mm.fieldResult); err != nil {
		log.Errorf("Terminating connection due to error sending field info: %v", err)
		receiver.cancel()
		return done
	}

	withStatus := &receiverWithStatus{
		receiver: receiver,
	}
	if len(mm.receivers) == 0 {
		mm.startVStream()
	}
	mm.receivers = append(mm.receivers, withStatus)
	MessageStats.Set([]string{mm.name.String(), "ClientCount"}, int64(len(mm.receivers)))
	if mm.curReceiver == -1 {
		mm.rescanReceivers(-1)
	}

	// Track the context and unsubscribe if it gets cancelled.
	go func() {
		<-done
		mm.unsubscribe(receiver)
	}()
	return done
}

func (mm *messageManager) unsubscribe(receiver *messageReceiver) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	for i, rcv := range mm.receivers {
		if rcv.receiver != receiver {
			continue
		}
		// Delete the item at current position.
		n := len(mm.receivers)
		copy(mm.receivers[i:n-1], mm.receivers[i+1:n])
		mm.receivers = mm.receivers[0 : n-1]
		MessageStats.Set([]string{mm.name.String(), "ClientCount"}, int64(len(mm.receivers)))
		break
	}
	// curReceiver is obsolete. Recompute.
	mm.rescanReceivers(-1)
	// If there are no receivers. Shut down the cache.
	if len(mm.receivers) == 0 {
		mm.stopVStream()
		mm.cache.Clear()
	}
}

// rescanReceivers finds the next available receiver
// using start as the starting point. If one was found,
// it sets curReceiver to that index. If curReceiver
// was previously -1, it broadcasts. If none was found,
// curReceiver is set to -1. If there's no starting point,
// it must be specified as -1.
func (mm *messageManager) rescanReceivers(start int) {
	cur := start
	for range mm.receivers {
		cur = (cur + 1) % len(mm.receivers)
		if !mm.receivers[cur].busy {
			if mm.curReceiver == -1 {
				mm.cond.Broadcast()
			}
			mm.curReceiver = cur
			return
		}
	}
	// Nothing was found.
	mm.curReceiver = -1
}

// Add adds the message to the cache. It returns true
// if successful. If the message is already present,
// it still returns true.
func (mm *messageManager) Add(mr *MessageRow) bool {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	if len(mm.receivers) == 0 {
		return false
	}
	// If cache is empty, we have to broadcast that we're not empty
	// any more.
	if mm.cache.IsEmpty() {
		mm.cond.Broadcast()
	}
	if !mm.cache.Add(mr) {
		// Cache is full. Enter "messagesPending" mode.
		mm.messagesPending = true
		return false
	}
	return true
}

func (mm *messageManager) runSend() {
	defer func() {
		mm.tsv.LogError()
		mm.wg.Done()
	}()

	mm.mu.Lock()
	defer mm.mu.Unlock()

	for {
		// It's theoretically possible that this loop can keep going without
		// a wait. If so, the lock will never be released for other functions
		// like Close to take action. So, let's release and acquire the lock
		// to avoid starving other contenders.
		mm.mu.Unlock()
		mm.mu.Lock()

		var rows [][]sqltypes.Value
		for {
			if !mm.isOpen {
				return
			}

			// If cache became empty, there are messages pending, and there are subscribed
			// receivers, we have to trigger the poller to fetch more.
			if mm.cache.IsEmpty() && mm.messagesPending && len(mm.receivers) != 0 {
				// Do this as a separate goroutine. Otherwise, this could cause
				// the following deadlock:
				// 1. runSend obtains a lock
				// 2. Poller gets triggered, and waits for lock.
				// 3. runSend calls this function, but the trigger will hang because
				// this function cannot return until poller returns.
				go mm.pollerTicks.Trigger()
			}

			// If there are no receivers or cache is empty, we wait.
			if mm.curReceiver == -1 || mm.cache.IsEmpty() {
				mm.cond.Wait()
				continue
			}

			// Fetch rows from cache.
			lateCount := int64(0)
			for i := 0; i < mm.batchSize; i++ {
				mr := mm.cache.Pop()
				if mr == nil {
					break
				}
				if mr.Epoch >= 1 {
					lateCount++
				}
				rows = append(rows, mr.Row)
			}
			MessageStats.Add([]string{mm.name.String(), "Delayed"}, lateCount)

			// If we have rows to send, break out of this loop.
			if rows != nil {
				break
			}
		}
		MessageStats.Add([]string{mm.name.String(), "Sent"}, int64(len(rows)))
		// If we're here, there is a current receiver, and messages
		// to send. Reserve the receiver and find the next one.
		receiver := mm.receivers[mm.curReceiver]
		receiver.busy = true
		mm.rescanReceivers(mm.curReceiver)

		// Send the message asynchronously.
		mm.wg.Add(1)
		go mm.send(receiver, &sqltypes.Result{Rows: rows})
	}
}

func (mm *messageManager) send(receiver *receiverWithStatus, qr *sqltypes.Result) {
	defer func() {
		mm.tsv.LogError()
		mm.wg.Done()
	}()

	ids := make([]string, len(qr.Rows))
	for i, row := range qr.Rows {
		ids[i] = row[0].ToString()
	}

	defer func() {
		// Hold streamMu to prevent the ids from being discarded
		// if poller is active. Otherwise, it could have read a
		// snapshot of a row before the postponement and requeue
		// the message.
		mm.streamMu.Lock()
		defer mm.streamMu.Unlock()
		mm.cache.Discard(ids)
	}()

	defer func() {
		mm.mu.Lock()
		defer mm.mu.Unlock()

		receiver.busy = false
		// Rescan if there were no previously available receivers
		// because the current receiver became non-busy.
		if mm.curReceiver == -1 {
			mm.rescanReceivers(-1)
		}
	}()

	if err := receiver.receiver.Send(qr); err != nil {
		// Log the error, but we still want to postpone the message.
		// Otherwise, if this is a chronic failure like "message too
		// big", we'll end up spamming non-stop.
		log.Errorf("Error sending messages: %v: %v", qr, err)
	}
	mm.postpone(mm.tsv, mm.name.String(), mm.ackWaitTime, ids)
}

func (mm *messageManager) postpone(tsv TabletService, name string, ackWaitTime time.Duration, ids []string) {
	// Use the semaphore to limit parallelism.
	if !mm.postponeSema.Acquire() {
		// Unreachable.
		return
	}
	defer mm.postponeSema.Release()
	ctx, cancel := context.WithTimeout(tabletenv.LocalContext(), ackWaitTime)
	defer cancel()
	if _, err := tsv.PostponeMessages(ctx, nil, name, ids); err != nil {
		// This can happen during spikes. Record the incident for monitoring.
		MessageStats.Add([]string{mm.name.String(), "PostponeFailed"}, 1)
	}
}

func (mm *messageManager) startVStream() {
	mm.streamMu.Lock()
	defer mm.streamMu.Unlock()
	if mm.streamCancel != nil {
		return
	}
	var ctx context.Context
	ctx, mm.streamCancel = context.WithCancel(tabletenv.LocalContext())
	go mm.runVStream(ctx)
}

func (mm *messageManager) stopVStream() {
	mm.streamMu.Lock()
	defer mm.streamMu.Unlock()
	if mm.streamCancel != nil {
		mm.streamCancel()
		mm.streamCancel = nil
	}
}

func (mm *messageManager) runVStream(ctx context.Context) {
	for {
		err := mm.runOneVStream(ctx)
		select {
		case <-ctx.Done():
			log.Info("Context canceled, exiting vstream")
			return
		default:
		}
		MessageStats.Add([]string{mm.name.String(), "VStreamFailed"}, 1)
		log.Infof("VStream ended: %v, retrying in 5 seconds", err)
		time.Sleep(5 * time.Second)
	}
}

// runOneVStream watches for any new rows or rows that have been modified.
// Whether it's an insert or an update, if the new value of the
// row indicates that the message is eligible to be sent, it's added to
// the cache.
// Deletes are ignored.
// If the poller updates lastPollPosition, then all GTIDs up to that
// point are deemed obsolete and are skipped.
func (mm *messageManager) runOneVStream(ctx context.Context) error {
	var curPos string
	var fields []*querypb.Field

	err := mm.vs.Stream(ctx, "current", nil, mm.vsFilter, func(events []*binlogdatapb.VEvent) error {
		mm.streamMu.Lock()
		defer mm.streamMu.Unlock()

		select {
		case <-ctx.Done():
			return io.EOF
		default:
		}

		mustSkip := func() (bool, error) {
			if mm.lastPollPosition == nil {
				return false, nil
			}
			if curPos == "" {
				return true, nil
			}
			cur, err := mysql.DecodePosition(curPos)
			if err != nil {
				return false, err
			}
			if cur.AtLeast(*mm.lastPollPosition) {
				mm.lastPollPosition = nil
				return false, nil
			}
			return true, nil
		}
		skipEvents, err := mustSkip()
		if err != nil {
			return err
		}
		var newPos string
		for _, ev := range events {
			switch ev.Type {
			case binlogdatapb.VEventType_FIELD:
				fields = ev.FieldEvent.Fields
			case binlogdatapb.VEventType_ROW:
				if skipEvents {
					continue
				}
				if err := mm.processRowEvent(fields, ev.RowEvent); err != nil {
					return err
				}
			case binlogdatapb.VEventType_GTID:
				newPos = ev.Gtid
			case binlogdatapb.VEventType_COMMIT, binlogdatapb.VEventType_DDL, binlogdatapb.VEventType_OTHER:
				// Update curPos only when the GTID concludes, which is through one
				// of the above events.
				curPos = newPos
				skipEvents, err = mustSkip()
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	return err
}

func (mm *messageManager) processRowEvent(fields []*querypb.Field, rowEvent *binlogdatapb.RowEvent) error {
	if fields == nil {
		// Unreachable.
		return fmt.Errorf("internal error: unexpected rows without fields")
	}

	now := time.Now().UnixNano()
	for _, rc := range rowEvent.RowChanges {
		if rc.After == nil {
			continue
		}
		row := sqltypes.MakeRowTrusted(fields, rc.After)
		mr, err := BuildMessageRow(row)
		if err != nil {
			return err
		}
		if mr.TimeAcked != 0 || mr.TimeNext > now {
			continue
		}
		mm.Add(mr)
	}
	return nil
}

func (mm *messageManager) runPoller() {
	// Fast-path. Skip all the work.
	if mm.receiverCount() == 0 {
		return
	}

	mm.streamMu.Lock()
	defer mm.streamMu.Unlock()

	ctx, cancel := context.WithTimeout(tabletenv.LocalContext(), mm.pollerTicks.Interval())
	defer func() {
		mm.tsv.LogError()
		cancel()
	}()

	size := mm.cache.Size()
	bindVars := map[string]*querypb.BindVariable{
		"time_next": sqltypes.Int64BindVariable(time.Now().UnixNano()),
		"max":       sqltypes.Int64BindVariable(int64(size)),
	}
	qr, err := mm.readPending(ctx, bindVars)
	if err != nil {
		return
	}

	// Obtain mu lock to verify and preserve that len(receivers) != 0.
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.messagesPending = false
	if len(qr.Rows) >= size {
		// There are probably more messages to be sent.
		mm.messagesPending = true
	}
	if len(mm.receivers) == 0 {
		// Almost never reachable because we just checked this.
		return
	}
	if len(qr.Rows) != 0 {
		// We've most likely added items.
		// Wake up the sender.
		defer mm.cond.Broadcast()
	}
	for _, row := range qr.Rows {
		mr, err := BuildMessageRow(row)
		if err != nil {
			mm.tsv.Stats().InternalErrors.Add("Messages", 1)
			log.Errorf("Error reading message row: %v", err)
			continue
		}
		if !mm.cache.Add(mr) {
			mm.messagesPending = true
			return
		}
	}
}

func (mm *messageManager) runPurge() {
	go purge(mm.tsv, mm.name.String(), mm.purgeAfter, mm.purgeTicks.Interval())
}

// purge is a non-member because it should be called asynchronously and should
// not rely on members of messageManager.
func purge(tsv TabletService, name string, purgeAfter, purgeInterval time.Duration) {
	ctx, cancel := context.WithTimeout(tabletenv.LocalContext(), purgeInterval)
	defer func() {
		tsv.LogError()
		cancel()
	}()
	for {
		count, err := tsv.PurgeMessages(ctx, nil, name, time.Now().Add(-purgeAfter).UnixNano())
		if err != nil {
			MessageStats.Add([]string{name, "PurgeFailed"}, 1)
			log.Errorf("Unable to delete messages: %v", err)
		} else {
			MessageStats.Add([]string{name, "Purged"}, count)
		}
		// If deleted 500 or more, we should continue.
		if count < 500 {
			return
		}
	}
}

// GenerateAckQuery returns the query and bind vars for acking a message.
func (mm *messageManager) GenerateAckQuery(ids []string) (string, map[string]*querypb.BindVariable) {
	idbvs := &querypb.BindVariable{
		Type:   querypb.Type_TUPLE,
		Values: make([]*querypb.Value, 0, len(ids)),
	}
	for _, id := range ids {
		idbvs.Values = append(idbvs.Values, &querypb.Value{
			Type:  querypb.Type_VARBINARY,
			Value: []byte(id),
		})
	}
	return mm.ackQuery.Query, map[string]*querypb.BindVariable{
		"time_acked": sqltypes.Int64BindVariable(time.Now().UnixNano()),
		"ids":        idbvs,
	}
}

// GeneratePostponeQuery returns the query and bind vars for postponing a message.
func (mm *messageManager) GeneratePostponeQuery(ids []string) (string, map[string]*querypb.BindVariable) {
	idbvs := &querypb.BindVariable{
		Type:   querypb.Type_TUPLE,
		Values: make([]*querypb.Value, 0, len(ids)),
	}
	for _, id := range ids {
		idbvs.Values = append(idbvs.Values, &querypb.Value{
			Type:  querypb.Type_VARBINARY,
			Value: []byte(id),
		})
	}

	bvs := map[string]*querypb.BindVariable{
		"time_now":    sqltypes.Int64BindVariable(time.Now().UnixNano()),
		"wait_time":   sqltypes.Int64BindVariable(int64(mm.ackWaitTime)),
		"min_backoff": sqltypes.Int64BindVariable(int64(mm.minBackoff)),
		"jitter":      sqltypes.Float64BindVariable(.666666 + rand.Float64()*.666666),
		"ids":         idbvs,
	}

	if mm.maxBackoff > 0 {
		bvs["max_backoff"] = sqltypes.Int64BindVariable(int64(mm.maxBackoff))
	}

	return mm.postponeQuery.Query, bvs
}

// GeneratePurgeQuery returns the query and bind vars for purging messages.
func (mm *messageManager) GeneratePurgeQuery(timeCutoff int64) (string, map[string]*querypb.BindVariable) {
	return mm.purgeQuery.Query, map[string]*querypb.BindVariable{
		"time_acked": sqltypes.Int64BindVariable(timeCutoff),
	}
}

// BuildMessageRow builds a MessageRow for a db row.
func BuildMessageRow(row []sqltypes.Value) (*MessageRow, error) {
	mr := &MessageRow{Row: row[4:]}
	if !row[0].IsNull() {
		v, err := evalengine.ToInt64(row[0])
		if err != nil {
			return nil, err
		}
		mr.Priority = v
	}
	if !row[1].IsNull() {
		v, err := evalengine.ToInt64(row[1])
		if err != nil {
			return nil, err
		}
		mr.TimeNext = v
	}
	if !row[2].IsNull() {
		v, err := evalengine.ToInt64(row[2])
		if err != nil {
			return nil, err
		}
		mr.Epoch = v
	}
	if !row[3].IsNull() {
		v, err := evalengine.ToInt64(row[3])
		if err != nil {
			return nil, err
		}
		mr.TimeAcked = v
	}
	return mr, nil
}

func (mm *messageManager) receiverCount() int {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	return len(mm.receivers)
}

func (mm *messageManager) readPending(ctx context.Context, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	query, err := mm.readByPriorityAndTimeNext.GenerateQuery(bindVars, nil)
	if err != nil {
		mm.tsv.Stats().InternalErrors.Add("Messages", 1)
		log.Errorf("Error reading rows from message table: %v", err)
		return nil, err
	}
	qr := &sqltypes.Result{}
	err = mm.vs.StreamResults(ctx, query, func(response *binlogdatapb.VStreamResultsResponse) error {
		if response.Fields != nil {
			qr.Fields = response.Fields
		}
		if response.Gtid != "" {
			pos, err := mysql.DecodePosition(response.Gtid)
			if err != nil {
				return err
			}
			mm.lastPollPosition = &pos
		}
		for _, row := range response.Rows {
			qr.Rows = append(qr.Rows, sqltypes.MakeRowTrusted(qr.Fields, row))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return qr, err
}
