/*
Copyright 2017 Google Inc.

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
	"io"
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// MessageStats tracks stats for messages.
var MessageStats = stats.NewGaugesWithMultiLabels(
	"Messages",
	"Stats for messages",
	[]string{"TableName", "Metric"})

// MessageDelayTimings records total latency from queueing to sent to clients.
var MessageDelayTimings = stats.NewMultiTimings(
	"MessageDelay",
	"MessageDelayTimings records total latency from queueing to client sends",
	[]string{"TableName"})

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
// also removed from the cache. If a send failed, then the messages are
// just removed from the cache. This triggers the messagesPending state explained
// below.
// The poller wakes up periodically, loads messages that are due and adds them
// to the cache. Most of these items are likely to be those that did not
// receive a timely ack.
//
// messagesPending mode
// This mode is a variation of the steady state mode. This mode is
// entered when there are outstanding items on disk that need to be sent
// but are not present in the cache. This state can be entered in one
// of three ways:
// 1. The poller read returns as many rows as the cache size
// 2. The Add of a message fails (cache full).
// 3. A send failed that caused a message to be discarded without postponement.
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
	DBLock sync.Mutex
	tsv    TabletService

	name         sqlparser.TableIdent
	fieldResult  *sqltypes.Result
	ackWaitTime  time.Duration
	purgeAfter   time.Duration
	batchSize    int
	pollerTicks  *timer.Timer
	purgeTicks   *timer.Timer
	conns        *connpool.Pool
	postponeSema *sync2.Semaphore

	mu     sync.Mutex
	isOpen bool
	// cond gets triggered if a receiver becomes available (curReceiver != -1),
	// an item gets added to the cache, or if the manager is closed.
	// The trigger wakes up the runSend thread.
	cond            sync.Cond
	cache           *cache
	receivers       []*receiverWithStatus
	curReceiver     int
	messagesPending bool

	// wg is for ensuring all running goroutines have returned
	// before we can close the manager. You need to Add before
	// launching any gorooutine while holding a lock on mu.
	// The goroutine must in turn defer on Done.
	wg sync.WaitGroup

	readByTimeNext    *sqlparser.ParsedQuery
	loadMessagesQuery *sqlparser.ParsedQuery
	ackQuery          *sqlparser.ParsedQuery
	postponeQuery     *sqlparser.ParsedQuery
	purgeQuery        *sqlparser.ParsedQuery
}

// newMessageManager creates a new message manager.
// Calls into tsv have to be made asynchronously. Otherwise,
// it can lead to deadlocks.
func newMessageManager(tsv TabletService, table *schema.Table, conns *connpool.Pool, postponeSema *sync2.Semaphore) *messageManager {
	mm := &messageManager{
		tsv:  tsv,
		name: table.Name,
		fieldResult: &sqltypes.Result{
			Fields: table.MessageInfo.Fields,
		},
		ackWaitTime:  table.MessageInfo.AckWaitDuration,
		purgeAfter:   table.MessageInfo.PurgeAfterDuration,
		batchSize:    table.MessageInfo.BatchSize,
		cache:        newCache(table.MessageInfo.CacheSize),
		pollerTicks:  timer.NewTimer(table.MessageInfo.PollInterval),
		purgeTicks:   timer.NewTimer(table.MessageInfo.PollInterval),
		conns:        conns,
		postponeSema: postponeSema,
	}
	mm.cond.L = &mm.mu

	columnList := buildSelectColumnList(table)
	mm.readByTimeNext = sqlparser.BuildParsedQuery(
		"select time_next, epoch, time_created, %s from %v where time_next < %a order by time_next desc limit %a",
		columnList, mm.name, ":time_next", ":max")
	mm.loadMessagesQuery = sqlparser.BuildParsedQuery(
		"select time_next, epoch, time_created, %s from %v where %a",
		columnList, mm.name, ":#pk")
	mm.ackQuery = sqlparser.BuildParsedQuery(
		"update %v set time_acked = %a, time_next = null where id in %a and time_acked is null",
		mm.name, ":time_acked", "::ids")
	mm.postponeQuery = sqlparser.BuildParsedQuery(
		"update %v set time_next = %a+(%a<<epoch), epoch = epoch+1 where id in %a and time_acked is null",
		mm.name, ":time_now", ":wait_time", "::ids")
	mm.purgeQuery = sqlparser.BuildParsedQuery(
		"delete from %v where time_acked < %a and time_acked is not null limit 500",
		mm.name, ":time_acked")
	return mm
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
	mm.cond.Broadcast()
	mm.mu.Unlock()

	mm.wg.Wait()
}

// Subscribe registers the send function as a receiver of messages
// and returns a 'done' channel that will be closed when the subscription
// ends. There are many reaons for a subscription to end: a grpc context
// cancel or timeout, or tabletserver shutdown, etc.
func (mm *messageManager) Subscribe(ctx context.Context, send func(*sqltypes.Result) error) <-chan struct{} {
	receiver, done := newMessageReceiver(ctx, send)
	mm.mu.Lock()
	defer mm.mu.Unlock()
	withStatus := &receiverWithStatus{
		receiver: receiver,
		busy:     true,
	}
	mm.receivers = append(mm.receivers, withStatus)
	MessageStats.Set([]string{mm.name.String(), "ClientCount"}, int64(len(mm.receivers)))

	// Send the message asynchronously.
	mm.wg.Add(1)
	go mm.send(withStatus, mm.fieldResult)

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
	if !mm.cache.Add(mr) {
		// Cache is full. Enter "messagesPending" mode to let the poller
		// fill the cache with messages from disk as soon as a cache
		// slot becomes available.
		// We also skip notifying the send routine via mm.cond.Broadcast()
		// because a full cache means that it's already active.
		mm.messagesPending = true
		return false
	}
	mm.cond.Broadcast()
	return true
}

func (mm *messageManager) runSend() {
	defer func() {
		tabletenv.LogError()
		mm.wg.Done()
	}()
	for {
		var rows [][]sqltypes.Value
		mm.mu.Lock()
		for {
			if !mm.isOpen {
				mm.mu.Unlock()
				return
			}

			// If there are no receivers, we wait.
			if mm.curReceiver == -1 {
				mm.cond.Wait()
				continue
			}

			// Fetch rows from cache.
			lateCount := int64(0)
			timingsKey := []string{mm.name.String()}
			for i := 0; i < mm.batchSize; i++ {
				if mr := mm.cache.Pop(); mr != nil {
					if mr.Epoch >= 1 {
						lateCount++
					}
					MessageDelayTimings.Record(timingsKey, time.Unix(0, mr.TimeCreated))
					rows = append(rows, mr.Row)
					continue
				}
				break
			}
			MessageStats.Add([]string{mm.name.String(), "Delayed"}, lateCount)

			// We have rows to send, break out of this loop.
			if rows != nil {
				break
			}

			if mm.messagesPending {
				// If messages are pending, trigger the poller to fetch more.
				// Do this as a separate goroutine. Otherwise, this could cause
				// the following deadlock:
				// 1. runSend obtains a lock
				// 2. Poller gets trigerred, and waits for lock.
				// 3. runSend calls this function, but the trigger will hang because
				// this function cannot return until poller returns.
				go mm.pollerTicks.Trigger()
			}

			// There are no rows in the cache. We wait.
			mm.cond.Wait()
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
		mm.mu.Unlock()
	}
}

func (mm *messageManager) send(receiver *receiverWithStatus, qr *sqltypes.Result) {
	defer func() {
		tabletenv.LogError()
		mm.wg.Done()
	}()

	ids := make([]string, len(qr.Rows))
	for i, row := range qr.Rows {
		ids[i] = row[0].ToString()
	}

	// This is the cleanup.
	defer func() {
		// Discard messages from cache only at the end. This will
		// prevent them from being requeued while they're being postponed.
		mm.cache.Discard(ids)

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
		if err == io.EOF {
			// If the receiver ended the stream, we do not postpone the message.
			// Instead, we mark messagesPending, which will proactively trigger
			// the poller when cache goes empty, and the message will be immediately
			// resent through another receiver.
			mm.mu.Lock()
			mm.messagesPending = true
			// If this was the last message from the cache, the send loop
			// could have gone idle. If so, wake it up.
			mm.cond.Broadcast()
			mm.mu.Unlock()
			// No need to call cancel. messageReceiver already
			// does that before returning this error.
			mm.unsubscribe(receiver.receiver)
			return
		}

		// A rare corner case:
		// If we fail to send the field info, then we should not send
		// rows on this connection anymore. We should instead terminate
		// the connection.
		if len(ids) == 0 {
			receiver.receiver.cancel()
			mm.unsubscribe(receiver.receiver)
			log.Errorf("Terminating connection due to error sending field info: %v", err)
			return
		}

		// Log the error, but we still want to postpone the message.
		// Otherwise, if this is a chronic failure like "message too
		// big", we'll end up spamming non-stop.
		log.Errorf("Error sending messages: %v: %v", qr, err)
	}
	mm.postpone(mm.tsv, mm.name.String(), mm.ackWaitTime, ids)
}

func (mm *messageManager) postpone(tsv TabletService, name string, ackWaitTime time.Duration, ids []string) {
	// ids can be empty if it's the field info being sent.
	if len(ids) == 0 {
		return
	}
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

func (mm *messageManager) runPoller() {
	ctx, cancel := context.WithTimeout(tabletenv.LocalContext(), mm.pollerTicks.Interval())
	defer func() {
		tabletenv.LogError()
		cancel()
	}()
	conn, err := mm.conns.Get(ctx)
	if err != nil {
		tabletenv.InternalErrors.Add("Messages", 1)
		log.Errorf("Error getting connection: %v", err)
		return
	}
	defer conn.Recycle()

	func() {
		// Fast-path. Skip all the work.
		if mm.receiverCount() == 0 {
			return
		}
		mm.DBLock.Lock()
		defer mm.DBLock.Unlock()

		size := mm.cache.Size()
		bindVars := map[string]*querypb.BindVariable{
			"time_next": sqltypes.Int64BindVariable(time.Now().UnixNano()),
			"max":       sqltypes.Int64BindVariable(int64(size)),
		}
		qr, err := mm.read(ctx, conn, mm.readByTimeNext, bindVars)
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
				tabletenv.InternalErrors.Add("Messages", 1)
				log.Errorf("Error reading message row: %v", err)
				continue
			}
			if !mm.cache.Add(mr) {
				mm.messagesPending = true
				return
			}
		}
	}()
}

func (mm *messageManager) runPurge() {
	go purge(mm.tsv, mm.name.String(), mm.purgeAfter, mm.purgeTicks.Interval())
}

// purge is a non-member because it should be called asynchronously and should
// not rely on members of messageManager.
func purge(tsv TabletService, name string, purgeAfter, purgeInterval time.Duration) {
	ctx, cancel := context.WithTimeout(tabletenv.LocalContext(), purgeInterval)
	defer func() {
		tabletenv.LogError()
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
			Type:  querypb.Type_VARCHAR,
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
			Type:  querypb.Type_VARCHAR,
			Value: []byte(id),
		})
	}
	return mm.postponeQuery.Query, map[string]*querypb.BindVariable{
		"time_now":  sqltypes.Int64BindVariable(time.Now().UnixNano()),
		"wait_time": sqltypes.Int64BindVariable(int64(mm.ackWaitTime)),
		"ids":       idbvs,
	}
}

// GeneratePurgeQuery returns the query and bind vars for purging messages.
func (mm *messageManager) GeneratePurgeQuery(timeCutoff int64) (string, map[string]*querypb.BindVariable) {
	return mm.purgeQuery.Query, map[string]*querypb.BindVariable{
		"time_acked": sqltypes.Int64BindVariable(timeCutoff),
	}
}

// BuildMessageRow builds a MessageRow for a db row.
func BuildMessageRow(row []sqltypes.Value) (*MessageRow, error) {
	timeNext, err := sqltypes.ToInt64(row[0])
	if err != nil {
		return nil, err
	}
	epoch, err := sqltypes.ToInt64(row[1])
	if err != nil {
		return nil, err
	}
	timeCreated, err := sqltypes.ToInt64(row[2])
	if err != nil {
		return nil, err
	}
	return &MessageRow{
		TimeNext:    timeNext,
		Epoch:       epoch,
		TimeCreated: timeCreated,
		Row:         row[3:],
	}, nil
}

func (mm *messageManager) receiverCount() int {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	return len(mm.receivers)
}

func (mm *messageManager) read(ctx context.Context, conn *connpool.DBConn, pq *sqlparser.ParsedQuery, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	b, err := pq.GenerateQuery(bindVars, nil)
	if err != nil {
		tabletenv.InternalErrors.Add("Messages", 1)
		log.Errorf("Error reading rows from message table: %v", err)
		return nil, err
	}
	return conn.Exec(ctx, string(b), mm.cache.Size()+1, false)
}
