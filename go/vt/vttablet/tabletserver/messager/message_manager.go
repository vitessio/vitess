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

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/connpool"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// MessageStats tracks stats for messages.
var MessageStats = stats.NewMultiCounters("Messages", []string{"TableName", "Metric"})

// MessageDelayTimings records total latency from queueing to sent to clients.
var MessageDelayTimings = stats.NewMultiTimings("MessageDelay", []string{"TableName"})

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
type messageManager struct {
	DBLock sync.Mutex
	tsv    TabletService

	isOpen bool

	name         sqlparser.TableIdent
	fieldResult  *sqltypes.Result
	ackWaitTime  time.Duration
	purgeAfter   time.Duration
	batchSize    int
	pollerTicks  *timer.Timer
	purgeTicks   *timer.Timer
	conns        *connpool.Pool
	postponeSema *sync2.Semaphore

	mu sync.Mutex
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
		"delete from %v where time_scheduled < %a and time_acked is not null limit 500",
		mm.name, ":time_scheduled")
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
			if mm.curReceiver == -1 {
				mm.cond.Wait()
				continue
			}
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
			if rows != nil {
				break
			}
			if mm.messagesPending {
				// Trigger the poller to fetch more.
				mm.pollerTicks.Trigger()
			}
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
			// If the receiver ended the stream, we want the messages
			// to be resent ASAP without postponement. Setting messagesPending
			// will trigger the poller as soon as the cache is clear.
			mm.mu.Lock()
			mm.messagesPending = true
			mm.mu.Unlock()
			// No need to call Cancel. messageReceiver already
			// does that before returning this error.
			mm.unsubscribe(receiver.receiver)
		} else {
			log.Errorf("Error sending messages: %v", qr)
		}
		return
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
			tabletenv.InternalErrors.Add("Messages", 1)
			log.Errorf("Unable to delete messages: %v", err)
		}
		MessageStats.Add([]string{name, "Purged"}, count)
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
		"time_scheduled": sqltypes.Int64BindVariable(timeCutoff),
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
