package vstreamclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"google.golang.org/protobuf/proto"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// EventFunc is an optional callback function that can be registered for individual event types
type EventFunc func(ctx context.Context, event *binlogdatapb.VEvent) error

// FlushFunc is called per batch of rows, which could be any number of rows, but will be limited by the
// MaxRowsPerFlush setting. The rows are always a slice of the data type for the table, which is configured
// per table. You can use type assertion to convert the rows to the correct type, and then process them as needed.
//
//	func(ctx context.Context, rows any, meta FlushMeta) error {
//		 typedRows := rows.([]*DataType)
//		 // do something with the rows
//		 return nil
//	}
//
// Returning an error will stop the stream, and the last vgtid will not be stored. The stream will need to be
// restarted from the last successful flushed vgtid.
type FlushFunc func(ctx context.Context, rows []Row, meta FlushMeta) error

// Row is the data structure that will be passed to the FlushFunc. It contains the row event, the row change,
// and the scanned data itself. The data will be the type registered for the table, unless it is a delete event, in
// which case it will be nil.
type Row struct {
	RowEvent  *binlogdatapb.RowEvent
	RowChange *binlogdatapb.RowChange
	Data      any // will be populated as the data type registered for the table, unless it is a delete event
}

// FlushMeta is the metadata that is passed to the FlushFunc. It's not necessary, but might be useful
// for logging, debugging, etc.
type FlushMeta struct {
	Keyspace string
	Table    string

	TableStats   TableStats
	VStreamStats VStreamStats

	LatestVGtid *binlogdatapb.VGtid
}

// Run starts the vstream, processing events from the stream until it ends or an error occurs.
func (v *VStreamClient) Run(ctx context.Context) error {
	// make a cancelable context, so the heartbeat monitor can cancel the stream if necessary
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go v.monitorHeartbeat(ctx, cancel)

	// ********************************************************************************************************
	// Event Processing
	//
	// this is the main loop that processes events from the stream. It will continue until the stream ends or an
	// error occurs. The context is checked before processing each event.
	// ********************************************************************************************************
	for {
		// events come in batches, depending on how busy the keyspace is. This is where the network communication
		// happens, so it's the most likely place for errors to occur.
		events, err := v.reader.Recv()
		switch {
		case err == nil: // no error, continue processing below

		case errors.Is(err, io.EOF):
			fmt.Println("vstreamclient: stream ended")
			return nil

		default:
			return fmt.Errorf("vstreamclient: remote error: %w", err)
		}

		for _, ev := range events {
			// check for context errors before processing the next event, since any processing will likely be wasted
			err = ctx.Err()
			if err != nil {
				return fmt.Errorf("vstreamclient: context error: %w", err)
			}

			// keep track of the last event time for heartbeat monitoring. We're purposefully not using the event
			// timestamp, since that would cause cancellation if the stream was copying, delayed, or lagging.
			v.lastEventReceivedAtUnix.Store(time.Now().Unix())

			// call the user-defined event function, if it exists
			fn, ok := v.eventFuncs[ev.Type]
			if ok {
				err = fn(ctx, ev)
				if err != nil {
					return fmt.Errorf("vstreamclient: user error processing %s event: %w", ev.Type.String(), err)
				}
			}

			// handle individual events based on their type
			switch ev.Type {
			// field events are sent first, and contain schema information for any tables that are being streamed,
			// so we cache the fields for each table as they come in
			case binlogdatapb.VEventType_FIELD:
				var table *TableConfig
				table, ok = v.tables[ev.FieldEvent.TableName]
				if !ok {
					return errors.New("vstreamclient: unexpected table name: " + ev.FieldEvent.TableName)
				}

				err = table.handleFieldEvent(ev.FieldEvent)
				if err != nil {
					return err
				}

			// row events are the actual data changes, and we'll process them based on the table name
			case binlogdatapb.VEventType_ROW:
				var table *TableConfig
				table, ok = v.tables[ev.RowEvent.TableName]
				if !ok {
					return errors.New("vstreamclient: unexpected table name: " + ev.FieldEvent.TableName)
				}

				err = table.handleRowEvent(ev.RowEvent, &v.stats)
				if err != nil {
					return err
				}

			// vgtid events are sent periodically, and we'll store the latest vgtid for checkpointing. We may get
			// this mid-transaction, so we don't flush here, so we don't propagate a partial transaction that may
			// be rolled back.
			case binlogdatapb.VEventType_VGTID:
				v.latestVgtid = ev.Vgtid

			// commit and other events are safe to flush on, since they indicate the end of a transaction.
			// Otherwise, there's not much to do with these events.
			case binlogdatapb.VEventType_COMMIT, binlogdatapb.VEventType_OTHER:
				// only flush when we have an event that guarantees we're not flushing mid-transaction
				err = v.flush(ctx)
				if err != nil {
					return err
				}

			// DDL events are schema changes, and we might want to handle them differently than data events.
			// They are safe to flush on, since they indicate the end of a transaction. If you want to
			// transparently adjust the destination schema based on DDL events, you would do that here.
			case binlogdatapb.VEventType_DDL:
				err = v.flush(ctx)
				if err != nil {
					return err
				}

			case binlogdatapb.VEventType_COPY_COMPLETED:
				// TODO: don't flush until the copy is completed? do some sort of cleanup if we haven't received this?

				err = setCopyCompleted(ctx, v.session, v.name, v.vgtidStateKeyspace, v.vgtidStateTable)
				if err != nil {
					return err
				}

			// heartbeat events are sent periodically, if the source keyspace is idle and there are no other events.
			// It's possible that there is still buffered data that hadn't exceeded the min duration or max rows
			// thresholds the last time flush was called. Most of the time, that won't be the case, but flush will
			// check for that and only run if necessary.
			case binlogdatapb.VEventType_HEARTBEAT:
				err = v.flush(ctx)
				if err != nil {
					return err
				}

			// even if there are no changes to the tables being streamed, we'll still get a begin, vgtid, and commit
			// event for each transaction. The other two are used for checkpoints, but nothing to do here.
			case binlogdatapb.VEventType_BEGIN:

			// journal events are sent on resharding. Unless you are manually targeting shards, vstream should
			// transparently handle resharding for you, so you shouldn't need to do anything with these events.
			// You might want to log them for debugging purposes, or to alert on resharding events in case
			// something goes wrong. After resharding, if the pre-reshard vgtid is no longer valid, you may need
			// to restart the stream from the beginning.
			case binlogdatapb.VEventType_JOURNAL:

			// there aren't strong cases for handling these events, but you might want to log them for debugging
			case binlogdatapb.VEventType_VERSION, binlogdatapb.VEventType_LASTPK, binlogdatapb.VEventType_SAVEPOINT:
			}
		}
	}
}

// ********************************************************************************************************
// Heartbeat Monitoring
//
// the heartbeat ticker will be used to ensure that we haven't been disconnected from the stream. This starts
// a goroutine that will cancel the context if we haven't received an event in twice the heartbeat duration.
// ********************************************************************************************************
func (v *VStreamClient) monitorHeartbeat(ctx context.Context, cancel context.CancelFunc) {
	heartbeatDur := time.Duration(v.flags.HeartbeatInterval) * time.Second
	heartbeat := time.NewTicker(heartbeatDur)
	defer heartbeat.Stop()

	for {
		select {
		case tm := <-heartbeat.C:
			// if we haven't received an event yet, we'll skip the heartbeat check
			if v.lastEventReceivedAtUnix.Load() == 0 {
				continue
			}

			// if we haven't received an event in twice the heartbeat duration, we'll cancel the context, since
			// we're likely disconnected, and exit the goroutine
			if tm.Sub(time.Unix(v.lastEventReceivedAtUnix.Load(), 0)) > heartbeatDur*2 {
				cancel()
				return
			}

		case <-ctx.Done():
			// this cancel is probably unnecessary, since the context is already done, but it's good practice
			cancel()
			return
		}
	}
}

// ********************************************************************************************************
// Flush Data + Store Vgtid
//
// as we process events, we'll periodically need to check point the last vgtid and store the customers in the
// database. You can control the frequency of this flush by adjusting the minFlushDuration and maxRowsPerFlush.
// This is only called when we have an event that guarantees we're not flushing mid-transaction.
// ********************************************************************************************************
//
// we might consider exporting Flush, but we'd need to have a mutex or something to block the stream from
// processing, and technically we'd need to let it run until a commit event happens.
func (v *VStreamClient) flush(ctx context.Context) error {
	// if the lastFlushedVgtid is the same as the latestVgtid, we don't need to do anything
	if proto.Equal(v.lastFlushedVgtid, v.latestVgtid) {
		return nil
	}

	// if we have exceeded the minFlushDuration, we'll force a flush, regardless how many rows each table has
	shouldFlush := time.Since(v.stats.LastFlushedAt) > v.minFlushDuration

	// if we haven't exceeded the min flush duration, we'll check if any of the tables have exceeded their
	// max rows to flush. If any of them have, we will force a flush. Every table needs to be flushed at
	// the same time, since the last vgtid covers all tables.
	if !shouldFlush {
		for _, table := range v.tables {
			if len(table.currentBatch) >= table.MaxRowsPerFlush {
				shouldFlush = true
				break
			}
		}
	}

	if !shouldFlush {
		return nil
	}

	// TODO: maybe start a transaction here, and pass it to the flush function

	for _, table := range v.tables {
		// flush the rows to the database, chunked using the max batch size
		for chunk := range slices.Chunk(table.currentBatch, table.MaxRowsPerFlush) {
			err := table.FlushFn(ctx, chunk, FlushMeta{
				Keyspace: table.Keyspace,
				Table:    table.Table,

				TableStats:   table.stats,
				VStreamStats: v.stats,

				LatestVGtid: v.latestVgtid,
			})
			if err != nil {
				return fmt.Errorf("vstreamclient: error flushing table %s: %w", table.Table, err)
			}

			// update the stats for the table and the vstream
			table.stats.FlushCount++
			table.stats.FlushedRowCount += len(chunk)
			table.stats.LastFlushedAt = time.Now()

			v.stats.TableFlushCount++
			v.stats.FlushedRowCount += len(chunk)
		}

		table.resetBatch()
	}

	// always store the latest vgtid, even if there are no customers to store
	err := updateLatestVGtid(ctx, v.session, v.name, v.vgtidStateKeyspace, v.vgtidStateTable, v.latestVgtid)
	if err != nil {
		return err
	}

	v.stats.FlushCount++
	v.stats.LastFlushedAt = time.Now()
	v.lastFlushedVgtid = v.latestVgtid

	return nil
}
