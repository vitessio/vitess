package vstreamclient

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

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
//
// TODO: currently the tx is always nil, because we don't have a way to pass it to the flush function
type FlushFunc func(ctx context.Context, tx *sql.Tx, rows any, meta FlushMeta) error

// FlushMeta is the metadata that is passed to the FlushFunc. It's not necessary, but might be useful
// for logging, debugging, etc.
type FlushMeta struct {
	Keyspace string
	Table    string

	TableFlushCount    int
	TableFlushedRows   int
	TableLastFlushedAt time.Time

	LastVGtid            *binlogdatapb.VGtid
	VStreamFlushCount    int
	VStreamFlushedRows   int
	VStreamLastFlushedAt time.Time
}

type EventFunc func(ctx context.Context, event *binlogdatapb.VEvent) error

// Run starts the vstreamer, processing events from the stream until it ends or an error occurs.
func (v *VStreamer) Run(ctx context.Context) error {
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
			fmt.Println("stream ended")
			return nil

		default:
			return fmt.Errorf("remote error: %w", err)
		}

		for _, ev := range events {
			// check for context errors before processing the next event, since any processing will likely be wasted
			err = ctx.Err()
			if err != nil {
				return fmt.Errorf("context error: %w", err)
			}

			// keep track of the last event time for heartbeat monitoring. We're purposefully not using the event
			// timestamp, since that would cause cancellation if the stream was copying, delayed, or lagging.
			v.lastEventReceivedAtUnix.Store(time.Now().Unix())

			// call the user-defined event function, if it exists
			fn, ok := v.eventFuncs[ev.Type]
			if ok {
				err = fn(ctx, ev)
				if err != nil {
					return fmt.Errorf("vstreamer: user error processing %s event: %w", ev.Type.String(), err)
				}
			}

			// handle individual events based on their type
			switch ev.Type {
			// field events are sent first, and contain schema information for any tables that are being streamed,
			// so we cache the fields for each table as they come in
			case binlogdatapb.VEventType_FIELD:
				table, ok := v.tables[ev.FieldEvent.TableName]
				if !ok {
					return errors.New("vstreamer: unexpected table name: " + ev.FieldEvent.TableName)
				}

				err = table.handleFieldEvent(ev.FieldEvent)
				if err != nil {
					return err
				}

			// row events are the actual data changes, and we'll process them based on the table name
			case binlogdatapb.VEventType_ROW:
				table, ok := v.tables[ev.RowEvent.TableName]
				if !ok {
					return errors.New("vstreamer: unexpected table name: " + ev.FieldEvent.TableName)
				}

				err = table.handleRowEvent(ev.RowEvent)
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

				// alter the destination schema based on the DDL event

			case binlogdatapb.VEventType_COPY_COMPLETED:
				// TODO: don't flush until the copy is completed? do some sort of cleanup if we haven't received this?

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
// We purposefully overwrite the ctx variable to ensure that we don't accidentally use the original context.
// ********************************************************************************************************
func (v *VStreamer) monitorHeartbeat(ctx context.Context, cancel context.CancelFunc) {
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
// database. You can control the frequency of this flush by adjusting the minFlushDuration and maxCustomersToFlush.
// This is only called when we have an event that guarantees we're not flushing mid-transaction.
// ********************************************************************************************************
//
// we might consider exporting Flush, but we'd need to have a mutex or something to block the stream from
// processing, and technically we'd need to let it run until a commit event happens.
func (v *VStreamer) flush(ctx context.Context) error {
	// if the lastFlushedVgtid is the same as the latestVgtid, we don't need to do anything
	if proto.Equal(v.lastFlushedVgtid, v.latestVgtid) {
		return nil
	}

	// if we have exceeded the minFlushDuration, we'll force a flush, regardless how many rows each table has
	shouldFlush := time.Since(v.lastFlushedAt) > v.minFlushDuration

	// if we haven't exceeded the min flush duration, we'll check if any of the tables have exceeded their
	// max rows to flush. If any of them have, we will force a flush. Every table needs to be flushed at
	// the same time, since the last vgtid covers all tables.
	if !shouldFlush {
		for _, table := range v.tables {
			if table.currentBatch.Len() >= table.MaxRowsPerFlush {
				shouldFlush = true
				break
			}
		}
	}

	if !shouldFlush {
		return nil
	}

	// TODO: maybe start a transaction here, and pass it to the flush function

	// even if we haven't exceeded the min flush duration, we'll flush if we have exceeded the max rows per flush
	// for any of the tables
	for _, table := range v.tables {
		// flush the rows to the database, chunked using the max batch size
		for chunk := range reflectChunk(table.currentBatch, table.MaxRowsPerFlush) {
			err := table.FlushFn(ctx, nil, chunk.Interface(), FlushMeta{
				Keyspace: table.Keyspace,
				Table:    table.Table,

				TableFlushCount:    table.flushCount,
				TableFlushedRows:   table.flushedRowCount,
				TableLastFlushedAt: table.lastFlushedAt,

				LastVGtid:            v.latestVgtid,
				VStreamLastFlushedAt: v.lastFlushedAt,
				VStreamFlushCount:    v.flushCount,
				VStreamFlushedRows:   v.flushedRowCount,
			})
			if err != nil {
				return fmt.Errorf("vstreamer: error flushing table %s: %w", table.Table, err)
			}

			// update the stats for the table and the vstream
			table.flushCount++
			table.flushedRowCount += chunk.Len()
			table.lastFlushedAt = time.Now()

			v.tableFlushCount++
			v.flushedRowCount += chunk.Len()
		}

		table.resetBatch()
	}

	// always store the latest vgtid, even if there are no customers to store
	// err := storeLastVGtid(ctx, v.session, v.vgtidStateKeyspace, v.vgtidStateTable, v.latestVgtid)
	// if err != nil {
	// 	return err
	// }

	v.flushCount++
	v.lastFlushedVgtid = v.latestVgtid
	v.lastFlushedAt = time.Now()

	return nil
}

func (table *TableConfig) resetBatch() {
	if table.ReuseBatchSlice && table.currentBatch.IsValid() {
		table.currentBatch = table.currentBatch.Slice(0, 0)
	} else {
		table.currentBatch = reflect.MakeSlice(reflect.SliceOf(reflect.PointerTo(table.underlyingType)), 0, table.MaxRowsPerFlush)
	}
}

func (table *TableConfig) handleFieldEvent(ev *binlogdatapb.FieldEvent) error {
	fieldMap := make(map[string]fieldMapping, len(ev.Fields))

	for i := 0; i < table.underlyingType.NumField(); i++ {
		structField := table.underlyingType.Field(i)
		if !structField.IsExported() {
			continue
		}

		// get the field name from the vstream, db, json tag, or the field name, in that order
		mappedFieldName := structField.Tag.Get("vstream")
		if mappedFieldName == "-" {
			continue
		}
		if mappedFieldName == "" {
			mappedFieldName = structField.Tag.Get("db")
		}
		if mappedFieldName == "" {
			mappedFieldName = structField.Tag.Get("json")
		}
		if mappedFieldName == "" {
			mappedFieldName = structField.Name
		}

		var found bool
		for j, tableField := range ev.Fields {
			if tableField.Name != mappedFieldName {
				continue
			}

			found = true
			fieldMap[mappedFieldName] = fieldMapping{
				rowIndex:    j,
				structIndex: structField.Index,
				kind:        structField.Type.Kind(),
				isPointer:   structField.Type.Kind() == reflect.Ptr,
			}
		}
		if !found && table.ErrorOnUnknownFields {
			return fmt.Errorf("vstreamer: field %s not found in provided data type", mappedFieldName)
		}
	}

	// sanity check that we found at least one field
	if len(fieldMap) == 0 {
		return fmt.Errorf("vstreamer: no matching fields found for table %s", table.Table)
	}

	table.shards[ev.Shard] = shardConfig{
		fieldMap: fieldMap,
		fields:   ev.Fields,
	}

	return nil
}

func (table *TableConfig) handleRowEvent(ev *binlogdatapb.RowEvent) error {
	shard, ok := table.shards[ev.Shard]
	if !ok {
		return fmt.Errorf("unexpected shard: %s", ev.Shard)
	}

	// grow the batch to the expected size
	// table.currentBatch.Grow(len(ev.RowChanges))

	for _, rc := range ev.RowChanges {
		// for deletes, we'll leave the row null
		//  TODO: this is wrong, it needs a PK
		if rc.After == nil {
			continue
		}

		row := sqltypes.MakeRowTrusted(shard.fields, rc.After)

		// create a new struct for the row
		v := reflect.New(table.underlyingType)
		table.currentBatch = reflect.Append(table.currentBatch, v)

		err := copyRowToStruct(shard, row, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// copyRowToStruct builds a customer from a row event
func copyRowToStruct(shard shardConfig, row []sqltypes.Value, vPtr reflect.Value) error {
	for fieldName, m := range shard.fieldMap {
		structField := reflect.Indirect(vPtr).FieldByIndex(m.structIndex)

		switch m.kind {
		case reflect.Bool:
			rowVal, err := row[m.rowIndex].ToBool()
			if err != nil {
				return fmt.Errorf("error converting row value to bool for field %s: %w", fieldName, err)
			}
			structField.SetBool(rowVal)

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			rowVal, err := row[m.rowIndex].ToInt64()
			if err != nil {
				return fmt.Errorf("error converting row value to int64 for field %s: %w", fieldName, err)
			}
			structField.SetInt(rowVal)

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			rowVal, err := row[m.rowIndex].ToUint64()
			if err != nil {
				return fmt.Errorf("error converting row value to uint64 for field %s: %w", fieldName, err)
			}
			structField.SetUint(rowVal)

		case reflect.Float32, reflect.Float64:
			rowVal, err := row[m.rowIndex].ToFloat64()
			if err != nil {
				return fmt.Errorf("error converting row value to float64 for field %s: %w", fieldName, err)
			}
			structField.SetFloat(rowVal)

		case reflect.String:
			rowVal := row[m.rowIndex].ToString()
			structField.SetString(rowVal)

		case reflect.Struct:
			switch m.structType.(type) {
			case time.Time, *time.Time:
				rowVal, err := row[m.rowIndex].ToTime()
				if err != nil {
					return fmt.Errorf("error converting row value to time.Time for field %s: %w", fieldName, err)
				}
				structField.Set(reflect.ValueOf(rowVal))
			}

		case reflect.Pointer,
			reflect.Slice,
			reflect.Array,
			reflect.Invalid,
			reflect.Uintptr,
			reflect.Complex64,
			reflect.Complex128,
			reflect.Chan,
			reflect.Func,
			reflect.Interface,
			reflect.Map,
			reflect.UnsafePointer:
			return fmt.Errorf("vstreamer: unsupported field type: %s", m.kind.String())
		}
	}

	return nil
}
