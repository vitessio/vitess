package mysql

import (
	"google.golang.org/protobuf/encoding/protowire"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type queryResultBuilder struct {
	b    []byte
	last int
}

func newQueryResultBuilder(sizehint int) queryResultBuilder {
	if sizehint <= 0 {
		sizehint = 1024
	}
	b := make([]byte, 0, sizehint)
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = append(b, 0, 0, 0, 0)
	return queryResultBuilder{b: b, last: len(b)}
}

func (build *queryResultBuilder) Finish() []byte {
	b := build.b
	v := uint64(len(b) - 5)

	switch {
	case v < 1<<28:
		b[1] = byte((v>>0)&0x7f | 0x80)
		b[2] = byte((v>>7)&0x7f | 0x80)
		b[3] = byte((v>>14)&0x7f | 0x80)
		b[4] = byte(v >> 21)
	default:
		panic("QueryResult protobuf is too large for the wire")
	}
	return b
}

func (b *queryResultBuilder) DiscardLastField() {
	b.b = b.b[:b.last]
}

func (b *queryResultBuilder) RowsAffected(rows uint64) {
	b.last = len(b.b)
	b.b = protowire.AppendTag(b.b, 2, protowire.VarintType)
	b.b = protowire.AppendVarint(b.b, rows)
}

func (b *queryResultBuilder) InsertId(insertid uint64) {
	b.last = len(b.b)
	b.b = protowire.AppendTag(b.b, 3, protowire.VarintType)
	b.b = protowire.AppendVarint(b.b, insertid)
}

func (b *queryResultBuilder) Info(info string) {
	b.last = len(b.b)
	b.b = protowire.AppendTag(b.b, 6, protowire.BytesType)
	b.b = protowire.AppendBytes(b.b, hack.StringBytes(info))
}

func (b *queryResultBuilder) SessionStateChanges(ssc string) {
	b.last = len(b.b)
	b.b = protowire.AppendTag(b.b, 7, protowire.BytesType)
	b.b = protowire.AppendBytes(b.b, hack.StringBytes(ssc))
}

func (b *queryResultBuilder) Packet(length int) []byte {
	b.last = len(b.b)
	b.b = protowire.AppendTag(b.b, 8, protowire.BytesType)
	b.b = protowire.AppendVarint(b.b, uint64(length))

	if length == 0 {
		return nil
	}

	if cap(b.b)-len(b.b) < length {
		b.b = growSlice(b.b, length)
	}

	pkt := b.b[len(b.b) : len(b.b)+length]
	b.b = b.b[:len(b.b)+length]
	return pkt
}

func growSlice(b []byte, n int) []byte {
	l := len(b)
	c := l + n // ensure enough space for n elements
	if c < 2*cap(b) {
		// The growth rate has historically always been 2x. In the future,
		// we could rely purely on append to determine the growth rate.
		c = 2 * cap(b)
	}
	return append(b, make([]byte, c)...)[:l]
}

func ParseResult(qr *querypb.QueryResult, wantfields bool) (*sqltypes.Result, error) {
	if qr.RawPackets == nil {
		return sqltypes.Proto3ToResult(qr), nil
	}

	var colcount int
	for i, p := range qr.RawPackets {
		if len(p) == 0 {
			colcount = i
			break
		}
	}

	var err error
	fieldArray := make([]querypb.Field, colcount)
	fieldPackets := qr.RawPackets[:colcount]
	rowPackets := qr.RawPackets[colcount+1:]

	result := &sqltypes.Result{
		RowsAffected:        qr.RowsAffected,
		InsertID:            qr.InsertId,
		SessionStateChanges: qr.SessionStateChanges,
		Info:                qr.Info,
		Fields:              make([]*querypb.Field, len(fieldPackets)),
		Rows:                make([]sqltypes.Row, 0, len(rowPackets)),
	}

	for i, fieldpkt := range fieldPackets {
		result.Fields[i] = &fieldArray[i]
		if wantfields {
			err = parseColumnDefinition(fieldpkt, result.Fields[i], i)
		} else {
			result.Fields[i].Type, err = parseColumnDefinitionType(fieldpkt, i)
		}
		if err != nil {
			return nil, err
		}
	}

	for _, rowpkt := range rowPackets {
		r, err := parseRow(rowpkt, result.Fields, readLenEncStringAsBytes, nil)
		if err != nil {
			return nil, err
		}
		result.Rows = append(result.Rows, r)
	}

	return result, nil
}
