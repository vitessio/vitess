package vstreamer

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

type uvstreamer struct {
	ctx    context.Context
	cancel func()

	vse       *Engine
	vs        *vstreamer
	send      func([]*binlogdatapb.VEvent) error
	tablePKs  []*TableLastPK
	copyState map[string]*sqltypes.Result

	cp       dbconfigs.Connector
	se       *schema.Engine
	startPos string
	filter   *binlogdatapb.Filter
	vschema  *localVSchema

	fields              []*querypb.Field
	pkfields            []*querypb.Field
	secondsBehindMaster int64 //TODO
}

func newUVStreamer(ctx context.Context, vse *Engine, cp dbconfigs.Connector, se *schema.Engine, sh schema.Historian,startPos string, tablePKs []*TableLastPK, filter *binlogdatapb.Filter, vschema *localVSchema, send func([]*binlogdatapb.VEvent) error) *uvstreamer {
	ctx, cancel := context.WithCancel(ctx)
	vs := newVStreamer(ctx, cp, se, sh, startPos, filter, vschema, send)
	uvstreamer := &uvstreamer{
		ctx:      ctx,
		cancel:   cancel,
		vse:      vse,
		vs:       vs,
		send:     send,
		tablePKs: tablePKs,
		cp:       cp,
		se:       se,
		startPos: startPos,
		filter:   filter,
		vschema:  vschema,
	}
	//init copy state
	return uvstreamer
}

func (uvs *uvstreamer) Cancel() {
	uvs.vs.cancel()
	uvs.cancel()
}

func (uvs *uvstreamer) init() error {
	if err := uvs.vs.Init(); err != nil {
		return err
	}
	//startpos validation for tablepk != nil
	if uvs.vs.pos.IsZero() && (uvs.tablePKs == nil || len(uvs.tablePKs) == 0) {
		return fmt.Errorf("Stream needs atleast a position or a table to copy")
	}
	return nil
}

// Stream streams binlog events.
func (uvs *uvstreamer) Stream() error {
	if err := uvs.init(); err != nil {
		return err
	}
	if uvs.tablePKs != nil {
		log.Info("TablePKs is not nil: starting vs.copy()")
		if err := uvs.copy(uvs.ctx); err != nil {
			return err
		}
	}
	return uvs.vs.Stream()
}
