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

package vtgateconn

import (
	"flag"
	"fmt"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

var (
	// VtgateProtocol defines the RPC implementation used for connecting to vtgate.
	VtgateProtocol = flag.String("vtgate_protocol", "grpc", "how to talk to vtgate")
)

// VTGateConn is the client API object to talk to vtgate.
// It can support concurrent sessions.
// It is constructed using the Dial method.
type VTGateConn struct {
	impl Impl
}

// Session returns a VTGateSession that can be used to access V3 functions.
func (conn *VTGateConn) Session(targetString string, options *querypb.ExecuteOptions) *VTGateSession {
	return &VTGateSession{
		session: &vtgatepb.Session{
			TargetString: targetString,
			Options:      options,
			Autocommit:   true,
		},
		impl: conn.impl,
	}
}

// ResolveTransaction resolves the 2pc transaction.
func (conn *VTGateConn) ResolveTransaction(ctx context.Context, dtid string) error {
	return conn.impl.ResolveTransaction(ctx, dtid)
}

// Close must be called for releasing resources.
func (conn *VTGateConn) Close() {
	conn.impl.Close()
	conn.impl = nil
}

// VStreamReader is returned by VStream.
type VStreamReader interface {
	// Recv returns the next result on the stream.
	// It will return io.EOF if the stream ended.
	Recv() ([]*binlogdatapb.VEvent, error)
}

// VStream streams binlog events.
func (conn *VTGateConn) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid,
	filter *binlogdatapb.Filter, flags *vtgatepb.VStreamFlags) (VStreamReader, error) {
	return conn.impl.VStream(ctx, tabletType, vgtid, filter, flags)
}

// VTGateSession exposes the V3 API to the clients.
// The object maintains client-side state and is comparable to a native MySQL connection.
// For example, if you enable autocommit on a Session object, all subsequent calls will respect this.
// Functions within an object must not be called concurrently.
// You can create as many objects as you want.
// All of them will share the underlying connection to vtgate ("VTGateConn" object).
type VTGateSession struct {
	session *vtgatepb.Session
	impl    Impl
}

// Execute performs a VTGate Execute.
func (sn *VTGateSession) Execute(ctx context.Context, query string, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	session, res, err := sn.impl.Execute(ctx, sn.session, query, bindVars)
	sn.session = session
	return res, err
}

// ExecuteBatch executes a list of queries on vtgate within the current transaction.
func (sn *VTGateSession) ExecuteBatch(ctx context.Context, query []string, bindVars []map[string]*querypb.BindVariable) ([]sqltypes.QueryResponse, error) {
	session, res, errs := sn.impl.ExecuteBatch(ctx, sn.session, query, bindVars)
	sn.session = session
	return res, errs
}

// StreamExecute executes a streaming query on vtgate.
// It returns a ResultStream and an error. First check the
// error. Then you can pull values from the ResultStream until io.EOF,
// or another error.
func (sn *VTGateSession) StreamExecute(ctx context.Context, query string, bindVars map[string]*querypb.BindVariable) (sqltypes.ResultStream, error) {
	// StreamExecute is only used for SELECT queries that don't change
	// the session. So, the protocol doesn't return an updated session.
	// This may change in the future.
	return sn.impl.StreamExecute(ctx, sn.session, query, bindVars)
}

//
// The rest of this file is for the protocol implementations.
//

// Impl defines the interface for a vtgate client protocol
// implementation. It can be used concurrently across goroutines.
type Impl interface {
	// Execute executes a non-streaming query on vtgate. This is a V3 function.
	Execute(ctx context.Context, session *vtgatepb.Session, query string, bindVars map[string]*querypb.BindVariable) (*vtgatepb.Session, *sqltypes.Result, error)

	// ExecuteBatch executes a non-streaming queries on vtgate. This is a V3 function.
	ExecuteBatch(ctx context.Context, session *vtgatepb.Session, queryList []string, bindVarsList []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error)

	// StreamExecute executes a streaming query on vtgate. This is a V3 function.
	StreamExecute(ctx context.Context, session *vtgatepb.Session, query string, bindVars map[string]*querypb.BindVariable) (sqltypes.ResultStream, error)

	// ResolveTransaction resolves the specified 2pc transaction.
	ResolveTransaction(ctx context.Context, dtid string) error

	// VStream streams binlogevents
	VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter, flags *vtgatepb.VStreamFlags) (VStreamReader, error)

	// Close must be called for releasing resources.
	Close()
}

// DialerFunc represents a function that will return an Impl
// object that can communicate with a VTGate.
type DialerFunc func(ctx context.Context, address string) (Impl, error)

var dialers = make(map[string]DialerFunc)

// RegisterDialer is meant to be used by Dialer implementations
// to self register.
func RegisterDialer(name string, dialer DialerFunc) {
	if _, ok := dialers[name]; ok {
		log.Warningf("Dialer %s already exists, overwriting it", name)
	}
	dialers[name] = dialer
}

// DialProtocol dials a specific protocol, and returns the *VTGateConn
func DialProtocol(ctx context.Context, protocol string, address string) (*VTGateConn, error) {
	dialer, ok := dialers[protocol]
	if !ok {
		return nil, fmt.Errorf("no dialer registered for VTGate protocol %s", protocol)
	}
	impl, err := dialer(ctx, address)
	if err != nil {
		return nil, err
	}
	return &VTGateConn{
		impl: impl,
	}, nil
}

// Dial dials using the command-line specified protocol, and returns
// the *VTGateConn.
func Dial(ctx context.Context, address string) (*VTGateConn, error) {
	return DialProtocol(ctx, *VtgateProtocol, address)
}
