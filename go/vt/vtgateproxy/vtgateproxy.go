/*
Copyright 2024 The Vitess Authors.

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

// Package vtgate provides a proxy service that accepts incoming mysql protocol
// connections and proxies to a vtgate using GRPC
package vtgateproxy

import (
	"context"
	"flag"
	"io"
	"net/url"
	"strings"
	"sync"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

var (
	vtgateHostsFile = flag.String("vtgate_hosts_file", "", "json file describing the host list to use for vtgate:// resolution")
	numConnections  = flag.Int("num_connections", 4, "number of outbound GPRC connections to maintain")
	poolTypeField   = flag.String("pool_type_field", "", "Field name used to specify the target vtgate type and filter the hosts")
	affinityField   = flag.String("affinity_field", "", "Attribute (both mysql protocol connection and JSON file) used to specify the routing affinity , e.g. 'az_id'")
	addressField    = flag.String("address_field", "address", "field name in the json file containing the address")
	portField       = flag.String("port_field", "port", "field name in the json file containing the port")

	vtGateProxy *VTGateProxy = &VTGateProxy{
		targetConns: map[string]*vtgateconn.VTGateConn{},
		mu:          sync.RWMutex{},
	}
)

type VTGateProxy struct {
	targetConns map[string]*vtgateconn.VTGateConn
	mu          sync.RWMutex
}

func (proxy *VTGateProxy) getConnection(ctx context.Context, target string) (*vtgateconn.VTGateConn, error) {
	log.V(100).Infof("Getting connection for %v\n", target)

	// If the connection exists, return it
	proxy.mu.RLock()
	existingConn := proxy.targetConns[target]
	proxy.mu.RUnlock()

	if existingConn != nil {
		log.V(100).Infof("Reused connection for %v\n", target)
		return existingConn, nil
	}

	// No luck, need to create a new one. Serialize new additions so we don't create multiple
	// for a given target.
	log.V(100).Infof("Need to create connection for %v\n", target)

	proxy.mu.Lock()
	defer proxy.mu.Unlock()

	// Check again in case conn was made between lock acquisitions.
	existingConn = proxy.targetConns[target]
	if existingConn != nil {
		log.V(100).Infof("Reused connection for %v\n", target)
		return existingConn, nil
	}

	// Otherwise create a new connection. TODO: confirm this doesn't actually make a TCP connection, and returns quickly,
	// otherwise we're going to have to do this while not holding the lock.
	conn, err := vtgateconn.DialProtocol(ctx, "grpc", target)
	if err != nil {
		return nil, err
	}

	log.V(100).Infof("Created new connection for %v\n", target)
	proxy.targetConns[target] = conn

	return conn, nil
}

func (proxy *VTGateProxy) NewSession(ctx context.Context, options *querypb.ExecuteOptions, connectionAttributes map[string]string) (*vtgateconn.VTGateSession, error) {

	targetUrl := url.URL{
		Scheme: "vtgate",
		Host:   "pool",
	}

	values := url.Values{}

	if *poolTypeField != "" {
		poolType, ok := connectionAttributes[*poolTypeField]
		if ok {
			values.Set(*poolTypeField, poolType)
		} else {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "pool type attribute %s not supplied by client", *poolTypeField)
		}
	}

	if *affinityField != "" {
		affinity, ok := connectionAttributes[*affinityField]
		if ok {
			values.Set(*affinityField, affinity)
		}
	}

	targetUrl.RawQuery = values.Encode()

	conn, err := proxy.getConnection(ctx, targetUrl.String())
	if err != nil {
		return nil, err
	}

	return conn.Session("", options), nil
}

// CloseSession closes the session, rolling back any implicit transactions. This has the
// same effect as if a "rollback" statement was executed, but does not affect the query
// statistics.
func (proxy *VTGateProxy) CloseSession(ctx context.Context, session *vtgateconn.VTGateSession) error {
	return session.CloseSession(ctx)
}

// ResolveTransaction resolves the specified 2PC transaction.
func (proxy *VTGateProxy) ResolveTransaction(ctx context.Context, dtid string) error {
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not implemented")
}

// Prepare supports non-streaming prepare statement query with multi shards
func (proxy *VTGateProxy) Prepare(ctx context.Context, session *vtgateconn.VTGateSession, sql string, bindVariables map[string]*querypb.BindVariable) (newsession *vtgateconn.VTGateSession, fld []*querypb.Field, err error) {
	return nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not implemented")
}

func (proxy *VTGateProxy) Execute(ctx context.Context, session *vtgateconn.VTGateSession, sql string, bindVariables map[string]*querypb.BindVariable) (qr *sqltypes.Result, err error) {

	// Intercept "use" statements since they just have to update the local session
	if strings.HasPrefix(sql, "use ") {
		targetString := sqlescape.UnescapeID(sql[4:])
		session.SessionPb().TargetString = targetString
		return &sqltypes.Result{}, nil
	}

	return session.Execute(ctx, sql, bindVariables)
}

func (proxy *VTGateProxy) StreamExecute(ctx context.Context, session *vtgateconn.VTGateSession, sql string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	stream, err := session.StreamExecute(ctx, sql, bindVariables)
	if err != nil {
		return err
	}

	for {
		qr, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		callback(qr)
	}

	return nil
}

func Init() {
	log.V(100).Infof("Registering GRPC dial options")
	grpcclient.RegisterGRPCDialOptions(func(opts []grpc.DialOption) ([]grpc.DialOption, error) {
		return append(opts, grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`)), nil
	})

	RegisterJSONGateResolver(
		*vtgateHostsFile,
		*addressField,
		*portField,
		*poolTypeField,
		*affinityField,
	)
}
