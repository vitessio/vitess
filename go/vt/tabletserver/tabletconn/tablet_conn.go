// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletconn

import (
	"flag"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

const (
	// ConnClosed is returned when the underlying connection was closed.
	ConnClosed = OperationalError("vttablet: Connection Closed")
)

var (
	// TabletProtocol is exported for unit tests
	TabletProtocol = flag.String("tablet_protocol", "grpc", "how to talk to the vttablets")
)

// ServerError represents an error that was returned from
// a vttablet server. it implements vterrors.VtError.
type ServerError struct {
	Err string
	// ServerCode is the error code that we got from the server.
	ServerCode vtrpcpb.ErrorCode
}

func (e *ServerError) Error() string { return e.Err }

// VtErrorCode returns the underlying Vitess error code.
// This makes ServerError implement vterrors.VtError.
func (e *ServerError) VtErrorCode() vtrpcpb.ErrorCode { return e.ServerCode }

// OperationalError represents an error due to a failure to
// communicate with vttablet.
type OperationalError string

func (e OperationalError) Error() string { return string(e) }

// In all the following calls, context is an opaque structure that may
// carry data related to the call. For instance, if an incoming RPC
// call is responsible for these outgoing calls, and the incoming
// protocol and outgoing protocols support forwarding information, use
// context.

// TabletDialer represents a function that will return a QueryService
// object that can communicate with a tablet. Only the tablet's
// HostName and PortMap should be used (and maybe the alias for debug
// messages).
//
// When using this TabletDialer to talk to a l2vtgate, only the Hostname
// will be set to the full address to dial. Implementations should detect
// this use case as the portmap will then be empty.
//
// timeout represents the connection timeout. If set to 0, this
// connection should be established in the background and the
// TabletDialer should return right away.
type TabletDialer func(tablet *topodatapb.Tablet, timeout time.Duration) (queryservice.QueryService, error)

var dialers = make(map[string]TabletDialer)

// RegisterDialer is meant to be used by TabletDialer implementations
// to self register.
func RegisterDialer(name string, dialer TabletDialer) {
	if _, ok := dialers[name]; ok {
		log.Fatalf("Dialer %s already exists", name)
	}
	dialers[name] = dialer
}

// GetDialer returns the dialer to use, described by the command line flag
func GetDialer() TabletDialer {
	td, ok := dialers[*TabletProtocol]
	if !ok {
		log.Fatalf("No dialer registered for tablet protocol %s", *TabletProtocol)
	}
	return td
}
