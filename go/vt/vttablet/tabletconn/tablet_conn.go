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

package tabletconn

import (
	"flag"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/grpcclient"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

var (
	// ConnClosed is returned when the underlying connection was closed.
	ConnClosed = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "vttablet: Connection Closed")
)

var (
	// TabletProtocol is exported for unit tests
	TabletProtocol = flag.String("tablet_protocol", "grpc", "how to talk to the vttablets")
)

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
type TabletDialer func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error)

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
		log.Exitf("No dialer registered for tablet protocol %s", *TabletProtocol)
	}
	return td
}
