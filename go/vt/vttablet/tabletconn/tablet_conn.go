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

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
// object that can communicate with a tablet.
//
// When using this TabletDialer to talk to a l2vtgate, only the Hostname
// will be set to the full address to dial. Implementations should detect
// this use case as the portmap will then be empty.
type TabletDialer func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error)

var dialers = make(map[string]TabletDialer)

// RegisterDialer registers an implementation of TabletDialer.
func RegisterDialer(name string, dialer TabletDialer) {
	if _, ok := dialers[name]; ok {
		log.Fatalf("Dialer %s already exists", name)
	}
	dialers[name] = dialer
}

// GetDialer returns the default dialer specified by the command line flag.
func GetDialer() TabletDialer {
	td, ok := dialers[*TabletProtocol]
	if !ok {
		log.Exitf("No dialer registered for tablet protocol %s", *TabletProtocol)
	}
	return td
}

// GetDialerByName returns the requested dialer.
// It returns nil if one is not found.
func GetDialerByName(dialer string) TabletDialer {
	return dialers[dialer]
}
