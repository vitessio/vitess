/*
Copyright 2020 The Vitess Authors.

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

package discovery

import (
	"bytes"
	"strings"

	"vitess.io/vitess/go/vt/vttablet/queryservice"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
)

// TabletHealth represents simple tablet health data that is returned to users of healthcheck.
// No synchronization is required because we always return a copy.
type TabletHealth struct {
	Conn                queryservice.QueryService
	Tablet              *topodata.Tablet
	Target              *query.Target
	Stats               *query.RealtimeStats
	MasterTermStartTime int64
	LastError           error
	Serving             bool
}

// DeepEqual compares two TabletHealth. Since we include protos, we
// need to use proto.Equal on these.
func (th *TabletHealth) DeepEqual(other *TabletHealth) bool {
	return proto.Equal(th.Tablet, other.Tablet) &&
		proto.Equal(th.Target, other.Target) &&
		th.Serving == other.Serving &&
		th.MasterTermStartTime == other.MasterTermStartTime &&
		proto.Equal(th.Stats, other.Stats) &&
		((th.LastError == nil && other.LastError == nil) ||
			(th.LastError != nil && other.LastError != nil && th.LastError.Error() == other.LastError.Error()))
}

// GetTabletHostPort formats a tablet host port address.
func (th *TabletHealth) GetTabletHostPort() string {
	hostname := th.Tablet.Hostname
	vtPort := th.Tablet.PortMap["vt"]
	return netutil.JoinHostPort(hostname, vtPort)
}

// GetHostNameLevel returns the specified hostname level. If the level does not exist it will pick the closest level.
// This seems unused but can be utilized by certain url formatting templates. See getTabletDebugURL for more details.
func (th *TabletHealth) GetHostNameLevel(level int) string {
	hostname := th.Tablet.Hostname
	chunkedHostname := strings.Split(hostname, ".")

	if level < 0 {
		return chunkedHostname[0]
	} else if level >= len(chunkedHostname) {
		return chunkedHostname[len(chunkedHostname)-1]
	} else {
		return chunkedHostname[level]
	}
}

// getTabletDebugURL formats a debug url to the tablet.
// It uses a format string that can be passed into the app to format
// the debug URL to accommodate different network setups. It applies
// the html/template string defined to a tabletHealthCheck object. The
// format string can refer to members and functions of tabletHealthCheck
// like a regular html/template string.
//
// For instance given a tablet with hostname:port of host.dc.domain:22
// could be configured as follows:
// http://{{.GetTabletHostPort}} -> http://host.dc.domain:22
// https://{{.Tablet.Hostname}} -> https://host.dc.domain
// https://{{.GetHostNameLevel 0}}.bastion.corp -> https://host.bastion.corp
func (th *TabletHealth) getTabletDebugURL() string {
	var buffer bytes.Buffer
	tabletURLTemplate.Execute(&buffer, th)
	return buffer.String()
}
