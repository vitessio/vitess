/*
Copyright 2023 The Vitess Authors.

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

package throttlerapp

import "strings"

func Concatenate(names ...string) string {
	return strings.Join(names, ":")
}

type Name string

func (n Name) String() string {
	return string(n)
}

func (n Name) Equals(s string) bool {
	return string(n) == s
}

func (n Name) ConcatenateString(s string) string {
	return Concatenate(n.String(), s)
}

func (n Name) Concatenate(other Name) Name {
	return Name(n.ConcatenateString(other.String()))
}
func (n Name) SplitStrings() []string {
	return strings.Split(n.String(), ":")
}

const (
	// AllName is a special catch-all name for all apps
	AllName Name = "all"
	// VitessName is used by vitess tablets when communicating between themselves,
	// as well as for self checks.
	// It is also the name used by checks that do not identify by any app name.
	VitessName Name = "vitess"
	// ThrottlerStimulatorName is used by a replica tablet to stimulate the throttler on the Primary tablet
	ThrottlerStimulatorName Name = "throttler-stimulator"

	TableGCName   Name = "tablegc"
	OnlineDDLName Name = "online-ddl"
	GhostName     Name = "gh-ost"
	PTOSCName     Name = "pt-osc"

	VReplicationName      Name = "vreplication"
	VStreamerName         Name = "vstreamer"
	VPlayerName           Name = "vplayer"
	VCopierName           Name = "vcopier"
	ResultStreamerName    Name = "resultstreamer"
	RowStreamerName       Name = "rowstreamer"
	ExternalConnectorName Name = "external-connector"
	ReplicaConnectorName  Name = "replica-connector"

	BinlogWatcherName Name = "binlog-watcher"
	MessagerName      Name = "messager"
	SchemaTrackerName Name = "schema-tracker"

	TestingName                Name = "test"
	TestingAlwaysThrottlerName Name = "always-throttled-app"
)

var (
	exemptFromChecks = map[string]bool{
		BinlogWatcherName.String(): true,
		MessagerName.String():      true,
		SchemaTrackerName.String(): true,
	}
)

// ExemptFromChecks returns 'true' for apps that should skip the throttler checks. The throttler should
// always respond with automated "OK" to those apps, without delay. These apps also do not cause a heartbeat renewal.
func ExemptFromChecks(appName string) bool {
	return exemptFromChecks[appName]
}
