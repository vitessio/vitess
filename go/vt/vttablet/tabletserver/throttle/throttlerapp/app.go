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

const (
	// DefaultName is the app name used by vitess when app doesn't indicate its name
	DefaultName = "default"
	VitessName  = "vitess"

	TableGCName   = "tablegc"
	OnlineDDLName = "online-ddl"
	GhostName     = "gh-ost"
	PTOSCName     = "pt-osc"

	VStreamerName         = "vstreamer"
	VReplicationName      = "vreplication"
	VPlayerName           = "vplayer"
	VCopierName           = "vcopier"
	ResultStreamerName    = "resultstreamer"
	RowStreamerName       = "rowstreamer"
	ExternalConnectorName = "external-connector"
	ReplicaConnectorName  = "replica-connector"

	BinlogWatcherName = "binlog-watcher"
	MessagerName      = "messager"
	SchemaTrackerName = "schema-tracker"
)

var (
	exemptFromChecks = map[string]bool{
		BinlogWatcherName: true,
		MessagerName:      true,
		SchemaTrackerName: true,
	}
)

// ExemptFromChecks returns 'true' for apps that should skip the throttler checks. The throttler should
// always repsond with automated "OK" to those apps, without delay. These apps also do not cause a heartbeat renewal.
func ExemptFromChecks(appName string) bool {
	return exemptFromChecks[appName]
}
