/*
   Copyright 2014 Outbrain Inc.

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

package agent

import "vitess.io/vitess/go/vt/orchestrator/inst"

// LogicalVolume describes an LVM volume
type LogicalVolume struct {
	Name            string
	GroupName       string
	Path            string
	IsSnapshot      bool
	SnapshotPercent float64
}

// Mount describes a file system mount point
type Mount struct {
	Path           string
	Device         string
	LVPath         string
	FileSystem     string
	IsMounted      bool
	DiskUsage      int64
	MySQLDataPath  string
	MySQLDiskUsage int64
}

// Agent presents the data of an agent
type Agent struct {
	Hostname                string
	Port                    int
	Token                   string
	LastSubmitted           string
	AvailableLocalSnapshots []string
	AvailableSnapshots      []string
	LogicalVolumes          []LogicalVolume
	MountPoint              Mount
	MySQLRunning            bool
	MySQLDiskUsage          int64
	MySQLPort               int64
	MySQLDatadirDiskFree    int64
	MySQLErrorLogTail       []string
}

// SeedOperation makes for the high level data & state of a seed operation
type SeedOperation struct {
	SeedId         int64
	TargetHostname string
	SourceHostname string
	StartTimestamp string
	EndTimestamp   string
	IsComplete     bool
	IsSuccessful   bool
}

// SeedOperationState represents a single state (step) in a seed operation
type SeedOperationState struct {
	SeedStateId    int64
	SeedId         int64
	StateTimestamp string
	Action         string
	ErrorMessage   string
}

// Build an instance key for a given agent
func (this *Agent) GetInstance() *inst.InstanceKey {
	return &inst.InstanceKey{Hostname: this.Hostname, Port: int(this.MySQLPort)}
}
