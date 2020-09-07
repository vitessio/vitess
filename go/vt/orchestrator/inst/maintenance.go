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

package inst

import (
	"vitess.io/vitess/go/vt/orchestrator/config"
)

// Maintenance indicates a maintenance entry (also in the database)
type Maintenance struct {
	MaintenanceId  uint
	Key            InstanceKey
	BeginTimestamp string
	SecondsElapsed uint
	IsActive       bool
	Owner          string
	Reason         string
}

var maintenanceOwner string = ""

func GetMaintenanceOwner() string {
	if maintenanceOwner != "" {
		return maintenanceOwner
	}
	return config.MaintenanceOwner
}

func SetMaintenanceOwner(owner string) {
	maintenanceOwner = owner
}
