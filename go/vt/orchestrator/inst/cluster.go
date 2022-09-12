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
	"regexp"

	"vitess.io/vitess/go/vt/orchestrator/config"
)

// ClusterInfo makes for a cluster status/info summary
type ClusterInfo struct {
	ClusterName                             string
	ClusterDomain                           string // CNAME/VIP/A-record/whatever of the primary of this cluster
	CountInstances                          uint
	HeuristicLag                            int64
	HasAutomatedPrimaryRecovery             bool
	HasAutomatedIntermediatePrimaryRecovery bool
}

// ReadRecoveryInfo
func (clusterInfo *ClusterInfo) ReadRecoveryInfo() {
	clusterInfo.HasAutomatedPrimaryRecovery = clusterInfo.filtersMatchCluster(config.Config.RecoverPrimaryClusterFilters)
	clusterInfo.HasAutomatedIntermediatePrimaryRecovery = clusterInfo.filtersMatchCluster(config.Config.RecoverIntermediatePrimaryClusterFilters)
}

// filtersMatchCluster will see whether the given filters match the given cluster details
func (clusterInfo *ClusterInfo) filtersMatchCluster(filters []string) bool {
	for _, filter := range filters {
		if filter == clusterInfo.ClusterName {
			return true
		}
		if filter == "*" {
			return true
		} else if matched, _ := regexp.MatchString(filter, clusterInfo.ClusterName); matched && filter != "" {
			return true
		}
	}
	return false
}
