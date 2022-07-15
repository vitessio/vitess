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
	"strings"

	"vitess.io/vitess/go/vt/orchestrator/config"
)

// mappedClusterNameToAlias attempts to match a cluster with an alias based on
// configured ClusterNameToAlias map
func mappedClusterNameToAlias(clusterName string) string {
	for pattern, alias := range config.Config.ClusterNameToAlias {
		if pattern == "" {
			// sanity
			continue
		}
		if matched, _ := regexp.MatchString(pattern, clusterName); matched {
			return alias
		}
	}
	return ""
}

// ClusterInfo makes for a cluster status/info summary
type ClusterInfo struct {
	ClusterName                             string
	ClusterAlias                            string // Human friendly alias
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
		if filter == clusterInfo.ClusterAlias {
			return true
		}
		if strings.HasPrefix(filter, "alias=") {
			// Match by exact cluster alias name
			alias := strings.SplitN(filter, "=", 2)[1]
			if alias == clusterInfo.ClusterAlias {
				return true
			}
		} else if strings.HasPrefix(filter, "alias~=") {
			// Match by cluster alias regex
			aliasPattern := strings.SplitN(filter, "~=", 2)[1]
			if matched, _ := regexp.MatchString(aliasPattern, clusterInfo.ClusterAlias); matched {
				return true
			}
		} else if filter == "*" {
			return true
		} else if matched, _ := regexp.MatchString(filter, clusterInfo.ClusterName); matched && filter != "" {
			return true
		}
	}
	return false
}

// ApplyClusterAlias updates the given clusterInfo's ClusterAlias property
func (clusterInfo *ClusterInfo) ApplyClusterAlias() {
	if clusterInfo.ClusterAlias != "" && clusterInfo.ClusterAlias != clusterInfo.ClusterName {
		// Already has an alias; abort
		return
	}
	if alias := mappedClusterNameToAlias(clusterInfo.ClusterName); alias != "" {
		clusterInfo.ClusterAlias = alias
	}
}
