/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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

// SetClusterAlias will write (and override) a single cluster name mapping
func SetClusterAlias(clusterName string, alias string) error {
	return writeClusterAlias(clusterName, alias)
}

// SetClusterAliasManualOverride will write (and override) a single cluster name mapping
func SetClusterAliasManualOverride(clusterName string, alias string) error {
	return writeClusterAliasManualOverride(clusterName, alias)
}

// GetClusterByAlias returns the cluster name associated with given alias.
// The function returns with error when:
// - No cluster is associated with the alias
// - More than one cluster is associated with the alias
func GetClusterByAlias(alias string) (string, error) {
	return ReadClusterNameByAlias(alias)
}
