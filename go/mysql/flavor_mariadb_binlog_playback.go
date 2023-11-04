/*

Copyright 2019 The Vitess Authors.

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

package mysql

// baseShowTables is part of the Flavor interface.
func (mariadbFlavor) baseShowTables() string {
	return mysqlFlavor{}.baseShowTables()
}

// baseShowTablesWithSizes is part of the Flavor interface.
func (mariadbFlavor101) baseShowTablesWithSizes() string {
	return TablesWithSize56
}

// baseShowTablesWithSizes is part of the Flavor interface.
func (mariadbFlavor102) baseShowTablesWithSizes() string {
	return TablesWithSize57
}
