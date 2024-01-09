/*
Copyright 2024 The Vitess Authors.

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

package capabilities

type FlavorCapability int

const (
	NoneFlavorCapability                        FlavorCapability = iota // default placeholder
	FastDropTableFlavorCapability                                       // supported in MySQL 8.0.23 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-23.html
	TransactionalGtidExecutedFlavorCapability                           //
	InstantDDLFlavorCapability                                          // ALGORITHM=INSTANT general support
	InstantAddLastColumnFlavorCapability                                //
	InstantAddDropVirtualColumnFlavorCapability                         //
	InstantAddDropColumnFlavorCapability                                // Adding/dropping column in any position/ordinal.
	InstantChangeColumnDefaultFlavorCapability                          //
	InstantExpandEnumCapability                                         //
	MySQLJSONFlavorCapability                                           // JSON type supported
	MySQLUpgradeInServerFlavorCapability                                //
	DynamicRedoLogCapacityFlavorCapability                              // supported in MySQL 8.0.30 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-30.html
	DisableRedoLogFlavorCapability                                      // supported in MySQL 8.0.21 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-21.html
	CheckConstraintsCapability                                          // supported in MySQL 8.0.16 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-16.html
	PerformanceSchemaDataLocksTableCapability
)

type CapableOf func(capability FlavorCapability) (bool, error)
