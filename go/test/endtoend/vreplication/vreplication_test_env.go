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

package vreplication

var dryRunResultsSwitchWritesCustomerShard = []string{
	"Lock keyspace product",
	"Lock keyspace customer",
	"Stop writes on keyspace product, tables [customer]:",
	"/       Keyspace product, Shard 0 at Position",
	"Wait for VReplication on stopped streams to catchup for upto 30s",
	"Create reverse replication workflow p2c_reverse",
	"Create journal entries on source databases",
	"Enable writes on keyspace customer tables [customer]",
	"Switch routing from keyspace product to keyspace customer",
	"Routing rules for tables [customer] will be updated",
	"SwitchWrites completed, freeze and delete vreplication streams on:",
	"       tablet 200 ",
	"       tablet 300 ",
	"Start reverse replication streams on:",
	"       tablet 100 ",
	"Mark vreplication streams frozen on:",
	"       Keyspace customer, Shard -80, Tablet 200, Workflow p2c, DbName vt_customer",
	"       Keyspace customer, Shard 80-, Tablet 300, Workflow p2c, DbName vt_customer",
	"Unlock keyspace customer",
	"Unlock keyspace product",
}

var dryRunResultsReadCustomerShard = []string{
	"Lock keyspace product",
	"Switch reads for tables [customer] to keyspace customer for tablet types [REPLICA,RDONLY]",
	"Routing rules for tables [customer] will be updated",
	"Unlock keyspace product",
}

var dryRunResultsSwitchWritesM2m3 = []string{
	"Lock keyspace merchant",
	"Stop streams on keyspace merchant",
	"/      Id 2 Keyspace customer Shard -80 Rules rules:<match:\"morders\" filter:\"select * from orders where in_keyrange(mname, 'merchant.md5', '-80')\" >  at Position ",
	"/      Id 2 Keyspace customer Shard -80 Rules rules:<match:\"morders\" filter:\"select * from orders where in_keyrange(mname, 'merchant.md5', '80-')\" >  at Position ",
	"/      Id 3 Keyspace customer Shard 80- Rules rules:<match:\"morders\" filter:\"select * from orders where in_keyrange(mname, 'merchant.md5', '-80')\" >  at Position ",
	"/      Id 3 Keyspace customer Shard 80- Rules rules:<match:\"morders\" filter:\"select * from orders where in_keyrange(mname, 'merchant.md5', '80-')\" >  at Position ",
	"/      Id 4 Keyspace customer Shard -80 Rules rules:<match:\"msales\" filter:\"select mname as merchant_name, count(*) as kount, sum(price) as amount from orders where in_keyrange(mname, 'merchant.md5', '-80') group by merchant_name\" >  at Position ",
	"/      Id 4 Keyspace customer Shard -80 Rules rules:<match:\"msales\" filter:\"select mname as merchant_name, count(*) as kount, sum(price) as amount from orders where in_keyrange(mname, 'merchant.md5', '80-') group by merchant_name\" >  at Position ",
	"/      Id 5 Keyspace customer Shard 80- Rules rules:<match:\"msales\" filter:\"select mname as merchant_name, count(*) as kount, sum(price) as amount from orders where in_keyrange(mname, 'merchant.md5', '-80') group by merchant_name\" >  at Position ",
	"/      Id 5 Keyspace customer Shard 80- Rules rules:<match:\"msales\" filter:\"select mname as merchant_name, count(*) as kount, sum(price) as amount from orders where in_keyrange(mname, 'merchant.md5', '80-') group by merchant_name\" >  at Position ",
	"Stop writes on keyspace merchant, tables [/.*]:",
	"/      Keyspace merchant, Shard -80 at Position",
	"/      Keyspace merchant, Shard 80- at Position",
	"Wait for VReplication on stopped streams to catchup for upto 30s",
	"Create reverse replication workflow m2m3_reverse",
	"Create journal entries on source databases",
	"Enable writes on keyspace merchant tables [/.*]",
	"Switch routing from keyspace merchant to keyspace merchant",
	"IsMasterServing will be set to false for:",
	"       Shard -80, Tablet 400 ",
	"       Shard 80-, Tablet 500 ",
	"IsMasterServing will be set to true for:",
	"       Shard -40, Tablet 1600 ",
	"       Shard 40-c0, Tablet 1700 ",
	"       Shard c0-, Tablet 1800 ",
	"SwitchWrites completed, freeze and delete vreplication streams on:",
	"       tablet 1600 ",
	"       tablet 1700 ",
	"       tablet 1800 ",
	"Start reverse replication streams on:",
	"       tablet 400 ",
	"       tablet 500 ",
	"Mark vreplication streams frozen on:",
	"       Keyspace merchant, Shard -40, Tablet 1600, Workflow m2m3, DbName vt_merchant",
	"       Keyspace merchant, Shard 40-c0, Tablet 1700, Workflow m2m3, DbName vt_merchant",
	"       Keyspace merchant, Shard c0-, Tablet 1800, Workflow m2m3, DbName vt_merchant",
	"Unlock keyspace merchant",
}

var dryRunResultsDropSourcesDropCustomerShard = []string{
	"Lock keyspace product",
	"Lock keyspace customer",
	"Dropping these tables from the database and removing them from the vschema for keyspace product:",
	"	Keyspace product Shard 0 DbName vt_product Tablet 100 Table customer",
	"Blacklisted tables [customer] will be removed from:",
	"	Keyspace product Shard 0 Tablet 100",
	"Delete reverse vreplication streams on source:",
	"	Keyspace product Shard 0 Workflow p2c_reverse DbName vt_product Tablet 100",
	"Delete vreplication streams on target:",
	"	Keyspace customer Shard -80 Workflow p2c DbName vt_customer Tablet 200",
	"	Keyspace customer Shard 80- Workflow p2c DbName vt_customer Tablet 300",
	"Routing rules for participating tables will be deleted",
	"Unlock keyspace customer",
	"Unlock keyspace product",
}

var dryRunResultsDropSourcesRenameCustomerShard = []string{
	"Lock keyspace product",
	"Lock keyspace customer",
	"Renaming these tables from the database and removing them from the vschema for keyspace product:",
	"	Keyspace product Shard 0 DbName vt_product Tablet 100 Table customer",
	"Blacklisted tables [customer] will be removed from:",
	"	Keyspace product Shard 0 Tablet 100",
	"Delete reverse vreplication streams on source:",
	"	Keyspace product Shard 0 Workflow p2c_reverse DbName vt_product Tablet 100",
	"Delete vreplication streams on target:",
	"	Keyspace customer Shard -80 Workflow p2c DbName vt_customer Tablet 200",
	"	Keyspace customer Shard 80- Workflow p2c DbName vt_customer Tablet 300",
	"Routing rules for participating tables will be deleted",
	"Unlock keyspace customer",
	"Unlock keyspace product",
}
