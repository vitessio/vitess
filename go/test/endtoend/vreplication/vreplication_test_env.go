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
	"Mirroring 0.00 percent of traffic from keyspace product to keyspace customer for tablet types [PRIMARY]",
	"/Stop writes on keyspace product for tables [Lead,Lead-1,blüb_tbl,customer,db_order_test,geom_tbl,json_tbl,loadtest,reftable,vdiff_order]: [keyspace:product;shard:0;position:",
	"Wait for vreplication on stopped streams to catchup for up to 30s",
	"Create reverse vreplication workflow p2c_reverse",
	"Create journal entries on source databases",
	"Enable writes on keyspace customer for tables [Lead,Lead-1,blüb_tbl,customer,db_order_test,geom_tbl,json_tbl,loadtest,reftable,vdiff_order]",
	"Switch routing from keyspace product to keyspace customer",
	"Routing rules for tables [Lead,Lead-1,blüb_tbl,customer,db_order_test,geom_tbl,json_tbl,loadtest,reftable,vdiff_order] will be updated",
	"Switch writes completed, freeze and delete vreplication streams on: [tablet:200,tablet:300]",
	"Start reverse vreplication streams on: [tablet:100]",
	"Mark vreplication streams frozen on: [keyspace:customer;shard:-80;tablet:200;workflow:p2c;dbname:vt_customer,keyspace:customer;shard:80-;tablet:300;workflow:p2c;dbname:vt_customer]",
	"Unlock keyspace customer",
	"Unlock keyspace product",
	"", // Additional empty newline in the output
}

var dryRunResultsReadCustomerShard = []string{
	"Lock keyspace product",
	"Mirroring 0.00 percent of traffic from keyspace product to keyspace customer for tablet types [RDONLY,REPLICA]",
	"Switch reads for tables [Lead,Lead-1,blüb_tbl,customer,db_order_test,geom_tbl,json_tbl,loadtest,reftable,vdiff_order] to keyspace customer for tablet types [RDONLY,REPLICA]",
	"Routing rules for tables [Lead,Lead-1,blüb_tbl,customer,db_order_test,geom_tbl,json_tbl,loadtest,reftable,vdiff_order] will be updated",
	"Serving VSchema will be rebuilt for the customer keyspace",
	"Unlock keyspace product",
	"", // Additional empty newline in the output
}

var dryRunResultsSwitchWritesM2m3 = []string{
	"Lock keyspace merchant-type",
	"Stop streams on keyspace merchant-type",
	"/      Id 2 Keyspace customer Shard -80 Rules rules:{match:\"morders\" filter:\"select oid, cid, mname, pid, price, qty, total from orders where in_keyrange(mname, 'merchant-type.md5', '-80')\"} at Position ",
	"/      Id 2 Keyspace customer Shard -80 Rules rules:{match:\"morders\" filter:\"select oid, cid, mname, pid, price, qty, total from orders where in_keyrange(mname, 'merchant-type.md5', '80-')\"} at Position ",
	"/      Id 3 Keyspace customer Shard 80- Rules rules:{match:\"morders\" filter:\"select oid, cid, mname, pid, price, qty, total from orders where in_keyrange(mname, 'merchant-type.md5', '-80')\"} at Position ",
	"/      Id 3 Keyspace customer Shard 80- Rules rules:{match:\"morders\" filter:\"select oid, cid, mname, pid, price, qty, total from orders where in_keyrange(mname, 'merchant-type.md5', '80-')\"} at Position ",
	"/      Id 4 Keyspace customer Shard -80 Rules rules:{match:\"msales\" filter:\"select mname as merchant_name, count(*) as kount, sum(price) as amount from orders where in_keyrange(mname, 'merchant-type.md5', '-80') group by merchant_name\"} at Position ",
	"/      Id 4 Keyspace customer Shard -80 Rules rules:{match:\"msales\" filter:\"select mname as merchant_name, count(*) as kount, sum(price) as amount from orders where in_keyrange(mname, 'merchant-type.md5', '80-') group by merchant_name\"} at Position ",
	"/      Id 5 Keyspace customer Shard 80- Rules rules:{match:\"msales\" filter:\"select mname as merchant_name, count(*) as kount, sum(price) as amount from orders where in_keyrange(mname, 'merchant-type.md5', '-80') group by merchant_name\"} at Position ",
	"/      Id 5 Keyspace customer Shard 80- Rules rules:{match:\"msales\" filter:\"select mname as merchant_name, count(*) as kount, sum(price) as amount from orders where in_keyrange(mname, 'merchant-type.md5', '80-') group by merchant_name\"} at Position ",
	"Stop writes on keyspace merchant-type, tables [/.*]:",
	"/      Keyspace merchant-type, Shard -80 at Position",
	"/      Keyspace merchant-type, Shard 80- at Position",
	"Wait for VReplication on stopped streams to catchup for up to 30s",
	"Create reverse replication workflow m2m3_reverse",
	"Create journal entries on source databases",
	"Enable writes on keyspace merchant-type tables [/.*]",
	"Switch routing from keyspace merchant-type to keyspace merchant-type",
	"IsPrimaryServing will be set to false for:",
	"       Shard -80, Tablet 400 ",
	"       Shard 80-, Tablet 500 ",
	"IsPrimaryServing will be set to true for:",
	"       Shard -40, Tablet 1600 ",
	"       Shard 40-c0, Tablet 1700 ",
	"       Shard c0-, Tablet 1800 ",
	"Switch writes completed, freeze and delete vreplication streams on:",
	"       tablet 1600 ",
	"       tablet 1700 ",
	"       tablet 1800 ",
	"Start reverse replication streams on:",
	"       tablet 400 ",
	"       tablet 500 ",
	"Mark vreplication streams frozen on:",
	"       Keyspace merchant-type, Shard -40, Tablet 1600, Workflow m2m3, DbName vt_merchant-type",
	"       Keyspace merchant-type, Shard 40-c0, Tablet 1700, Workflow m2m3, DbName vt_merchant-type",
	"       Keyspace merchant-type, Shard c0-, Tablet 1800, Workflow m2m3, DbName vt_merchant-type",
	"Unlock keyspace merchant-type",
}

var dryRunResultsSwitchReadM2m3 = []string{
	"Lock keyspace merchant-type",
	"Switch reads from keyspace merchant-type to keyspace merchant-type for shards -80,80- to shards -40,40-c0,c0-",
	"Unlock keyspace merchant-type",
}
