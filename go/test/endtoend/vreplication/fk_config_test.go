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

package vreplication

// The tables in the schema are selected so that we have one parent/child table with names in reverse lexical order
// (child before parent), t1,t2  are in lexical order, and t11,t12  have valid circular foreign key constraints.
var (
	initialFKSchema = `
create table parent(id int, name varchar(128), primary key(id)) engine=innodb;
create table child(id int, parent_id int, name varchar(128), primary key(id), foreign key(parent_id) references parent(id) on delete cascade) engine=innodb;
create view vparent as select * from parent;
create table t1(id int, name varchar(128), primary key(id)) engine=innodb;
create table t2(id int, t1id int, name varchar(128), primary key(id), foreign key(t1id) references t1(id) on delete cascade) engine=innodb;
create table t11 (id int primary key, i int);
create table t12 (id int primary key, i int);
alter table t11 add constraint f11 foreign key (i) references t12 (id);
alter table t12 add constraint f12 foreign key (i) references t11 (id);
`
	initialFKData = `
insert into parent values(1, 'parent1'), (2, 'parent2');
insert into child values(1, 1, 'child11'), (2, 1, 'child21'), (3, 2, 'child32');
insert into t1 values(1, 't11'), (2, 't12');
insert into t2 values(1, 1, 't21'), (2, 1, 't22'), (3, 2, 't23');
insert into t11 values(1, null);
insert into t12 values(1, 1);
update t11 set i = 1 where id = 1;
`

	initialFKSourceVSchema = `
{
  "tables": {
	"parent": {},
	"child": {},
	"t1": {},
	"t2": {},
	"t11": {},
	"t12": {}
  }
}
`

	initialFKTargetVSchema = `
{
  "sharded": true,
  "vindexes": {
    "reverse_bits": {
      "type": "reverse_bits"
    }
  },
  "tables": {
    "parent": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "reverse_bits"
        }
      ]
    },
    "child": {
      "column_vindexes": [
        {
          "column": "parent_id",
          "name": "reverse_bits"
        }
      ]
    },
    "t1": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "reverse_bits"
        }
      ]
    },
    "t2": {
      "column_vindexes": [
        {
          "column": "t1id",
          "name": "reverse_bits"
        }
      ]
    },
    "t11": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "reverse_bits"
        }
      ]
    },
    "t12": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "reverse_bits"
        }
      ]
    }
  }
}
`
)
