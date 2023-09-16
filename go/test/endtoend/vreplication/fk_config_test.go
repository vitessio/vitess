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

var (
	initialFKSchema = `
create table parent(id int, name varchar(128), primary key(id)) engine=innodb;
create table child(id int, parent_id int, name varchar(128), primary key(id), foreign key(parent_id) references parent(id) on delete cascade) engine=innodb;
`
	initialFKData = `
insert into parent values(1, 'parent1'), (2, 'parent2');
insert into child values(1, 1, 'child11'), (2, 1, 'child21'), (3, 2, 'child32');`

	initialFKSourceVSchema = `
{
  "tables": {
	"parent": {},
	"child": {}
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
    }
  }
}
`
)
