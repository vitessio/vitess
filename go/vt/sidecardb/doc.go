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

package sidecardb

/*

The sidecardb module is used to create and upgrade the sidecar database schema on tablet init. The sidecar database
is named `_vt`.

The schema subdirectory has subdirectories, categorized by module, with one file per table in _vt. Each has the latest
schema for each table in _vt (in the form of a create table statement).

sidecardb uses the schemadiff module in Vitess to reach the desired schema for each table.

Note:

The `if not exists` in the schema files should not be needed since we only create tables in the sidecar database if they don't exist.
However, during development, we came across some Vitess flows like backup restore on replicas where the database
already had the tables but mysql replication also found these `create`s
in the primary's binlog causing the replica tablet to halt since it could not execute the duplicate create.

We did fix these flows and hence ideally this **should never happen**, but as an abundance of caution
I have left it in now for operational reasons, so that we paper over any bugs for now.
We can remove it in v17 or v18 once the schema init is stable and we have done more testing.

*/
