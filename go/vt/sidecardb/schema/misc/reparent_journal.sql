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

CREATE TABLE IF NOT EXISTS _vt.reparent_journal
(
    `time_created_ns`      bigint(20) unsigned NOT NULL,
    `action_name`          varbinary(250)      NOT NULL,
    `primary_alias`        varbinary(32)       NOT NULL,
    `replication_position` varbinary(64000) DEFAULT NULL,

    PRIMARY KEY (`time_created_ns`)
) ENGINE = InnoDB
