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

CREATE TABLE IF NOT EXISTS _vt.vdiff
(
    `id`                 bigint(20)   NOT NULL AUTO_INCREMENT,
    `vdiff_uuid`         varchar(64)  NOT NULL,
    `workflow`           varbinary(1024)       DEFAULT NULL,
    `keyspace`           varbinary(256)        DEFAULT NULL,
    `shard`              varchar(255) NOT NULL,
    `db_name`            varbinary(1024)       DEFAULT NULL,
    `state`              varbinary(64)         DEFAULT NULL,
    `options`            json                  DEFAULT NULL,
    `created_at`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `started_at`         timestamp    NULL     DEFAULT NULL,
    `liveness_timestamp` timestamp    NULL     DEFAULT NULL,
    `completed_at`       timestamp    NULL     DEFAULT NULL,
    `last_error`         varbinary(512)        DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uuid_idx` (`vdiff_uuid`),
    KEY `state` (`state`),
    KEY `ks_wf_idx` (`keyspace`(64), `workflow`(64))
) ENGINE = InnoDB
