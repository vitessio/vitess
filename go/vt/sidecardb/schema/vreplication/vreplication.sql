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

CREATE TABLE IF NOT EXISTS vreplication
(
    `id`                    int              NOT NULL AUTO_INCREMENT,
    `workflow`              varbinary(1000)           DEFAULT NULL,
    `source`                mediumblob       NOT NULL,
    `pos`                   varbinary(10000) NOT NULL,
    `stop_pos`              varbinary(10000)          DEFAULT NULL,
    `max_tps`               bigint           NOT NULL,
    `max_replication_lag`   bigint           NOT NULL,
    `cell`                  varbinary(1000)           DEFAULT NULL,
    `tablet_types`          varbinary(100)            DEFAULT NULL,
    `time_updated`          bigint           NOT NULL,
    `transaction_timestamp` bigint           NOT NULL,
    `state`                 varbinary(100)   NOT NULL,
    `message`               varbinary(1000)           DEFAULT NULL,
    `db_name`               varbinary(255)   NOT NULL,
    `rows_copied`           bigint           NOT NULL DEFAULT '0',
    `tags`                  varbinary(1024)  NOT NULL DEFAULT '',
    `time_heartbeat`        bigint           NOT NULL DEFAULT '0',
    `workflow_type`         int              NOT NULL DEFAULT '0',
    `time_throttled`        bigint           NOT NULL DEFAULT '0',
    `component_throttled`   varchar(255)     NOT NULL DEFAULT '',
    `workflow_sub_type`     int              NOT NULL DEFAULT '0',
    `defer_secondary_keys`  tinyint(1)       NOT NULL DEFAULT '0',
    PRIMARY KEY (`id`),
    KEY `workflow_idx` (`workflow`(64))
) ENGINE = InnoDB
