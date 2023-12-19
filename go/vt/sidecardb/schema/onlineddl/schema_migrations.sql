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

CREATE TABLE IF NOT EXISTS schema_migrations
(
    `id`                              bigint unsigned  NOT NULL AUTO_INCREMENT,
    `migration_uuid`                  varchar(64)      NOT NULL,
    `keyspace`                        varchar(256)     NOT NULL,
    `shard`                           varchar(255)     NOT NULL,
    `mysql_schema`                    varchar(128)     NOT NULL,
    `mysql_table`                     varchar(128)     NOT NULL,
    `migration_statement`             text             NOT NULL,
    `strategy`                        varchar(128)     NOT NULL,
    `options`                         varchar(8192)    NOT NULL,
    `added_timestamp`                 timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `requested_timestamp`             timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `ready_timestamp`                 timestamp        NULL     DEFAULT NULL,
    `started_timestamp`               timestamp        NULL     DEFAULT NULL,
    `liveness_timestamp`              timestamp        NULL     DEFAULT NULL,
    `completed_timestamp`             timestamp(6)     NULL     DEFAULT NULL,
    `cleanup_timestamp`               timestamp        NULL     DEFAULT NULL,
    `migration_status`                varchar(128)     NOT NULL,
    `log_path`                        varchar(1024)    NOT NULL,
    `artifacts`                       text             NOT NULL,
    `retries`                         int unsigned     NOT NULL DEFAULT '0',
    `tablet`                          varchar(128)     NOT NULL DEFAULT '',
    `tablet_failure`                  tinyint unsigned NOT NULL DEFAULT '0',
    `progress`                        float            NOT NULL DEFAULT '0',
    `migration_context`               varchar(1024)    NOT NULL DEFAULT '',
    `ddl_action`                      varchar(16)      NOT NULL DEFAULT '',
    `message`                         text             NOT NULL,
    `eta_seconds`                     bigint           NOT NULL DEFAULT '-1',
    `rows_copied`                     bigint unsigned  NOT NULL DEFAULT '0',
    `table_rows`                      bigint           NOT NULL DEFAULT '0',
    `added_unique_keys`               int unsigned     NOT NULL DEFAULT '0',
    `removed_unique_keys`             int unsigned     NOT NULL DEFAULT '0',
    `log_file`                        varchar(1024)    NOT NULL DEFAULT '',
    `retain_artifacts_seconds`        bigint           NOT NULL DEFAULT '0',
    `postpone_completion`             tinyint unsigned NOT NULL DEFAULT '0',
    `removed_unique_key_names`        text             NOT NULL,
    `dropped_no_default_column_names` text             NOT NULL,
    `expanded_column_names`           text             NOT NULL,
    `revertible_notes`                text             NOT NULL,
    `allow_concurrent`                tinyint unsigned NOT NULL DEFAULT '0',
    `reverted_uuid`                   varchar(64)      NOT NULL DEFAULT '',
    `is_view`                         tinyint unsigned NOT NULL DEFAULT '0',
    `ready_to_complete`               tinyint unsigned NOT NULL DEFAULT '0',
    `vitess_liveness_indicator`       bigint           NOT NULL DEFAULT '0',
    `user_throttle_ratio`             float            NOT NULL DEFAULT '0',
    `special_plan`                    text             NOT NULL,
    `last_throttled_timestamp`        timestamp        NULL     DEFAULT NULL,
    `component_throttled`             tinytext         NOT NULL,
    `cancelled_timestamp`             timestamp        NULL     DEFAULT NULL,
    `postpone_launch`                 tinyint unsigned NOT NULL DEFAULT '0',
    `stage`                           text             NOT NULL,
    `cutover_attempts`                int unsigned     NOT NULL DEFAULT '0',
    `is_immediate_operation`          tinyint unsigned NOT NULL DEFAULT '0',
    `reviewed_timestamp`              timestamp        NULL DEFAULT NULL,
    `ready_to_complete_timestamp`     timestamp        NULL DEFAULT NULL,
    `removed_foreign_key_names`       text             NOT NULL,
    `last_cutover_attempt_timestamp`  timestamp        NULL DEFAULT NULL,
    `force_cutover`                   tinyint unsigned NOT NULL DEFAULT '0',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uuid_idx` (`migration_uuid`),
    KEY `keyspace_shard_idx` (`keyspace`(64), `shard`(64)),
    KEY `status_idx` (`migration_status`, `liveness_timestamp`),
    KEY `cleanup_status_idx` (`cleanup_timestamp`, `migration_status`),
    KEY `tablet_failure_idx` (`tablet_failure`, `migration_status`, `retries`),
    KEY `table_complete_idx` (`migration_status`, `keyspace`(64), `mysql_table`(64), `completed_timestamp`),
    KEY `migration_context_idx` (`migration_context`(64)),
    KEY `reverted_uuid_idx` (`reverted_uuid`)
) ENGINE = InnoDB
