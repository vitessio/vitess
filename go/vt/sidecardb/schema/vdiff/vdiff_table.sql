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

CREATE TABLE IF NOT EXISTS vdiff_table
(
    `vdiff_id`      varchar(64)    NOT NULL,
    `table_name`    varbinary(128) NOT NULL,
    `state`         varbinary(64)           DEFAULT NULL,
    `lastpk`        varbinary(2000)         DEFAULT NULL,
    `table_rows`    bigint(20)     NOT NULL DEFAULT '0',
    `rows_compared` bigint(20)     NOT NULL DEFAULT '0',
    `mismatch`      tinyint(1)     NOT NULL DEFAULT '0',
    `report`        json                    DEFAULT NULL,
    `created_at`    timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`    timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`vdiff_id`, `table_name`)
) ENGINE = InnoDB
