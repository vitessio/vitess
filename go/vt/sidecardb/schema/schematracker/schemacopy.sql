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

CREATE TABLE IF NOT EXISTS _vt.schemacopy
(
    `table_schema`       varchar(64)     NOT NULL,
    `table_name`         varchar(64)     NOT NULL,
    `column_name`        varchar(64)     NOT NULL,
    `ordinal_position`   bigint unsigned NOT NULL,
    `character_set_name` varchar(32) DEFAULT NULL,
    `collation_name`     varchar(32) DEFAULT NULL,
    `data_type`          varchar(64)     NOT NULL,
    `column_key`         varchar(3)      NOT NULL,
    PRIMARY KEY (`table_schema`, `table_name`, `ordinal_position`)
) ENGINE = InnoDB
