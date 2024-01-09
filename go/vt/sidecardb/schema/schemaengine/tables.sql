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

CREATE TABLE IF NOT EXISTS tables
(
    TABLE_SCHEMA varchar(64) CHARACTER SET `utf8mb3` COLLATE `utf8mb3_bin` NOT NULL,
    TABLE_NAME varchar(64) CHARACTER SET `utf8mb3` COLLATE `utf8mb3_bin` NOT NULL,
    CREATE_STATEMENT longtext,
    CREATE_TIME BIGINT,
    PRIMARY KEY (TABLE_SCHEMA, TABLE_NAME)
) engine = InnoDB
