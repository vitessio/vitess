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

CREATE TABLE IF NOT EXISTS _vt.schema_version
(
    id           INT NOT NULL AUTO_INCREMENT,
    pos          VARBINARY(10000) NOT NULL,
    time_updated BIGINT(20)       NOT NULL,
    ddl          BLOB DEFAULT NULL,
    schemax      LONGBLOB         NOT NULL,
    PRIMARY KEY (id)
) ENGINE = InnoDB
