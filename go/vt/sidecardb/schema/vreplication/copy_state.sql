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

CREATE TABLE IF NOT EXISTS _vt.copy_state
(
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `vrepl_id`   int            NOT NULL,
    `table_name` varbinary(128) NOT NULL,
    `lastpk`     varbinary(2000) DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `vrepl_id` (`vrepl_id`,`table_name`)
) ENGINE = InnoDB
