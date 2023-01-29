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

CREATE TABLE IF NOT EXISTS _vt.vreplication_log
(
    `id`         bigint         NOT NULL AUTO_INCREMENT,
    `vrepl_id`   int            NOT NULL,
    `type`       varbinary(256) NOT NULL,
    `state`      varbinary(100) NOT NULL,
    `created_at` timestamp      NULL     DEFAULT CURRENT_TIMESTAMP,
    `updated_at` timestamp      NULL     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `message`    text           NOT NULL,
    `count`      bigint         NOT NULL DEFAULT '1',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
