/**
 * Copyright 2024 The Vitess Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { invertBy } from 'lodash-es';
import { vtctldata } from '../proto/vtadmin';

/**
 * SCHEMA_MIGRATION_STATUS maps numeric schema migration status back to human readable strings.
 */
export const SCHEMA_MIGRATION_STATUS = Object.entries(invertBy(vtctldata.SchemaMigration.Status)).reduce(
    (acc, [k, vs]) => {
        acc[k] = vs[0];
        return acc;
    },
    {} as { [k: string]: string }
);

export const formatSchemaMigrationStatus = (schemaMigration: vtctldata.ISchemaMigration) =>
    schemaMigration.status && SCHEMA_MIGRATION_STATUS[schemaMigration.status];
