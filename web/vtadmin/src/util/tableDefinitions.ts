/**
 * Copyright 2021 The Vitess Authors.
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
import { vtadmin as pb } from '../proto/vtadmin';

export interface TableDefinition {
    cluster?: pb.Schema['cluster'];
    keyspace?: pb.Schema['keyspace'];
    // The [0] index is a typescript quirk to infer the type of
    // an entry in an array, and therefore the type of ALL entries
    // in the array (not just the first one).
    tableDefinition?: pb.Schema['table_definitions'][0];
    tableSize?: pb.Schema['table_sizes'][0];
}

/**
 * getTableDefinitions is a helper function for transforming an array of Schemas
 * into a flat array of table definitions.
 */
export const getTableDefinitions = (schemas: pb.Schema[] | null | undefined): TableDefinition[] => {
    return (schemas || []).reduce((acc: TableDefinition[], schema: pb.Schema) => {
        // Index table definitions in this Schema by name, since we necessarily loop twice
        const sts: { [tableName: string]: TableDefinition } = {};

        (schema.table_definitions || []).forEach((td) => {
            if (!td.name) return;
            sts[td.name] = {
                cluster: schema.cluster,
                keyspace: schema.keyspace,
                tableDefinition: td,
            };
        });

        Object.entries(schema.table_sizes || {}).forEach(([tableName, tableSize]) => {
            // Include tables that have size/rows defined but do not have a table definition.
            if (!(tableName in sts)) {
                sts[tableName] = {
                    cluster: schema.cluster,
                    keyspace: schema.keyspace,
                    tableDefinition: { name: tableName },
                };
            }

            sts[tableName].tableSize = tableSize;
        });

        return acc.concat(Object.values(sts));
    }, []);
};
