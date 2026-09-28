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
import { getTableDefinitions, TableDefinition } from './tableDefinitions';

describe('getTableDefinitions', () => {
    const tests: {
        name: string;
        input: pb.Schema[] | null | undefined;
        expected: TableDefinition[];
    }[] = [
        {
            name: 'handles empty arrays',
            input: [],
            expected: [],
        },
        {
            name: 'handles undefined input',
            input: undefined,
            expected: [],
        },
        {
            name: 'handles null input',
            input: null,
            expected: [],
        },
        {
            name: 'extracts table definitions and sizes',
            input: [
                pb.Schema.create({
                    cluster: { id: 'c1', name: 'cluster1' },
                    keyspace: 'fauna',
                    table_definitions: [{ name: 'cats' }, { name: 'dogs' }],
                    table_sizes: {
                        cats: { row_count: 1234, data_length: 4321 },
                        dogs: { row_count: 5678, data_length: 8765 },
                    },
                }),
                pb.Schema.create({
                    cluster: { id: 'c2', name: 'cluster2' },
                    keyspace: 'flora',
                    table_definitions: [{ name: 'trees' }, { name: 'flowers' }],
                    table_sizes: {
                        flowers: { row_count: 1234, data_length: 4321 },
                        trees: { row_count: 5678, data_length: 8765 },
                    },
                }),
            ],
            expected: [
                {
                    cluster: { id: 'c1', name: 'cluster1' },
                    keyspace: 'fauna',
                    tableDefinition: { name: 'cats' },
                    tableSize: { row_count: 1234, data_length: 4321 },
                },
                {
                    cluster: { id: 'c1', name: 'cluster1' },
                    keyspace: 'fauna',
                    tableDefinition: { name: 'dogs' },
                    tableSize: { row_count: 5678, data_length: 8765 },
                },
                {
                    cluster: { id: 'c2', name: 'cluster2' },
                    keyspace: 'flora',
                    tableDefinition: { name: 'trees' },
                    tableSize: { row_count: 5678, data_length: 8765 },
                },
                {
                    cluster: { id: 'c2', name: 'cluster2' },
                    keyspace: 'flora',
                    tableDefinition: { name: 'flowers' },
                    tableSize: { row_count: 1234, data_length: 4321 },
                },
            ],
        },
        {
            name: 'handles when a table has a definition but no defined size',
            input: [
                pb.Schema.create({
                    cluster: { id: 'c1', name: 'cluster1' },
                    keyspace: 'fauna',
                    table_definitions: [{ name: 'cats' }],
                }),
            ],
            expected: [
                {
                    cluster: { id: 'c1', name: 'cluster1' },
                    keyspace: 'fauna',
                    tableDefinition: { name: 'cats' },
                },
            ],
        },
        {
            name: 'handles when a table defines sizes but not a definition',
            input: [
                pb.Schema.create({
                    cluster: { id: 'c1', name: 'cluster1' },
                    keyspace: 'fauna',
                    table_sizes: {
                        cats: { row_count: 1234, data_length: 4321 },
                    },
                }),
            ],
            expected: [
                {
                    cluster: { id: 'c1', name: 'cluster1' },
                    keyspace: 'fauna',
                    tableDefinition: { name: 'cats' },
                    tableSize: { row_count: 1234, data_length: 4321 },
                },
            ],
        },
    ];

    test.each(tests.map(Object.values))('%s', (name: string, input: pb.Schema[], expected: TableDefinition[]) => {
        const result = getTableDefinitions(input);
        expect(result).toEqual(expected);
    });
});
