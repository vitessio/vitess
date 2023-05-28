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
import * as vs from './vschemas';
import { describe, expect } from 'vitest';

describe('getVindexesForTable', () => {
    const tests: {
        name: string;
        input: Parameters<typeof vs.getVindexesForTable>;
        expected: ReturnType<typeof vs.getVindexesForTable>;
    }[] = [
        {
            name: 'should return column vindexes',
            input: [
                pb.VSchema.create({
                    v_schema: {
                        tables: {
                            customer: {
                                column_vindexes: [{ column: 'customer_id', name: 'hash' }],
                            },
                        },
                        vindexes: {
                            hash: { type: 'hash' },
                        },
                    },
                }),
                'customer',
            ],
            expected: [
                {
                    column: 'customer_id',
                    name: 'hash',
                    meta: { type: 'hash' },
                },
            ],
        },
        {
            name: 'should return column vindexes + metadata',
            input: [
                pb.VSchema.create({
                    v_schema: {
                        tables: {
                            dogs: {
                                column_vindexes: [
                                    { column: 'id', name: 'hash' },
                                    { name: 'dogs_domain_vdx', columns: ['domain', 'is_good_dog'] },
                                ],
                            },
                        },
                        vindexes: {
                            hash: { type: 'hash' },
                            dogs_domain_vdx: {
                                type: 'lookup_hash',
                                owner: 'dogs',
                                params: {
                                    from: 'domain,is_good_dog',
                                    table: 'dogs_domain_idx',
                                    to: 'id',
                                },
                            },
                        },
                    },
                }),
                'dogs',
            ],
            expected: [
                {
                    column: 'id',
                    name: 'hash',
                    meta: { type: 'hash' },
                },
                {
                    columns: ['domain', 'is_good_dog'],
                    name: 'dogs_domain_vdx',
                    meta: {
                        owner: 'dogs',
                        params: {
                            from: 'domain,is_good_dog',
                            table: 'dogs_domain_idx',
                            to: 'id',
                        },
                        type: 'lookup_hash',
                    },
                },
            ],
        },
        {
            name: 'should handle vschemas where the given table is not defined',
            input: [
                pb.VSchema.create({
                    v_schema: {
                        tables: {
                            customer: {
                                column_vindexes: [{ column: 'customer_id', name: 'hash' }],
                            },
                        },
                        vindexes: {
                            hash: { type: 'hash' },
                        },
                    },
                }),
                'does-not-exist',
            ],
            expected: [],
        },
    ];

    test.each(tests.map(Object.values))(
        '%s',
        (
            name: string,
            input: Parameters<typeof vs.getVindexesForTable>,
            expected: ReturnType<typeof vs.getVindexesForTable>
        ) => {
            const result = vs.getVindexesForTable(...input);
            expect(result).toEqual(expected);
        }
    );
});
