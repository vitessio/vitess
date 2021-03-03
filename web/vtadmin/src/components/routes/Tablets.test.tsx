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
import { vtadmin as pb, topodata } from '../../proto/vtadmin';
import { formatRows } from './Tablets';

describe('Tablets', () => {
    describe('filterRows', () => {
        const tests: {
            name: string;
            filter: string | null;
            tablets: pb.Tablet[] | null;
            // Undefined `expected` keys will be ignored.
            // Unfortunately, Partial<ReturnType<typeof formatRows>> does not
            // work as expected + requires all property keys to be defined.
            expected: { [k: string]: unknown }[];
        }[] = [
            {
                name: 'empty tablets',
                filter: null,
                tablets: null,
                expected: [],
            },
            {
                name: 'sort by primary first, then other tablet types alphabetically',
                filter: null,
                tablets: [
                    pb.Tablet.create({
                        tablet: {
                            alias: {
                                cell: 'cell1',
                                uid: 4,
                            },
                            type: topodata.TabletType.BACKUP,
                        },
                    }),
                    pb.Tablet.create({
                        tablet: {
                            alias: {
                                cell: 'cell1',
                                uid: 2,
                            },
                            type: topodata.TabletType.REPLICA,
                        },
                    }),
                    pb.Tablet.create({
                        tablet: {
                            alias: {
                                cell: 'cell1',
                                uid: 3,
                            },
                            type: topodata.TabletType.MASTER,
                        },
                    }),
                    pb.Tablet.create({
                        tablet: {
                            alias: {
                                cell: 'cell1',
                                uid: 1,
                            },
                            type: topodata.TabletType.REPLICA,
                        },
                    }),
                ],
                expected: [
                    { alias: 'cell1-3', type: 'PRIMARY' },
                    { alias: 'cell1-4', type: 'BACKUP' },
                    { alias: 'cell1-1', type: 'REPLICA' },
                    { alias: 'cell1-2', type: 'REPLICA' },
                ],
            },
        ];

        test.each(tests.map(Object.values))(
            '%s',
            (name: string, filter: string, tablets: pb.Tablet[], expected: { [k: string]: unknown }[]) => {
                const result = formatRows(tablets, filter);
                expect(result).toMatchObject(expected);
            }
        );
    });
});
