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
import { topodata, vtadmin as pb } from '../../../proto/vtadmin';
import { formatRows } from './KeyspaceShards';

describe('KeyspaceShards', () => {
    describe('formatRows', () => {
        const tests: {
            name: string;
            params: Parameters<typeof formatRows>;
            expected: ReturnType<typeof formatRows>;
        }[] = [
            {
                name: 'should handle when keyspace is null',
                params: [null, [], null],
                expected: [],
            },
            {
                name: 'should filter across shards and tablets',
                params: [
                    pb.Keyspace.create({
                        cluster: {
                            id: 'cluster1',
                        },
                        keyspace: {
                            name: 'keyspace1',
                        },
                        shards: {
                            '-80': {
                                name: '-80',
                                shard: {
                                    is_master_serving: false,
                                },
                            },
                            '80-': {
                                name: '80-',
                                shard: {
                                    is_master_serving: true,
                                },
                            },
                        },
                    }),
                    [
                        pb.Tablet.create({
                            cluster: { id: 'cluster1' },
                            state: pb.Tablet.ServingState.SERVING,
                            tablet: {
                                alias: {
                                    cell: 'zone1',
                                    uid: 100,
                                },
                                keyspace: 'keyspace1',
                                shard: '-80',
                                type: topodata.TabletType.MASTER,
                            },
                        }),
                        pb.Tablet.create({
                            cluster: { id: 'cluster1' },
                            state: pb.Tablet.ServingState.SERVING,
                            tablet: {
                                alias: {
                                    cell: 'zone1',
                                    uid: 200,
                                },
                                keyspace: 'keyspace2',
                                shard: '-',
                                type: topodata.TabletType.MASTER,
                            },
                        }),
                        pb.Tablet.create({
                            cluster: { id: 'cluster1' },
                            state: pb.Tablet.ServingState.NOT_SERVING,
                            tablet: {
                                alias: {
                                    cell: 'zone1',
                                    uid: 300,
                                },
                                hostname: 'tablet-zone1-300-80-00',
                                keyspace: 'keyspace1',
                                shard: '80-',
                                type: topodata.TabletType.MASTER,
                            },
                        }),
                    ],
                    'not',
                ],
                expected: [
                    {
                        // -80 matches the filter string "not" because shardState is "NOT_SERVING'",
                        // however all of its tablets are serving and therefore none of them match.
                        isShardServing: false,
                        shard: '-80',
                        shardState: 'NOT_SERVING',
                        tablets: [],
                    },
                    {
                        // 80- (the shard itself) does not match the filter string, since
                        // its shardState is "SERVING'. However, its non-serving tablet
                        // does match the filter string, so we include it.
                        isShardServing: true,
                        shard: '80-',
                        shardState: 'SERVING',
                        tablets: [
                            {
                                alias: 'zone1-300',
                                hostname: 'tablet-zone1-300-80-00',
                                shard: '80-',
                                tabletState: 'NOT_SERVING',
                                tabletType: 'PRIMARY',
                                _tabletStateEnum: pb.Tablet.ServingState.NOT_SERVING,
                            },
                        ],
                    },
                ],
            },
        ];

        test.each(tests.map(Object.values))(
            '%s',
            (name: string, params: Parameters<typeof formatRows>, expected: ReturnType<typeof formatRows>) => {
                const result = formatRows(...params);
                expect(result).toMatchObject(expected);
            }
        );
    });
});
