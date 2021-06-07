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
import { getShardsByState, ShardsByState, ShardState } from './keyspaces';
import { vtadmin as pb } from '../proto/vtadmin';

describe('getShardsByState', () => {
    const tests: {
        name: string;
        keyspace: pb.Keyspace | null | undefined;
        expected: ShardsByState;
    }[] = [
        {
            name: 'should return shards by state',
            keyspace: pb.Keyspace.create({
                shards: {
                    '-80': {
                        name: '-80',
                        shard: { is_master_serving: true },
                    },
                    '-': {
                        name: '-',
                        shard: { is_master_serving: false },
                    },
                    '80-': {
                        name: '80-',
                        shard: { is_master_serving: true },
                    },
                },
            }),
            expected: {
                [ShardState.serving]: [
                    {
                        name: '-80',
                        shard: { is_master_serving: true },
                    },
                    {
                        name: '80-',
                        shard: { is_master_serving: true },
                    },
                ],
                [ShardState.nonserving]: [
                    {
                        name: '-',
                        shard: { is_master_serving: false },
                    },
                ],
            },
        },
        {
            name: 'should handle shard states without any shards',
            keyspace: pb.Keyspace.create({
                shards: {
                    '-': {
                        name: '-',
                        shard: { is_master_serving: true },
                    },
                },
            }),
            expected: {
                [ShardState.serving]: [
                    {
                        name: '-',
                        shard: { is_master_serving: true },
                    },
                ],
                [ShardState.nonserving]: [],
            },
        },
        {
            name: 'should handle keyspaces without any shards',
            keyspace: pb.Keyspace.create(),
            expected: {
                [ShardState.serving]: [],
                [ShardState.nonserving]: [],
            },
        },
        {
            name: 'should handle empty input',
            keyspace: null,
            expected: {
                [ShardState.serving]: [],
                [ShardState.nonserving]: [],
            },
        },
    ];

    test.each(tests.map(Object.values))('%s', (name: string, keyspace: pb.Keyspace, expected: ShardsByState) => {
        const result = getShardsByState(keyspace);
        expect(result).toEqual(expected);
    });
});
