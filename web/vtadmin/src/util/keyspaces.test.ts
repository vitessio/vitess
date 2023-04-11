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
import { getShardsByState, getShardSortRange, ShardRange, ShardsByState, ShardState } from './keyspaces';
import { vtadmin as pb } from '../proto/vtadmin';
import { describe, it, expect } from 'vitest';

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
                        shard: { is_primary_serving: true },
                    },
                    '-': {
                        name: '-',
                        shard: { is_primary_serving: false },
                    },
                    '80-': {
                        name: '80-',
                        shard: { is_primary_serving: true },
                    },
                },
            }),
            expected: {
                [ShardState.serving]: [
                    {
                        name: '-80',
                        shard: { is_primary_serving: true },
                    },
                    {
                        name: '80-',
                        shard: { is_primary_serving: true },
                    },
                ],
                [ShardState.nonserving]: [
                    {
                        name: '-',
                        shard: { is_primary_serving: false },
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
                        shard: { is_primary_serving: true },
                    },
                },
            }),
            expected: {
                [ShardState.serving]: [
                    {
                        name: '-',
                        shard: { is_primary_serving: true },
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

describe('getShardSortRange', () => {
    const tests: {
        shardName: string;
        expected: ShardRange;
    }[] = [
        {
            shardName: '0',
            expected: { start: Number.MIN_VALUE, end: Number.MAX_VALUE },
        },
        {
            shardName: '-',
            expected: { start: Number.MIN_VALUE, end: Number.MAX_VALUE },
        },
        {
            shardName: '-40',
            expected: { start: Number.MIN_VALUE, end: 64 },
        },
        {
            shardName: '40-80',
            expected: { start: 64, end: 128 },
        },
        {
            shardName: '80-c0',
            expected: { start: 128, end: 192 },
        },
        {
            shardName: 'c0-',
            expected: { start: 192, end: Number.MAX_VALUE },
        },
        {
            shardName: 'c0-',
            expected: { start: 192, end: Number.MAX_VALUE },
        },
    ];

    test.each(tests.map(Object.values))('%s', (shardName: string, expected: ShardRange) => {
        const result = getShardSortRange(shardName);
        expect(result).toEqual(expected);
    });

    it('handles invalid shard names', () => {
        ['nope', '--', '', '40--'].forEach((s) => {
            expect(() => {
                getShardSortRange(s);
            }).toThrow(`could not parse sortable range from shard ${s}`);
        });
    });
});
