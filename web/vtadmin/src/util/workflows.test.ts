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
import { getStream, getStreams, getStreamTablets } from './workflows';
import { describe, expect } from 'vitest';

describe('getStreams', () => {
    const tests: {
        name: string;
        input: Parameters<typeof getStreams>;
        expected: ReturnType<typeof getStreams>;
    }[] = [
        {
            name: 'should return a flat list of streams',
            input: [
                pb.Workflow.create({
                    workflow: {
                        shard_streams: {
                            '-80/us_east_1a-123456': {
                                streams: [
                                    { id: 1, shard: '-80' },
                                    { id: 2, shard: '-80' },
                                ],
                            },
                            '80-/us_east_1a-789012': {
                                streams: [
                                    { id: 1, shard: '80-' },
                                    { id: 2, shard: '80-' },
                                ],
                            },
                        },
                    },
                }),
            ],
            expected: [
                { id: 1, shard: '-80' },
                { id: 2, shard: '-80' },
                { id: 1, shard: '80-' },
                { id: 2, shard: '80-' },
            ],
        },
        {
            name: 'should handle when shard streams undefined',
            input: [pb.Workflow.create()],
            expected: [],
        },
        {
            name: 'should handle null input',
            input: [null],
            expected: [],
        },
    ];

    test.each(tests.map(Object.values))(
        '%s',
        (name: string, input: Parameters<typeof getStreams>, expected: ReturnType<typeof getStreams>) => {
            expect(getStreams(...input)).toEqual(expected);
        }
    );
});

describe('getStream', () => {
    const tests: {
        name: string;
        input: Parameters<typeof getStream>;
        expected: ReturnType<typeof getStream>;
    }[] = [
        {
            name: 'should return the stream for the streamKey',
            input: [
                pb.Workflow.create({
                    workflow: {
                        shard_streams: {
                            '-80/us_east_1a-123456': {
                                streams: [
                                    {
                                        id: 1,
                                        shard: '-80',
                                        tablet: {
                                            cell: 'us_east_1a',
                                            uid: 123456,
                                        },
                                    },
                                    {
                                        id: 2,
                                        shard: '-80',
                                        tablet: {
                                            cell: 'us_east_1a',
                                            uid: 123456,
                                        },
                                    },
                                ],
                            },
                            '-80/us_east_1a-789012': {
                                streams: [
                                    {
                                        id: 1,
                                        shard: '-80',
                                        tablet: {
                                            cell: 'us_east_1a',
                                            uid: 789012,
                                        },
                                    },
                                ],
                            },
                        },
                    },
                }),
                'us_east_1a-123456/2',
            ],
            expected: {
                id: 2,
                shard: '-80',
                tablet: {
                    cell: 'us_east_1a',
                    uid: 123456,
                },
            },
        },
        {
            name: 'should handle no matching stream in workflow',
            input: [
                pb.Workflow.create({
                    workflow: {
                        shard_streams: {
                            '-80/us_east_1a-123456': {
                                streams: [
                                    {
                                        id: 1,
                                        shard: '-80',
                                        tablet: {
                                            cell: 'us_east_1a',
                                            uid: 123456,
                                        },
                                    },
                                ],
                            },
                        },
                    },
                }),
                'us_east_1a-123456/2',
            ],
            expected: undefined,
        },
        {
            name: 'should handle undefined streamKey',
            input: [
                pb.Workflow.create({
                    workflow: {
                        shard_streams: {
                            '-80/us_east_1a-123456': {
                                streams: [
                                    {
                                        id: 1,
                                        shard: '-80',
                                        tablet: {
                                            cell: 'us_east_1a',
                                            uid: 123456,
                                        },
                                    },
                                ],
                            },
                        },
                    },
                }),
                undefined,
            ],
            expected: undefined,
        },
        {
            name: 'should handle undefined workflow',
            input: [undefined, 'us_east_1a-123456/1'],
            expected: undefined,
        },
    ];

    test.each(tests.map(Object.values))(
        '%s',
        (name: string, input: Parameters<typeof getStream>, expected: ReturnType<typeof getStream>) => {
            expect(getStream(...input)).toEqual(expected);
        }
    );
});

describe('getStreamTablets', () => {
    const tests: {
        name: string;
        input: Parameters<typeof getStreamTablets>;
        expected: ReturnType<typeof getStreamTablets>;
    }[] = [
        {
            name: 'should return a set of unique tablet aliases',
            input: [
                pb.Workflow.create({
                    workflow: {
                        shard_streams: {
                            '-80/us_east_1a-123456': {
                                streams: [
                                    { id: 1, shard: '-80', tablet: { cell: 'us_east_1a', uid: 123456 } },
                                    { id: 2, shard: '-80', tablet: { cell: 'us_east_1a', uid: 123456 } },
                                ],
                            },
                            '80-/us_east_1a-789012': {
                                streams: [{ id: 1, shard: '80-', tablet: { cell: 'us_east_1a', uid: 789012 } }],
                            },
                        },
                    },
                }),
            ],
            expected: ['us_east_1a-123456', 'us_east_1a-789012'],
        },
        {
            name: 'should handle empty workflow',
            input: [pb.Workflow.create()],
            expected: [],
        },
        {
            name: 'should handle null input',
            input: [null],
            expected: [],
        },
    ];

    test.each(tests.map(Object.values))(
        '%s',
        (name: string, input: Parameters<typeof getStreamTablets>, expected: ReturnType<typeof getStreamTablets>) => {
            expect(getStreamTablets(...input)).toEqual(expected);
        }
    );
});
