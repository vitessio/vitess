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

import { UseQueryResult } from 'react-query';
import { TabletDebugVarsResponse } from '../../api/http';
import { vtadmin as pb } from '../../proto/vtadmin';
import { formatSeries } from './WorkflowStreamsLagChart';

describe('WorkflowStreamsLagChart', () => {
    describe('formatSeries', () => {
        it('should return series for all streams in the workflow', () => {
            const workflow = pb.Workflow.create({
                cluster: {
                    id: 'zone1',
                    name: 'zone1',
                },
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
            });

            const queries: Partial<UseQueryResult<TabletDebugVarsResponse, Error>>[] = [
                {
                    data: {
                        params: { alias: 'us_east_1a-123456', clusterID: 'zone1' },
                        data: {
                            VReplicationLag: {
                                All: [3, 3, 3],
                                '1': [1, 1, 1],
                                '2': [2, 2, 2],
                            },
                        },
                    },
                    dataUpdatedAt: 1000000000000,
                },
                {
                    data: {
                        params: { alias: 'us_east_1a-789012', clusterID: 'zone1' },
                        data: {
                            VReplicationLag: {
                                All: [],
                                '1': [1, 1, 1],
                                // Some other stream running on the tablet that isn't part
                                // of this workflow.
                                '2': [2, 2, 2],
                            },
                        },
                    },
                    dataUpdatedAt: 1000000000000,
                },
            ];

            // A sneaky cast to UseQueryResult since otherwise enumerating the many fields
            // UseQueryResult (most of which we don't use) is pointlessly verbose.
            const result = formatSeries(workflow, queries as UseQueryResult<TabletDebugVarsResponse, Error>[]);

            // Use snapshot matching since defining expected values for arrays of 180 data points is... annoying.
            expect(result).toMatchSnapshot();

            // ...but! Add additional validation so that failing tests are easier to debug.
            // (And because it can be tempting to not examine snapshot changes in detail...) :)
            expect(result.length).toEqual(3);

            expect(result[0].name).toEqual('us_east_1a-123456/1');
            expect(result[1].name).toEqual('us_east_1a-123456/2');
            expect(result[2].name).toEqual('us_east_1a-789012/1');
        });

        it('should handle empty input', () => {
            const result = formatSeries(null, []);
            expect(result).toEqual([]);
        });
    });
});
