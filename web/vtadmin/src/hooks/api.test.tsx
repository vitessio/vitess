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
import React from 'react';
import { QueryClient, QueryClientProvider } from 'react-query';
import { renderHook } from '@testing-library/react-hooks';

import * as api from './api';
import * as httpAPI from '../api/http';
import { vtadmin as pb } from '../proto/vtadmin';

jest.mock('../api/http');

describe('useWorkflows', () => {
    const tests: {
        name: string;
        response: pb.GetWorkflowsResponse | undefined;
        expected: pb.Workflow[] | undefined;
    }[] = [
        {
            name: 'returns a flat list of workflows',
            response: pb.GetWorkflowsResponse.create({
                workflows_by_cluster: {
                    east: {
                        workflows: [
                            {
                                workflow: { name: 'one-goes-east' },
                            },
                        ],
                    },
                    west: {
                        workflows: [
                            {
                                workflow: { name: 'one-goes-west' },
                            },
                        ],
                    },
                },
            }),
            expected: [
                pb.Workflow.create({ workflow: { name: 'one-goes-east' } }),
                pb.Workflow.create({ workflow: { name: 'one-goes-west' } }),
            ],
        },
    ];

    const queryClient = new QueryClient();
    const wrapper: React.FunctionComponent = ({ children }) => (
        <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );

    test.each(tests.map(Object.values))(
        '%s',
        async (name: string, response: pb.GetWorkflowsResponse | undefined, expected: pb.Workflow[] | undefined) => {
            (httpAPI.fetchWorkflows as any).mockResolvedValueOnce(response);

            const { result, waitFor } = renderHook(() => api.useWorkflows(), { wrapper });

            // Check that our query helper handles when the query is still in flight
            expect(result.current.data).toBeUndefined();

            // "Wait" for the underlying fetch request to resolve (scare-quotes because,
            // in practice, we're not "waiting" for anything since the response is mocked.)
            await waitFor(() => result.current.isSuccess);

            expect(result.current.data).toEqual(expected);
        }
    );
});
