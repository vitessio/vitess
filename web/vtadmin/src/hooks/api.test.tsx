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
import { describe, it, expect, vi } from 'vitest';

vi.mock('../api/http');

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

describe('useWorkflow', () => {
    it('fetches data if no cached data exists', async () => {
        const queryClient = new QueryClient();
        const wrapper: React.FunctionComponent = ({ children }) => (
            <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
        );

        const fetchWorkflowResponse = pb.Workflow.create({
            cluster: { name: 'cluster1', id: 'cluster1' },
            keyspace: 'dogs',
            workflow: {
                name: 'one-goes-west',
                max_v_replication_lag: 0,
            },
        });

        (httpAPI.fetchWorkflow as any).mockResolvedValueOnce(fetchWorkflowResponse);

        const { result, waitFor } = renderHook(
            () =>
                api.useWorkflow({
                    clusterID: 'cluster1',
                    keyspace: 'dogs',
                    name: 'one-goes-west',
                }),
            { wrapper }
        );

        expect(result.current.data).toBeUndefined();

        await waitFor(() => result.current.isSuccess);
        expect(result.current.data).toEqual(fetchWorkflowResponse);
    });

    // This test corresponds to a common UI flow from a component that fetches all the workflows
    // to a component that fetches a single workflow.
    it('uses cached data as initialData', async () => {
        httpAPI.fetchWorkflow.mockReset()
        const queryClient = new QueryClient();
        const wrapper: React.FunctionComponent = ({ children }) => (
            <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
        );

        (httpAPI.fetchWorkflows as any).mockResolvedValueOnce(
            pb.GetWorkflowsResponse.create({
                workflows_by_cluster: {
                    cluster1: {
                        workflows: [
                            {
                                cluster: { name: 'cluster1', id: 'cluster1' },
                                keyspace: 'dogs',
                                workflow: {
                                    name: 'one-goes-east',
                                    max_v_replication_lag: 0,
                                },
                            },
                            {
                                cluster: { name: 'cluster1', id: 'cluster1' },
                                keyspace: 'dogs',
                                workflow: {
                                    name: 'one-goes-west',
                                    max_v_replication_lag: 0,
                                },
                            },
                        ],
                    },
                    cluster2: {
                        workflows: [
                            {
                                cluster: { name: 'cluster2', id: 'cluster2' },
                                keyspace: 'dogs',
                                workflow: {
                                    name: 'one-goes-west',
                                    max_v_replication_lag: 0,
                                },
                            },
                        ],
                    },
                },
            })
        );

        (httpAPI.fetchWorkflow as any).mockResolvedValueOnce(
            pb.Workflow.create({
                cluster: { name: 'cluster1', id: 'cluster1' },
                keyspace: 'dogs',
                workflow: {
                    name: 'one-goes-west',
                    max_v_replication_lag: 420,
                },
            })
        );

        // Execute a useWorkflows query to populate the query cache.
        // eslint-disable-next-line testing-library/render-result-naming-convention
        const useWorkflowsCall = renderHook(() => api.useWorkflows(), { wrapper });
        await useWorkflowsCall.waitFor(() => useWorkflowsCall.result.current.isSuccess);

        // Next, execute the useWorkflow query we *actually* want to inspect.
        const { result, waitFor } = renderHook(
            () =>
                api.useWorkflow(
                    {
                        clusterID: 'cluster1',
                        keyspace: 'dogs',
                        name: 'one-goes-west',
                    },
                    // Force the query to refetch
                    { staleTime: 0 }
                ),
            { wrapper }
        );

        // We expect the result to be successful, even though we've yet to resolve the /workflow API call,
        // since the workflow we want exists in the cache.
        expect(result.current.isSuccess).toBe(true);

        expect(result.current.data).toEqual(
            pb.Workflow.create({
                cluster: { name: 'cluster1', id: 'cluster1' },
                keyspace: 'dogs',
                workflow: {
                    name: 'one-goes-west',
                    max_v_replication_lag: 0,
                },
            })
        );

        // We _also_ check that a fetch request is in-flight to fetch updated data.
        expect(result.current.isFetching).toBe(true);
        expect(httpAPI.fetchWorkflow).toHaveBeenCalledTimes(1);

        // Then, we resolve the API call, with updated data (in this case max_v_replication_lag)
        // so we can check that the cache is updated.
        await waitFor(() => !result.current.isFetching && result.current.isSuccess);
        expect(result.current.data).toEqual(
            pb.Workflow.create({
                cluster: { name: 'cluster1', id: 'cluster1' },
                keyspace: 'dogs',
                workflow: {
                    name: 'one-goes-west',
                    max_v_replication_lag: 420,
                },
            })
        );
    });
});
