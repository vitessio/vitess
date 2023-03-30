/**
 * Copyright 2022 The Vitess Authors.
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

import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { createMemoryHistory } from 'history';
import { merge } from 'lodash-es';
import React from 'react';
import { QueryClient, QueryClientProvider } from 'react-query';
import { Router } from 'react-router';

import { topodata, vtadmin } from '../../../proto/vtadmin';
import { formatAlias } from '../../../util/tablets';
import Advanced from './Advanced';
import { describe, it, expect, vi } from 'vitest';

const makeTablet = (overrides: Partial<vtadmin.ITablet> = {}): vtadmin.Tablet => {
    const defaults: vtadmin.ITablet = {
        cluster: { id: 'some-cluster-id', name: 'some-cluster-name' },
        state: vtadmin.Tablet.ServingState.SERVING,
        tablet: {
            alias: {
                cell: 'zone1',
                uid: 101,
            },
            type: topodata.TabletType.REPLICA,
        },
    };
    return vtadmin.Tablet.create(merge(defaults, overrides));
};

const makePrimaryTablet = (overrides: Partial<vtadmin.ITablet> = {}): vtadmin.Tablet => {
    return makeTablet({
        ...overrides,
        tablet: {
            type: topodata.TabletType.PRIMARY,
        },
    });
};

const renderHelper = (children: React.ReactNode) => {
    const history = createMemoryHistory();

    const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
    });

    render(
        <Router history={history}>
            <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
        </Router>
    );
};

const ORIGINAL_PROCESS_ENV = import.meta.env;
const TEST_PROCESS_ENV = {
    ...import.meta.env,
    VITE_VTADMIN_API_ADDRESS: '',
};

describe('Advanced', () => {
    beforeAll(() => {
        import.meta.env = { ...TEST_PROCESS_ENV } as NodeJS.ProcessEnv;
        vi.spyOn(global, 'fetch');
    });

    beforeEach(() => {
        import.meta.env = { ...TEST_PROCESS_ENV } as NodeJS.ProcessEnv;
        vi.clearAllMocks();
    });

    afterAll(() => {
        import.meta.env = { ...ORIGINAL_PROCESS_ENV };
    });

    describe('Advanced tablet actions', () => {
        describe('Start Replication', () => {
            it('starts replication', async () => {
                const tablet = makeTablet();
                const alias = formatAlias(tablet.tablet?.alias) as string;
                renderHelper(<Advanced alias={alias} clusterID={tablet.cluster?.id as string} tablet={tablet} />);

                const container = screen.getByTitle('Start Replication');
                const button = within(container).getByRole('button');

                // This action does not require confirmation
                const input = within(container).queryByRole('textbox');
                expect(input).toBeNull();

                expect(button).not.toHaveAttribute('disabled');

                fireEvent.click(button);

                await waitFor(() => {
                    expect(global.fetch).toHaveBeenCalledTimes(1);
                });

                expect(global.fetch).toHaveBeenCalledWith(
                    `/api/tablet/${alias}/start_replication?cluster=some-cluster-id`,
                    {
                        credentials: undefined,
                        method: 'put',
                    }
                );
            });

            it('prevents starting replication if primary', () => {
                const tablet = makePrimaryTablet();
                renderHelper(
                    <Advanced
                        alias={formatAlias(tablet.tablet?.alias) as string}
                        clusterID={tablet.cluster?.id as string}
                        tablet={tablet}
                    />
                );

                const container = screen.getByTitle('Start Replication');
                const button = within(container).getByRole('button');
                expect(button).toHaveAttribute('disabled');
            });
        });

        describe('Stop Replication', () => {
            it('stops replication', async () => {
                const tablet = makeTablet();
                const alias = formatAlias(tablet.tablet?.alias) as string;
                renderHelper(<Advanced alias={alias} clusterID={tablet.cluster?.id as string} tablet={tablet} />);

                const container = screen.getByTitle('Stop Replication');
                const button = within(container).getByRole('button');

                // This action does not require confirmation
                const input = within(container).queryByRole('textbox');
                expect(input).toBeNull();

                expect(button).not.toHaveAttribute('disabled');

                fireEvent.click(button);

                await waitFor(() => {
                    expect(global.fetch).toHaveBeenCalledTimes(1);
                });

                expect(global.fetch).toHaveBeenCalledWith(
                    `/api/tablet/${alias}/stop_replication?cluster=some-cluster-id`,
                    {
                        credentials: undefined,
                        method: 'put',
                    }
                );
            });

            it('prevents stopping replication if primary', () => {
                const tablet = makePrimaryTablet();
                renderHelper(
                    <Advanced
                        alias={formatAlias(tablet.tablet?.alias) as string}
                        clusterID={tablet.cluster?.id as string}
                        tablet={tablet}
                    />
                );

                const container = screen.getByTitle('Stop Replication');
                const button = within(container).getByRole('button');
                expect(button).toHaveAttribute('disabled');
            });
        });

        describe('Refresh Replication Source', () => {
            it('refreshes replication source', async () => {
                const tablet = makeTablet();
                const alias = formatAlias(tablet.tablet?.alias) as string;
                renderHelper(<Advanced alias={alias} clusterID={tablet.cluster?.id as string} tablet={tablet} />);

                const container = screen.getByTitle('Refresh Replication Source');
                const button = within(container).getByRole('button');

                // This action does not require confirmation
                const input = within(container).queryByRole('textbox');
                expect(input).toBeNull();

                expect(button).not.toHaveAttribute('disabled');

                fireEvent.click(button);

                await waitFor(() => {
                    expect(global.fetch).toHaveBeenCalledTimes(1);
                });

                expect(global.fetch).toHaveBeenCalledWith(`/api/tablet/${alias}/refresh_replication_source`, {
                    credentials: undefined,
                    method: 'put',
                });
            });

            it('prevents refreshing if primary', () => {
                const tablet = makePrimaryTablet();
                renderHelper(
                    <Advanced
                        alias={formatAlias(tablet.tablet?.alias) as string}
                        clusterID={tablet.cluster?.id as string}
                        tablet={tablet}
                    />
                );

                const container = screen.getByTitle('Refresh Replication Source');
                const button = within(container).getByRole('button');
                expect(button).toHaveAttribute('disabled');
            });
        });

        describe('Delete', () => {
            it('deletes the tablet', async () => {
                const tablet = makeTablet();
                renderHelper(
                    <Advanced
                        alias={formatAlias(tablet.tablet?.alias) as string}
                        clusterID={tablet.cluster?.id as string}
                        tablet={tablet}
                    />
                );

                const container = screen.getByTitle('Delete Tablet');
                const button = within(container).getByRole('button');
                const input = within(container).getByRole('textbox');

                expect(button).toHaveAttribute('disabled');

                fireEvent.change(input, { target: { value: 'zone1-101' } });
                expect(button).not.toHaveAttribute('disabled');

                fireEvent.click(button);

                await waitFor(() => {
                    expect(global.fetch).toHaveBeenCalledTimes(1);
                });

                expect(global.fetch).toHaveBeenCalledWith('/api/tablet/zone1-101?cluster=some-cluster-id', {
                    credentials: undefined,
                    method: 'delete',
                });
            });

            it('deletes the tablet with allow_primary=true if primary', async () => {
                const tablet = makePrimaryTablet();
                renderHelper(
                    <Advanced
                        alias={formatAlias(tablet.tablet?.alias) as string}
                        clusterID={tablet.cluster?.id as string}
                        tablet={tablet}
                    />
                );

                const container = screen.getByTitle('Delete Tablet');
                const button = within(container).getByRole('button');
                const input = within(container).getByRole('textbox');

                expect(button).toHaveAttribute('disabled');

                fireEvent.change(input, { target: { value: 'zone1-101' } });
                expect(button).not.toHaveAttribute('disabled');

                fireEvent.click(button);

                await waitFor(() => {
                    expect(global.fetch).toHaveBeenCalledTimes(1);
                });

                expect(global.fetch).toHaveBeenCalledWith(
                    '/api/tablet/zone1-101?cluster=some-cluster-id&allow_primary=true',
                    {
                        credentials: undefined,
                        method: 'delete',
                    }
                );
            });
        });
    });
});
