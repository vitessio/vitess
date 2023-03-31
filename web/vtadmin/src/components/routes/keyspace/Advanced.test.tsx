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

import { rest } from 'msw';
import { setupServer } from 'msw/node';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from 'react-query';
import { Advanced } from './Advanced';
import { vtadmin } from '../../../proto/vtadmin';
import { describe, it, expect, vi } from 'vitest';
import { Response } from 'cross-fetch'

const ORIGINAL_PROCESS_ENV = process.env;
const TEST_PROCESS_ENV = {
    ...process.env,
    VITE_VTADMIN_API_ADDRESS: '',
};

describe('Advanced keyspace actions', () => {
    const keyspace: vtadmin.IKeyspace = {
        cluster: { id: 'some-cluster', name: 'some-cluster' },
        keyspace: {
            name: 'some-keyspace',
        },
    };

    const server = setupServer(
        rest.get('/api/keyspace/:clusterID/:keyspace', (req, res, ctx) => {
            return res(ctx.json({ ok: true, result: keyspace }));
        }),
        rest.put('/api/schemas/reload', (req, res, ctx) => {
            return res(ctx.json({ ok: true }));
        })
    );

    const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
    });

    beforeAll(() => {
        process.env = { ...TEST_PROCESS_ENV } as NodeJS.ProcessEnv;
        server.listen();
    });

    beforeEach(() => {
        process.env = { ...TEST_PROCESS_ENV } as NodeJS.ProcessEnv;
        vi.clearAllMocks();
    });

    afterAll(() => {
        process.env = { ...ORIGINAL_PROCESS_ENV };
        server.close();
    });

    describe('Reload Schema', () => {
        it('reloads the schema', async () => {
            const response: Promise<Response> = new Promise((resolve) => resolve(new Response('{"ok": "true", "result": {}}', { status: 200 })));
            vi.spyOn(global, 'fetch').mockReturnValue(response);


            render(
                <QueryClientProvider client={queryClient}>
                    <Advanced clusterID="some-cluster" name="some-keyspace" />
                </QueryClientProvider>
            );

            expect(screen.getByText('Loading...')).not.toBeNull();

            await waitFor(async () => {
                expect(screen.queryByText('Loading...')).toBeNull();
            });

            expect(global.fetch).toHaveBeenCalledTimes(1);
            expect(global.fetch).toHaveBeenCalledWith(`/api/keyspace/some-cluster/some-keyspace`, {
                credentials: undefined,
            });

            vi.clearAllMocks();

            const container = screen.getByTitle('Reload Schema');
            const button = within(container).getByRole('button');
            expect(button).not.toHaveAttribute('disabled');

            const user = userEvent.setup();
            await user.click(button);
            await waitFor(() => {
                expect(global.fetch).toHaveBeenCalledTimes(1);
            });

            expect(global.fetch).toHaveBeenCalledWith(
                `/api/schemas/reload?cluster=some-cluster&keyspace=some-keyspace`,
                {
                    credentials: undefined,
                    method: 'put',
                }
            );
        });
    });
});
