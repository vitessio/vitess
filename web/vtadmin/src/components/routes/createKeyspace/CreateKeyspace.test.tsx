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
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryHistory } from 'history';
import { QueryClient, QueryClientProvider } from 'react-query';
import { Router } from 'react-router-dom';

import { CreateKeyspace } from './CreateKeyspace';
import { vtadmin } from '../../../proto/vtadmin';
import * as Snackbar from '../../Snackbar';

const ORIGINAL_PROCESS_ENV = process.env;
const TEST_PROCESS_ENV = {
    ...process.env,
    REACT_APP_VTADMIN_API_ADDRESS: '',
};

// This integration test verifies the behaviour from the form UI
// all the way down to the network level (which we mock with msw).
// It's a very comprehensive test (good!), but does make some assumptions
// about UI structure (boo!), which means this test is rather brittle
// to UI changes (e.g., like how the Select works, adding new form fields, etc.)
describe('CreateKeyspace integration test', () => {
    const server = setupServer();

    beforeAll(() => {
        process.env = { ...TEST_PROCESS_ENV } as NodeJS.ProcessEnv;
    });

    afterEach(() => {
        process.env = { ...TEST_PROCESS_ENV } as NodeJS.ProcessEnv;
    });

    afterAll(() => {
        process.env = { ...ORIGINAL_PROCESS_ENV };
        server.close();
    });

    it('successfully creates a keyspace', async () => {
        jest.spyOn(global, 'fetch');
        jest.spyOn(Snackbar, 'success');

        const cluster = { id: 'local', name: 'local' };

        server.use(
            rest.get('/api/clusters', (req, res, ctx) => {
                return res(ctx.json({ result: { clusters: [cluster] }, ok: true }));
            }),
            rest.post('/api/keyspace/:clusterID', (req, res, ctx) => {
                const data: vtadmin.ICreateKeyspaceResponse = {
                    keyspace: {
                        cluster: { id: cluster.id, name: cluster.name },
                        keyspace: { name: 'some-keyspace' },
                    },
                };
                return res(ctx.json({ result: data, ok: true }));
            })
        );
        server.listen();

        const history = createMemoryHistory();
        jest.spyOn(history, 'push');

        const queryClient = new QueryClient({
            defaultOptions: { queries: { retry: false } },
        });

        // Finally, render the view
        render(
            <Router history={history}>
                <QueryClientProvider client={queryClient}>
                    <CreateKeyspace />
                </QueryClientProvider>
            </Router>
        );

        // Wait for initial queries to load. Given that the "initial queries" for this
        // page are presently only the call to GET /api/clusters, checking that the
        // Select is populated with the clusters from the response defined above is
        // sufficient, albeit clumsy. This will need to be reworked in a future where
        // we add more queries that fire on form load (e.g., to fetch all keyspaces.)
        await waitFor(() => {
            expect(screen.queryByTestId('select-empty')).toBeNull();
        });

        // Reset the fetch mock after the initial queries have completed so that
        // form submission assertions are easier.
        (global.fetch as any).mockClear();

        // From here on we can proceed with filling out the form fields.
        const user = userEvent.setup();
        await user.click(screen.getByText('local (local)'));
        await user.type(screen.getByLabelText('Keyspace Name'), 'some-keyspace');

        // Submit the form
        const submitButton = screen.getByText('Create Keyspace', {
            selector: 'button[type="submit"]',
        });
        await user.click(submitButton);

        // Assert that the client sent the correct API request
        expect(global.fetch).toHaveBeenCalledTimes(1);
        expect(global.fetch).toHaveBeenCalledWith('/api/keyspace/local', {
            credentials: undefined,
            body: JSON.stringify({
                name: 'some-keyspace',
            }),
            method: 'post',
        });

        // Validate form UI loading state, while the API request is "in flight"
        expect(submitButton).toHaveTextContent('Creating Keyspace...');
        expect(submitButton).toHaveAttribute('disabled');

        // Wait for the API request to complete
        await waitFor(() => {
            expect(submitButton).toHaveTextContent('Create Keyspace');
        });

        // Validate redirect to the new keyspace's detail page
        expect(history.push).toHaveBeenCalledTimes(1);
        expect(history.push).toHaveBeenCalledWith('/keyspace/local/some-keyspace');

        // Validate that snackbar was triggered
        expect(Snackbar.success).toHaveBeenCalledTimes(1);
        expect(Snackbar.success).toHaveBeenCalledWith('Created keyspace some-keyspace', { autoClose: 1600 });
    });
});
