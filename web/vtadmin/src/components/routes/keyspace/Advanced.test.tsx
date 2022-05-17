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
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from 'react-query';
import { Advanced } from './Advanced';

const ORIGINAL_PROCESS_ENV = process.env;
const TEST_PROCESS_ENV = {
    ...process.env,
    REACT_APP_VTADMIN_API_ADDRESS: '',
};

describe('Advanced keyspace actions', () => {
    const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
    });

    beforeAll(() => {
        process.env = { ...TEST_PROCESS_ENV } as NodeJS.ProcessEnv;
        jest.spyOn(global, 'fetch');
    });

    beforeEach(() => {
        process.env = { ...TEST_PROCESS_ENV } as NodeJS.ProcessEnv;
        jest.clearAllMocks();
    });

    afterAll(() => {
        process.env = { ...ORIGINAL_PROCESS_ENV };
    });

    describe('Reload Schema', () => {
        it('reloads the schema', async () => {
            render(
                <QueryClientProvider client={queryClient}>
                    <Advanced clusterID="some-cluster" name="some-keyspace" />
                </QueryClientProvider>
            );

            const container = screen.getByTitle('Reload Schema');
            const button = within(container).getByRole('button');
            expect(button).not.toHaveAttribute('disabled');

            const user = userEvent.setup();
            user.click(button);

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
