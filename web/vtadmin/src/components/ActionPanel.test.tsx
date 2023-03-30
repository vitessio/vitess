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

import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { rest } from 'msw';
import { setupServer } from 'msw/node';
import { QueryClient, QueryClientProvider, useMutation } from 'react-query';
import { describe, it, expect, beforeAll, vi } from 'vitest';


import ActionPanel, { ActionPanelProps } from './ActionPanel';

const ORIGINAL_PROCESS_ENV = process.env;
const TEST_PROCESS_ENV = {
    ...process.env,
    VITE_VTADMIN_API_ADDRESS: '',
};

describe('ActionPanel', () => {
    const server = setupServer(
        rest.post('/api/test', (req, res, ctx) => {
            return res(ctx.json({ ok: true }));
        })
    );

    const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
    });

    /**
     * The useMutation query hook must be defined in the body of a function
     * that is _within_ the context of a QueryClientProvider. This Wrapper component
     * provides such a function and should be `render`ed in the context QueryClientProvider.
     */
    const Wrapper: React.FC<Omit<ActionPanelProps, 'mutation'>> = (props) => {
        const mutation = useMutation(() => fetch('/api/test', { method: 'post' }));
        return <ActionPanel {...props} mutation={mutation as any} />;
    };

    beforeAll(() => {
        process.env = { ...TEST_PROCESS_ENV } as NodeJS.ProcessEnv;
        server.listen();
    });

    afterEach(() => {
        process.env = { ...TEST_PROCESS_ENV } as NodeJS.ProcessEnv;
        vi.clearAllMocks();
    });

    afterAll(() => {
        process.env = { ...ORIGINAL_PROCESS_ENV };
        server.close();
    });

    it('initiates the mutation', async () => {
        vi.spyOn(global, 'fetch');

        render(
            <QueryClientProvider client={queryClient}>
                <Wrapper
                    confirmationValue="zone1-101"
                    description="Do an action."
                    documentationLink="https://test.com"
                    loadedText="Do Action"
                    loadingText="Doing Action..."
                    title="A Title"
                />
            </QueryClientProvider>
        );

        const user = userEvent.setup();

        const button = screen.getByRole('button');
        const input = screen.getByRole('textbox');

        // Enter the confirmation text
        await user.type(input, 'zone1-101');
        expect(button).not.toHaveAttribute('disabled');

        await user.click(button);

        // Validate form while API request is in flight
        expect(button).toHaveTextContent('Doing Action...');

        expect(global.fetch).toHaveBeenCalledTimes(1);
        expect(global.fetch).toHaveBeenCalledWith('/api/test', { method: 'post' });

        // Wait for API request to complete
        await waitFor(() => expect(button).toHaveTextContent('Do Action'));
    });

    it('enables form submission if and only if input matches confirmation', async () => {
        render(
            <QueryClientProvider client={queryClient}>
                <Wrapper
                    confirmationValue="zone1-101"
                    description={<>Hello world!</>}
                    documentationLink="https://test.com"
                    loadedText="Do Action"
                    loadingText="Doing Action..."
                    title="A Title"
                />
            </QueryClientProvider>
        );

        const user = userEvent.setup();

        const button = screen.getByRole('button');
        const input = screen.getByRole('textbox');

        expect(button).toHaveAttribute('disabled');

        const invalidInputs = [' ', 'zone-100', 'zone1'];
        for (let i = 0; i < invalidInputs.length; i++) {
            await user.clear(input);
            await user.type(input, invalidInputs[i]);
            expect(button).toHaveAttribute('disabled');
        }

        await user.clear(input);
        await user.type(input, 'zone1-101');
        expect(button).not.toHaveAttribute('disabled');
    });

    it('does not render confirmation if "confirmationValue" not set', async () => {
        render(
            <QueryClientProvider client={queryClient}>
                <Wrapper
                    description={<>Hello world!</>}
                    documentationLink="https://test.com"
                    loadedText="Do Action"
                    loadingText="Doing Action..."
                    title="A Title"
                />
            </QueryClientProvider>
        );

        const button = screen.getByRole('button');
        const input = screen.queryByRole('textbox');

        expect(input).toBeNull();
        expect(button).not.toHaveAttribute('disabled');
    });

    it('disables interaction when "disabled" prop is set', () => {
        render(
            <QueryClientProvider client={queryClient}>
                <Wrapper
                    confirmationValue="zone1-101"
                    description={<>Hello world!</>}
                    disabled
                    documentationLink="https://test.com"
                    loadedText="Do Action"
                    loadingText="Doing Action..."
                    title="A Title"
                />
            </QueryClientProvider>
        );

        const button = screen.getByRole('button');
        const input = screen.queryByRole('textbox');

        expect(input).toBeNull();
        expect(button).toHaveAttribute('disabled');
    });
});
