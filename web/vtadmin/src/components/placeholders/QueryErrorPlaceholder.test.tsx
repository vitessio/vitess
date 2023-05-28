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
import { fireEvent, render, screen, within } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { QueryClient, QueryClientProvider } from 'react-query';

import { QueryErrorPlaceholder } from './QueryErrorPlaceholder';
import * as httpAPI from '../../api/http';
import { useClusters } from '../../hooks/api';
import { describe, it, expect, vi } from 'vitest';

vi.mock('../../api/http');

const queryHelper = () => {
    const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
    });

    const wrapper: React.FC = ({ children }) => (
        <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );

    return renderHook(() => useClusters(), { wrapper });
};

describe('QueryErrorPlaceholder', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('renders an error message', async () => {
        const errorMessage = 'nope';
        (httpAPI.fetchClusters as any).mockRejectedValueOnce(new Error(errorMessage));

        const { result, waitFor } = queryHelper();
        await waitFor(() => result.current.isError);

        render(<QueryErrorPlaceholder query={result.current} />);

        const placeholder = await screen.findByRole('status');
        expect(placeholder).not.toBeNull();

        const button = within(placeholder).getByRole('button');
        expect(button).not.toBeNull();

        const message = within(placeholder).getByTestId('error-message');
        expect(message.textContent).toEqual(errorMessage);
    });

    it('refetches', async () => {
        (httpAPI.fetchClusters as any).mockRejectedValueOnce(new Error()).mockRejectedValueOnce(new Error());

        expect((httpAPI.fetchClusters as any).mock.calls.length).toEqual(0);

        const { result, waitFor } = queryHelper();
        await waitFor(() => result.current.isError);

        render(<QueryErrorPlaceholder query={result.current} />);

        expect((httpAPI.fetchClusters as any).mock.calls.length).toEqual(1);

        let placeholder = await screen.findByRole('status');
        let button = within(placeholder).getByRole('button');
        expect(button).not.toBeNull();
        expect(button.textContent).toEqual('Try again');

        fireEvent.click(button);

        await waitFor(() => result.current.isFetching);
        expect((httpAPI.fetchClusters as any).mock.calls.length).toEqual(2);
    });

    it('does not render when no error', async () => {
        (httpAPI.fetchClusters as any).mockResolvedValueOnce({ clusters: [] });
        const { result, waitFor } = queryHelper();

        render(<QueryErrorPlaceholder query={result.current} />);

        await waitFor(() => result.current.isSuccess);

        const placeholder = screen.queryByRole('status');
        expect(placeholder).toBeNull();
    });

    it('does not render when loading', async () => {
        (httpAPI.fetchClusters as any).mockRejectedValueOnce(new Error());
        const { result, waitFor } = queryHelper();

        const { rerender } = render(<QueryErrorPlaceholder query={result.current} />);
        await waitFor(() => result.current.isLoading);

        let placeholder = screen.queryByRole('status');
        expect(placeholder).toBeNull();

        await waitFor(() => !result.current.isLoading);

        rerender(<QueryErrorPlaceholder query={result.current} />);
        placeholder = screen.queryByRole('status');
        expect(placeholder).not.toBeNull();
    });
});
