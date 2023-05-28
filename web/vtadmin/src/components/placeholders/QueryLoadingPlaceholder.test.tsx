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

import { render, screen } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { QueryClient, QueryClientProvider } from 'react-query';

import * as httpAPI from '../../api/http';

import { useClusters } from '../../hooks/api';
import { QueryLoadingPlaceholder } from './QueryLoadingPlaceholder';
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

describe('QueryLoadingPlaceholder', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('renders only when loading', async () => {
        (httpAPI.fetchClusters as any).mockResolvedValueOnce({ clusters: [] });
        const { result, waitFor } = queryHelper();

        const { rerender } = render(<QueryLoadingPlaceholder query={result.current} />);

        await waitFor(() => result.current.isLoading);

        let placeholder = screen.queryByRole('status');
        expect(placeholder).not.toBeNull();
        expect(placeholder?.textContent).toEqual('Loading...');

        await waitFor(() => result.current.isSuccess);

        rerender(<QueryLoadingPlaceholder query={result.current} />);
        placeholder = screen.queryByRole('status');
        expect(placeholder).toBeNull();
    });
});
