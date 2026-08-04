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
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { describe, it, expect, beforeEach, vi } from 'vitest';

import { Tablet } from './Tablet';

// Preserve import.meta.env to restore its original values after each test run
const ORIGINAL_PROCESS_ENV = { ...import.meta.env };

const INITIAL_HISTORY = ['/tablet/someCluster/someAlias/qps'];

const renderHelper = (entries?: string[]) => {
    const queryClient = new QueryClient();

    const initialEntries = entries || INITIAL_HISTORY;

    return render(
        <QueryClientProvider client={queryClient}>
            <MemoryRouter initialEntries={initialEntries}>
                <Routes>
                    <Route path="/tablet/:clusterID/:alias/*" element={<Tablet />} />
                    <Route path="*" element={<div>no match</div>} />
                </Routes>
            </MemoryRouter>
        </QueryClientProvider>
    );
};

describe('Tablet view', () => {
    afterEach(() => {
        Object.assign(import.meta.env, ORIGINAL_PROCESS_ENV);
        vi.clearAllMocks();
    });

    it('renders', async () => {
        renderHelper(INITIAL_HISTORY);
        const title = screen.getByRole('heading', { level: 1 });
        expect(title).toHaveTextContent('someAlias');
    });

    it('displays the advanced tab', async () => {
        expect(import.meta.env.VITE_READONLY_MODE).toBeFalsy();
        renderHelper();

        const tab = screen.getByRole('tab', { name: 'Advanced' });
        expect(tab).toHaveTextContent('Advanced');
    });

    it('renders the correct content for the default route', () => {
        renderHelper(INITIAL_HISTORY);
        const title = screen.getByRole('heading', { level: 1 });
        expect(title).toHaveTextContent('someAlias');
    });

    describe('read-only mode', () => {
        beforeEach(() => {
            (process as any).env.VITE_READONLY_MODE = 'true';
        });

        it('hides the "Advanced" tab', () => {
            renderHelper();
            const tab = screen.queryByRole('tab', { name: 'Advanced' });
            expect(tab).toBe(null);
        });

        it('redirects from /advanced', () => {
            renderHelper(['/tablet/someCluster/someAlias/advanced']);
            // After redirect, the heading should still show the alias
            const title = screen.getByRole('heading', { level: 1 });
            expect(title).toHaveTextContent('someAlias');
        });
    });
});
