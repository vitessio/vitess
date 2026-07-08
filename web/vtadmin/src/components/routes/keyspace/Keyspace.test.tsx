/**
 * Copyright 2026 The Vitess Authors.
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
import { describe, it, expect } from 'vitest';

import { Keyspace } from './Keyspace';

const renderHelper = (entries: string[]) => {
    const queryClient = new QueryClient();

    return render(
        <QueryClientProvider client={queryClient}>
            <MemoryRouter initialEntries={entries}>
                <Routes>
                    <Route path="/keyspace/:clusterID/:name/*" element={<Keyspace />} />
                    <Route path="*" element={<div>no match</div>} />
                </Routes>
            </MemoryRouter>
        </QueryClientProvider>
    );
};

describe('Keyspace tab navigation', () => {
    it('tab links use absolute paths and do not append to the current splat URL', () => {
        renderHelper(['/keyspace/iad/aux1/shards']);

        const tabs = screen.getAllByRole('tab');
        const hrefs = tabs.map((tab) => tab.getAttribute('href'));

        expect(hrefs).toContain('/keyspace/iad/aux1/shards');
        expect(hrefs).toContain('/keyspace/iad/aux1/vschema');
        expect(hrefs).toContain('/keyspace/iad/aux1/json');
        expect(hrefs).toContain('/keyspace/iad/aux1/json_tree');

        // Ensure no tab link contains a doubled path segment (the regression pattern)
        hrefs.forEach((href) => {
            expect(href).not.toMatch(/\/shards\/vschema/);
            expect(href).not.toMatch(/\/shards\/json/);
            expect(href).not.toMatch(/\/shards\/shards/);
        });
    });
});
