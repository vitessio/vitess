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
import { createMemoryHistory, MemoryHistory } from 'history';
import { QueryClient, QueryClientProvider } from 'react-query';
import { Route, Router, Switch } from 'react-router-dom';
import { describe, it, expect, beforeEach, vi } from 'vitest';

import { Tablet } from './Tablet';

// Preserve process.env to restore its original values after each test run
const ORIGINAL_PROCESS_ENV = { ...process.env };

const INITIAL_HISTORY = ['/tablet/someCluster/someAlias/qps'];

const renderHelper = (history?: MemoryHistory) => {
    const queryClient = new QueryClient();

    // Note that when testing redirects, history.location will have the expected value
    // but window.location will not.
    const _history = history || createMemoryHistory({ initialEntries: INITIAL_HISTORY });

    return render(
        <QueryClientProvider client={queryClient}>
            <Router history={_history}>
                <Switch>
                    <Route path="/tablet/:clusterID/:alias">
                        <Tablet />
                    </Route>

                    {/* <Route path="/tablet/:clusterID/:alias">
                        <Tablet />
                    </Route> */}

                    <Route>
                        <div>no match</div>
                    </Route>
                </Switch>
            </Router>
        </QueryClientProvider>
    );
};

describe('Tablet view', () => {
    afterEach(() => {
        process.env = ORIGINAL_PROCESS_ENV;
        vi.clearAllMocks();
    });

    it('renders', async () => {
        const history = createMemoryHistory({ initialEntries: INITIAL_HISTORY });
        renderHelper(history);
        const title = screen.getByRole('heading', { level: 1 });
        expect(title).toHaveTextContent('someAlias');
    });

    it('displays the advanced tab', async () => {
        expect(process.env.VITE_READONLY_MODE).toBeFalsy();
        renderHelper();

        const tab = screen.getByRole('tab', { name: 'Advanced' });
        expect(tab).toHaveTextContent('Advanced');
    });

    // Weird thing worth mentioning -- when testing redirects, the page contents doesn't render
    // as expected, so any assertions against `screen` will (probably) not work. This might be an async thing;
    // either way, this kind of redirect is deprecated in the next version of react-router.
    it('redirects from "/" to a default route', () => {
        const history = createMemoryHistory({ initialEntries: INITIAL_HISTORY });
        renderHelper(history);
        expect(history.location.pathname).toEqual('/tablet/someCluster/someAlias/qps');
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
            const history = createMemoryHistory({ initialEntries: ['/tablet/someCluster/someAlias/advanced'] });
            renderHelper(history);
            expect(history.location.pathname).toEqual('/tablet/someCluster/someAlias/qps');
        });
    });
});
