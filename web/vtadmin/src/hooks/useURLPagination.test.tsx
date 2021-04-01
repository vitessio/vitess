/**
 * Copyright 2021 The Vitess Authors.
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
import { renderHook } from '@testing-library/react-hooks';
import { createMemoryHistory, To } from 'history';
import { Router } from 'react-router-dom';
import { PaginationOpts, PaginationParams, useURLPagination } from './useURLPagination';

describe('useURLPagination', () => {
    const tests: {
        name: string;
        url: string;
        opts: PaginationOpts;
        // URL query parameters after any redirects have taken place
        expected: PaginationParams;
        // If defined, checks whether history.replace was called with the given parameters.
        // If null, checks that history.replace was not called. (Unfortunately, we can't
        // make this an optional param, else Jest times out because the length of
        // callback args must be consistent between tests.)
        redirectParams: To | null;
    }[] = [
        {
            name: 'returns pagination parameters in the URL',
            url: '/test?page=1&foo=bar',
            opts: { totalPages: 10 },
            expected: { page: 1 },
            redirectParams: null,
        },
        {
            name: 'assumes an undefined page parameter is the first page',
            url: '/test?foo=bar',
            opts: { totalPages: 10 },
            expected: { page: 1 },
            redirectParams: null,
        },
        {
            name: 'redirects to the first page if current page > total pages',
            url: '/test?page=100&foo=bar',
            opts: { totalPages: 10 },
            expected: { page: 1 },
            redirectParams: { search: '?foo=bar&page=1' },
        },
        {
            name: 'redirects to the first page if current page is a negative number',
            url: '/test?page=-123&foo=bar',
            opts: { totalPages: 10 },
            expected: { page: 1 },
            redirectParams: { search: '?foo=bar&page=1' },
        },
        {
            name: 'redirects to the first page if current page is not a number',
            url: '/test?page=abc&foo=bar',
            opts: { totalPages: 10 },
            expected: { page: 1 },
            redirectParams: { search: '?foo=bar&page=1' },
        },
        {
            name: 'does not redirect if totalPages is 0',
            url: '/test?page=100&foo=bar',
            opts: { totalPages: 0 },
            expected: { page: 100 },
            redirectParams: null,
        },
    ];

    test.concurrent.each(tests.map(Object.values))(
        '%s',
        (name: string, url: string, opts: PaginationOpts, expected: PaginationParams, redirectParams: To | null) => {
            const history = createMemoryHistory({ initialEntries: [url] });
            jest.spyOn(history, 'replace');

            const { result } = renderHook(() => useURLPagination(opts), {
                wrapper: ({ children }) => {
                    return <Router history={history}>{children}</Router>;
                },
            });

            expect(result.current).toEqual(expected);

            if (redirectParams) {
                expect(history.replace).toHaveBeenCalledWith(redirectParams);
            } else {
                expect(history.replace).not.toHaveBeenCalled();
            }
        }
    );
});
