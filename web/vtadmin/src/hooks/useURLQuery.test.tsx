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
import { act } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { createMemoryHistory } from 'history';
import { Router } from 'react-router-dom';
import { QueryParams } from '../util/queryString';

import { useURLQuery } from './useURLQuery';

describe('useURLQuery', () => {
    describe('parsing', () => {
        const tests: {
            name: string;
            url: string;
            expected: ReturnType<typeof useURLQuery>['query'];
        }[] = [
            {
                name: 'parses numbers',
                url: '/test?page=1',
                expected: { page: 1 },
            },
            {
                name: 'parses booleans',
                url: '/test?foo=true&bar=false',
                expected: { foo: true, bar: false },
            },
            {
                name: 'parses arrays by duplicate keys',
                url: '/test?list=1&list=2&list=3',
                expected: { list: [1, 2, 3] },
            },
            {
                name: 'parses complex URLs',
                url: '/test?page=1&isTrue=true&isFalse=false&list=one&list=two&list=three&foo=bar',
                expected: { page: 1, isTrue: true, isFalse: false, list: ['one', 'two', 'three'], foo: 'bar' },
            },
        ];

        test.each(tests.map(Object.values))(
            '%s',
            (name: string, url: string, expected: ReturnType<typeof useURLQuery>) => {
                const history = createMemoryHistory({
                    initialEntries: [url],
                });

                const { result } = renderHook(() => useURLQuery(), {
                    wrapper: ({ children }) => {
                        return <Router history={history}>{children}</Router>;
                    },
                });

                expect(result.current.query).toEqual(expected);
            }
        );
    });

    describe('pushQuery', () => {
        const tests: {
            name: string;
            initialEntries: string[];
            nextQuery: QueryParams;
            expected: QueryParams;
        }[] = [
            {
                name: 'stringifies and pushes query parameters onto history',
                initialEntries: ['/test'],
                nextQuery: { goodnight: 'moon' },
                expected: { goodnight: 'moon' },
            },
            {
                name: 'merges the next query with the current query',
                initialEntries: ['/test?hello=world'],
                nextQuery: { goodnight: 'moon' },
                expected: { hello: 'world', goodnight: 'moon' },
            },
            {
                name: 'it does not merge array-like queries',
                initialEntries: ['/test?arr=one&arr=two'],
                nextQuery: { arr: [3, 4, 5] },
                expected: { arr: [3, 4, 5] },
            },
        ];

        test.concurrent.each(tests.map(Object.values))(
            '%s',
            (name: string, initialEntries: string[], nextQuery: QueryParams, expected: QueryParams) => {
                const history = createMemoryHistory({ initialEntries });
                const initialPathname = history.location.pathname;

                jest.spyOn(history, 'push');

                const { result } = renderHook(() => useURLQuery(), {
                    wrapper: ({ children }) => {
                        return <Router history={history}>{children}</Router>;
                    },
                });

                act(() => {
                    result.current.pushQuery(nextQuery);
                });

                expect(history.push).toHaveBeenCalledTimes(1);
                expect(result.current.query).toEqual(expected);
                expect(history.location.pathname).toEqual(initialPathname);
            }
        );
    });

    describe('replaceQuery', () => {
        const tests: {
            name: string;
            initialEntries: string[];
            nextQuery: QueryParams;
            expected: QueryParams;
        }[] = [
            {
                name: 'stringifies and replaces query parameters onto history',
                initialEntries: ['/test'],
                nextQuery: { goodnight: 'moon' },
                expected: { goodnight: 'moon' },
            },
            {
                name: 'merges the next query with the current query',
                initialEntries: ['/test?hello=world'],
                nextQuery: { goodnight: 'moon' },
                expected: { hello: 'world', goodnight: 'moon' },
            },
            {
                name: 'it does not merge array-like queries',
                initialEntries: ['/test?arr=one&arr=two'],
                nextQuery: { arr: [3, 4, 5] },
                expected: { arr: [3, 4, 5] },
            },
        ];

        test.concurrent.each(tests.map(Object.values))(
            '%s',
            (name: string, initialEntries: string[], nextQuery: QueryParams, expected: QueryParams) => {
                const history = createMemoryHistory({ initialEntries });
                const initialPathname = history.location.pathname;

                jest.spyOn(history, 'replace');

                const { result } = renderHook(() => useURLQuery(), {
                    wrapper: ({ children }) => {
                        return <Router history={history}>{children}</Router>;
                    },
                });

                act(() => {
                    result.current.replaceQuery(nextQuery);
                });

                expect(history.replace).toHaveBeenCalledTimes(1);
                expect(result.current.query).toEqual(expected);
                expect(history.location.pathname).toEqual(initialPathname);
            }
        );
    });

    it('uses parsing/formatting options when specified', () => {
        const history = createMemoryHistory({ initialEntries: ['/test?foo=true&count=123'] });
        const { result } = renderHook(
            () =>
                useURLQuery({
                    parseBooleans: false,
                    parseNumbers: false,
                }),
            {
                wrapper: ({ children }) => {
                    return <Router history={history}>{children}</Router>;
                },
            }
        );

        expect(result.current.query).toEqual({ foo: 'true', count: '123' });

        act(() => {
            result.current.pushQuery({ foo: false, count: 456 });
        });

        expect(result.current.query).toEqual({ foo: 'false', count: '456' });

        act(() => {
            result.current.pushQuery({ foo: true, count: 789 });
        });

        expect(result.current.query).toEqual({ foo: 'true', count: '789' });
    });

    it('memoizes the query object by search string', () => {
        const history = createMemoryHistory({ initialEntries: ['/test?hello=world'] });

        jest.spyOn(history, 'push');

        const { result } = renderHook(() => useURLQuery(), {
            wrapper: ({ children }) => {
                return <Router history={history}>{children}</Router>;
            },
        });

        const firstResult = result.current.query;
        expect(firstResult).toEqual({ hello: 'world' });

        act(() => {
            result.current.pushQuery({ hello: 'world' });
        });

        // Make sure the returned object is memoized when the search string
        // is updated but the value doesn't change.
        expect(history.push).toHaveBeenCalledTimes(1);
        expect(result.current.query).toEqual({ hello: 'world' });
        expect(result.current.query).toBe(firstResult);

        // Make sure the returned query actually changes when the search string changes,
        // and that we're not memoizing too aggressively.
        act(() => {
            result.current.pushQuery({ hello: 'moon' });
        });

        expect(history.push).toHaveBeenCalledTimes(2);
        expect(result.current.query).toEqual({ hello: 'moon' });
    });
});
