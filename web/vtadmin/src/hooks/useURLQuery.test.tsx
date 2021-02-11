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
import { createMemoryHistory } from 'history';
import { Router } from 'react-router-dom';

import { useURLQuery } from './useURLQuery';

describe('useURLQuery', () => {
    const tests: {
        name: string;
        url: string;
        expected: ReturnType<typeof useURLQuery>;
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

    test.each(tests.map(Object.values))('%s', (name: string, url: string, expected: ReturnType<typeof useURLQuery>) => {
        const history = createMemoryHistory({
            initialEntries: [url],
        });

        const { result } = renderHook(() => useURLQuery(), {
            wrapper: ({ children }) => {
                return <Router history={history}>{children}</Router>;
            },
        });

        expect(result.current).toEqual(expected);
    });
});
