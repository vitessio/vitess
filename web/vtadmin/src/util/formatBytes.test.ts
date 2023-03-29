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
import { formatBytes } from './formatBytes';
import { describe, expect } from 'vitest';

describe('formatBytes', () => {
    const tests: {
        name: string;
        params: Parameters<typeof formatBytes>;
        expected: string | null;
    }[] = [
        {
            name: 'handles numeric inputs',
            params: [1024],
            expected: '1 KiB',
        },
        {
            name: 'handles string inputs',
            params: ['1024'],
            expected: '1 KiB',
        },
        {
            name: 'handles undefined inputs',
            params: [undefined],
            expected: null,
        },
        {
            name: 'handles null inputs',
            params: [null],
            expected: null,
        },
        {
            name: 'handles NaN inputs',
            params: ['not-a-number'],
            expected: null,
        },
        {
            name: 'uses default precision',
            params: [1234],
            expected: '1.21 KiB',
        },
        {
            name: 'uses units parameter if defined',
            params: [1234567890, 'MiB'],
            expected: '1,177.38 MiB',
        },
    ];

    test.each(tests.map(Object.values))(
        '%s',
        (name: string, params: Parameters<typeof formatBytes>, expected: string | null) => {
            expect(formatBytes(...params)).toEqual(expected);
        }
    );
});
