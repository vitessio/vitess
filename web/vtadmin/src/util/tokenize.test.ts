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
import { Token, tokenizeSearch, SearchToken, SearchTokenTypes } from './tokenize';
import { describe, expect, test } from 'vitest';

describe('tokenize', () => {
    describe('tokenizeSearch', () => {
        const tests: {
            name: string;
            input: string;
            expected: SearchToken[];
        }[] = [
            {
                name: 'parses fuzzy strings',
                input: 'hello',
                expected: [{ type: SearchTokenTypes.FUZZY, value: 'hello' }],
            },
            {
                name: 'parses exact strings',
                input: '"hello"',
                expected: [{ type: SearchTokenTypes.EXACT, value: 'hello' }],
            },
            {
                name: 'parses key/values',
                input: 'hello:moon',
                expected: [{ type: SearchTokenTypes.KEY_VALUE, key: 'hello', value: 'moon' }],
            },
            {
                name: 'parses multiple tokens',
                input: 'hello "moon" goodbye:world',
                expected: [
                    { type: SearchTokenTypes.FUZZY, value: 'hello' },
                    { type: SearchTokenTypes.EXACT, value: 'moon' },
                    { type: SearchTokenTypes.KEY_VALUE, key: 'goodbye', value: 'world' },
                ],
            },
            {
                name: 'parses numbers and symbols',
                input: 'hello-123 "moon-456" goodbye:world-789',
                expected: [
                    { type: SearchTokenTypes.FUZZY, value: 'hello-123' },
                    { type: SearchTokenTypes.EXACT, value: 'moon-456' },
                    { type: SearchTokenTypes.KEY_VALUE, key: 'goodbye', value: 'world-789' },
                ],
            },
        ];

        test.each(tests.map(Object.values))('%s', (name: string, input: string, expected: Token[]) => {
            const result = tokenizeSearch(input);
            expect(result).toEqual(expected);
        });
    });
});
