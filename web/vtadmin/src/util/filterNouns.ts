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

import { partition } from 'lodash-es';
import { KeyValueSearchToken, SearchTokenTypes, tokenizeSearch } from './tokenize';

/**
 * `filterNouns` filters a list of nouns by a search string.
 */
export const filterNouns = <T extends { [k: string]: any }>(needle: string | null, haystack: T[]): T[] => {
    if (!needle) return haystack;

    const tokens = tokenizeSearch(needle);

    // Separate key/value tokens (which can be additive or subtractive depending on whether the
    // key is the same) vs. other tokens (e.g., fuzzy/exact string filters, which are always additive).
    const [kvTokens, otherTokens] = partition(tokens, (t) => t.type === SearchTokenTypes.KEY_VALUE);

    // This assumes that values are ALWAYS exact matches.
    const kvMap = (kvTokens as KeyValueSearchToken[]).reduce((acc, t) => {
        if (!(t.key in acc)) acc[t.key] = [];
        acc[t.key].push(t.value.toLowerCase());
        return acc;
    }, {} as { [key: string]: string[] });

    // Filter out key/value tokens first
    const results = haystack.filter((noun: T) => {
        // Every specified key must map to a matching value for at least one property
        return Object.entries(kvMap).every(([k, vs]) => {
            const nv = noun[k];
            if (!nv) return false;

            // Key/value matches always use an exact (never fuzzy),
            // case insensitive match
            return vs.indexOf(nv.toLowerCase()) >= 0;
        });
    });

    return otherTokens.reduce((acc, token) => {
        switch (token.type) {
            case SearchTokenTypes.EXACT:
                const needle = token.value;
                return acc.filter((o: T) => {
                    return Object.values(o).some((val) => val === needle);
                });
            case SearchTokenTypes.FUZZY:
                return acc.filter((o: T) => {
                    return Object.values(o).some((val) => fuzzyCompare(token.value, `${val}`));
                });
            default:
                return acc;
        }
    }, results as T[]);
};

const fuzzyCompare = (needle: string, haystack: string) => {
    return `${haystack}`.toLowerCase().indexOf(needle.toLowerCase()) >= 0;
};
