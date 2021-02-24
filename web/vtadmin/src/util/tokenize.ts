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

interface Token {
    matches: string[];
    token: string;
    type: string;
}

/**
 * `tokenize` is a tiny, simple tokenizer that parses tokens from the `input` string
 * based on the regexes in `patterns`.
 *
 * At the time of writing, `tokenize` is only used as the tokenizer behind `tokenizeSearch`.
 * Isolating tokenization logic in its own function, however, is a useful separation of concerns
 * should we want to swap in a different implementation.
 *
 * Since `tokenize` is regex-based, it is by no means as robust as using a "real"
 * query syntax. If or when we need more complex parsing capabilities,
 * it would be worth investigating a true parser (like peg.js), combined with a well-known
 * query syntax (like Apache Lucene).
 *
 * Mostly lifted from https://gist.github.com/borgar/451393/7698c95178898c9466214867b46acb2ab2f56d68.
 */
const tokenize = (input: string, patterns: { [k: string]: RegExp }): Token[] => {
    const tokens: Token[] = [];
    let s = input;

    while (s) {
        let t = null;
        let m = s.length;

        for (const key in patterns) {
            const r = patterns[key].exec(s);
            // Try to choose the best match if there are several,
            // where "best" is the closest to the current starting point
            if (r && r.index < m) {
                t = {
                    token: r[0],
                    type: key,
                    matches: r.slice(1),
                };
                m = r.index;
            }
        }

        if (t) {
            tokens.push(t);
        }

        s = s.substr(m + (t ? t.token.length : 0));
    }

    return tokens;
};

export enum SearchTokenTypes {
    EXACT = 'exact',
    FUZZY = 'fuzzy',
    KEY_VALUE = 'keyValue',
}

export type SearchToken = ExactSearchToken | FuzzySearchToken | KeyValueSearchToken;

export interface ExactSearchToken {
    type: SearchTokenTypes.EXACT;
    value: string;
}

export interface FuzzySearchToken {
    type: SearchTokenTypes.FUZZY;
    value: string;
}
export interface KeyValueSearchToken {
    type: SearchTokenTypes.KEY_VALUE;
    key: string;
    value: string;
}

/**
 * `tokenizeSearch` parses tokens from search strings, such as those used to filter
 * lists of tablets and other nouns.
 */
export const tokenizeSearch = (input: string): SearchToken[] => {
    return tokenize(input, {
        keyValue: /(\w+):([^\s"]+)/,
        exact: /"([^\s"]+)"/,
        fuzzy: /([^\s"]+)/,
    }).reduce((acc, token) => {
        switch (token.type) {
            case SearchTokenTypes.EXACT:
                acc.push({ type: token.type, value: token.matches[0] });
                break;
            case SearchTokenTypes.FUZZY:
                acc.push({ type: token.type, value: token.matches[0] });
                break;
            case SearchTokenTypes.KEY_VALUE:
                acc.push({ type: token.type, key: token.matches[0], value: token.matches[1] });
                break;
        }
        return acc;
    }, [] as SearchToken[]);
};
