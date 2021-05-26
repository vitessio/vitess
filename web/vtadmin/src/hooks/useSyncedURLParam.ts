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
import { useCallback } from 'react';

import { useURLQuery } from './useURLQuery';

/**
 * useSyncedURLValue is a hook for synchronizing a string between a component
 * and the URL. It is optimized for values that change quickly, like user input.
 *
 * Note: the value returned is always a string, so any formatting/parsing is
 * left to the caller.
 *
 * @param key - The key for the URL parameter. A key of "search", for example, would
 * manipulate the `?search=...` value in the URL.
 */
export const useSyncedURLParam = (
    key: string
): {
    updateValue: (nextValue: string | null | undefined) => void;
    // `value` is always a string, since (a) the value in the URL will
    // be a string in the end :) and (b) primitive values like strings are much,
    // much easier to memoize and cache. This means all parsing/formatting is
    // left to the caller.
    value: string | null | undefined;
} => {
    // TODO(doeg): a potentially nice enhancement is to maintain an ephemeral cache
    // (optionally) mapping routes to the last used set of URL parameters.
    // So, for example, if you were (1) on the /tablets view, (2) updated the "filter" parameter,
    // (3) navigated away, and then (4) clicked a nav link back to /tablets, the "filter" parameter
    // parameter you typed in (2) will be lost, since it's only preserved on the history stack,
    // which is only traversable with the "back" button.

    // Ensure we never parse booleans/numbers since the contract (noted above) is that the value is always a string.
    const { query, pushQuery, replaceQuery } = useURLQuery({ parseBooleans: false, parseNumbers: false });
    const value = `${query[key] || ''}`;

    const updateValue = useCallback(
        (nextValue: string | null | undefined) => {
            if (!nextValue) {
                // Push an undefined value to omit the parameter from the URL.
                // This gives us URLs like `?goodbye=moon` instead of `?hello=&goodbye=moon`.
                pushQuery({ [key]: undefined });
            } else if (nextValue && !value) {
                // Replace the current value with a new value. There's a bit of nuance here, since this
                // means that clearing the input is the _only_ way to persist discrete values to
                // the history stack, which is not very intuitive or delightful!
                //
                // TODO(doeg): One possible, more nuanced re-implementation is to push entries onto the stack
                // on a timeout. This means you could type a query, pause and click around a bit,
                // and then type a new query -- both queries would be persisted to the stack,
                // without resorting to calling `pushQuery` on _every_ character typed.
                pushQuery({ [key]: nextValue });
            } else {
                // Replace the current value in the URL with a new one. The previous
                // value (that was replaced) will no longer be available (i.e., it won't
                // be accessible via the back button).
                //
                // We use replaceQuery instead of pushQuery, as pushQuery would push
                // every single letter onto the history stack, which means every click
                // of the back button would iterate backwards, one letter at a time.
                replaceQuery({ [key]: nextValue });
            }
        },
        [key, value, pushQuery, replaceQuery]
    );

    return { value, updateValue };
};
