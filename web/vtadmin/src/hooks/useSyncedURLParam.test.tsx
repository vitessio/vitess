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
import { describe, expect, vi } from 'vitest';

import { useSyncedURLParam } from './useSyncedURLParam';

describe('useSyncedURLParam', () => {
    it('should push to history when updating initially empty value', () => {
        const history = createMemoryHistory();

        vi.spyOn(history, 'push');
        vi.spyOn(history, 'replace');

        const { result } = renderHook(() => useSyncedURLParam('hello'), {
            wrapper: ({ children }) => {
                return <Router history={history}>{children}</Router>;
            },
        });

        expect(history.location.search).toEqual('');
        expect(result.current.value).toEqual('');

        act(() => {
            result.current.updateValue('world');
        });

        expect(history.location.search).toEqual('?hello=world');
        expect(result.current.value).toEqual('world');

        expect(history.push).toHaveBeenCalledTimes(1);
        expect(history.replace).toHaveBeenCalledTimes(0);
    });

    it('should replace history when a value is already defined in the URL', () => {
        const history = createMemoryHistory({ initialEntries: ['/?hello=world'] });

        vi.spyOn(history, 'push');
        vi.spyOn(history, 'replace');

        const { result } = renderHook(() => useSyncedURLParam('hello'), {
            wrapper: ({ children }) => {
                return <Router history={history}>{children}</Router>;
            },
        });

        expect(history.location.search).toEqual('?hello=world');
        expect(result.current.value).toEqual('world');

        act(() => {
            result.current.updateValue('moon');
        });

        expect(history.location.search).toEqual('?hello=moon');
        expect(result.current.value).toEqual('moon');

        expect(history.push).toHaveBeenCalledTimes(0);
        expect(history.replace).toHaveBeenCalledTimes(1);
    });

    it('should clear the URL parameter and push to history when given an empty value', () => {
        const history = createMemoryHistory({ initialEntries: ['/?hello=world'] });

        vi.spyOn(history, 'push');
        vi.spyOn(history, 'replace');

        const { result } = renderHook(() => useSyncedURLParam('hello'), {
            wrapper: ({ children }) => {
                return <Router history={history}>{children}</Router>;
            },
        });

        expect(history.location.search).toEqual('?hello=world');
        expect(result.current.value).toEqual('world');

        act(() => {
            result.current.updateValue(null);
        });

        expect(history.location.search).toEqual('?');
        expect(result.current.value).toEqual('');

        expect(history.push).toHaveBeenCalledTimes(1);
        expect(history.replace).toHaveBeenCalledTimes(0);
    });

    it('should not modify unrelated URL parameters', () => {
        const history = createMemoryHistory({ initialEntries: ['/?goodbye=world'] });

        const { result } = renderHook(() => useSyncedURLParam('hello'), {
            wrapper: ({ children }) => {
                return <Router history={history}>{children}</Router>;
            },
        });

        expect(history.location.search).toEqual('?goodbye=world');
        expect(result.current.value).toEqual('');

        act(() => {
            result.current.updateValue('moon');
        });

        expect(history.location.search).toEqual('?goodbye=world&hello=moon');
        expect(result.current.value).toEqual('moon');

        act(() => {
            result.current.updateValue(null);
        });

        expect(history.location.search).toEqual('?goodbye=world');
        expect(result.current.value).toEqual('');
    });

    // This is a longer, integration-y test that simulates traversing the history stack
    // with the browser's "back" button.
    it('should properly manipulate history given a sequence of inputs', () => {
        const history = createMemoryHistory();

        vi.spyOn(history, 'push');
        vi.spyOn(history, 'replace');

        const { result } = renderHook(() => useSyncedURLParam('sequence'), {
            wrapper: ({ children }) => {
                return <Router history={history}>{children}</Router>;
            },
        });

        act(() => {
            result.current.updateValue('one');
        });

        expect(history.location.search).toEqual('?sequence=one');
        expect(result.current.value).toEqual('one');

        act(() => {
            result.current.updateValue('two');
        });

        expect(history.location.search).toEqual('?sequence=two');
        expect(result.current.value).toEqual('two');

        act(() => {
            result.current.updateValue(null);
        });

        expect(history.location.search).toEqual('?');
        expect(result.current.value).toEqual('');

        act(() => {
            result.current.updateValue('three');
        });

        expect(history.location.search).toEqual('?sequence=three');
        expect(result.current.value).toEqual('three');

        act(() => {
            history.back();
        });

        expect(history.location.search).toEqual('?');
        expect(result.current.value).toEqual('');

        act(() => {
            history.back();
        });

        expect(history.location.search).toEqual('?sequence=two');
        expect(result.current.value).toEqual('two');

        act(() => {
            history.back();
        });

        // Note that we expect to be missing the "one" value since it was *replaced* by "two"
        expect(history.location.search).toEqual('');
        expect(result.current.value).toEqual('');
    });
});
