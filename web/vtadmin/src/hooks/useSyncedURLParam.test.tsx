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
import { act, renderHook } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect } from 'vitest';

import { useSyncedURLParam } from './useSyncedURLParam';

describe('useSyncedURLParam', () => {
    it('should push to history when updating initially empty value', () => {
        const { result } = renderHook(() => useSyncedURLParam('hello'), {
            wrapper: ({ children }) => {
                return <MemoryRouter initialEntries={['/']}>{children}</MemoryRouter>;
            },
        });

        expect(result.current.value).toEqual('');

        act(() => {
            result.current.updateValue('world');
        });

        expect(result.current.value).toEqual('world');
    });

    it('should replace history when a value is already defined in the URL', () => {
        const { result } = renderHook(() => useSyncedURLParam('hello'), {
            wrapper: ({ children }) => {
                return <MemoryRouter initialEntries={['/?hello=world']}>{children}</MemoryRouter>;
            },
        });

        expect(result.current.value).toEqual('world');

        act(() => {
            result.current.updateValue('moon');
        });

        expect(result.current.value).toEqual('moon');
    });

    it('should clear the URL parameter and push to history when given an empty value', () => {
        const { result } = renderHook(() => useSyncedURLParam('hello'), {
            wrapper: ({ children }) => {
                return <MemoryRouter initialEntries={['/?hello=world']}>{children}</MemoryRouter>;
            },
        });

        expect(result.current.value).toEqual('world');

        act(() => {
            result.current.updateValue(null);
        });

        expect(result.current.value).toEqual('');
    });

    it('should not modify unrelated URL parameters', () => {
        const { result } = renderHook(() => useSyncedURLParam('hello'), {
            wrapper: ({ children }) => {
                return <MemoryRouter initialEntries={['/?goodbye=world']}>{children}</MemoryRouter>;
            },
        });

        expect(result.current.value).toEqual('');

        act(() => {
            result.current.updateValue('moon');
        });

        expect(result.current.value).toEqual('moon');

        act(() => {
            result.current.updateValue(null);
        });

        expect(result.current.value).toEqual('');
    });

    // This test verifies value changes through a sequence of updateValue calls.
    // Note: history.back() testing is not supported with MemoryRouter, so we only
    // verify forward navigation through updateValue.
    it('should properly update values given a sequence of inputs', () => {
        const { result } = renderHook(() => useSyncedURLParam('sequence'), {
            wrapper: ({ children }) => {
                return <MemoryRouter initialEntries={['/']}>{children}</MemoryRouter>;
            },
        });

        act(() => {
            result.current.updateValue('one');
        });

        expect(result.current.value).toEqual('one');

        act(() => {
            result.current.updateValue('two');
        });

        expect(result.current.value).toEqual('two');

        act(() => {
            result.current.updateValue(null);
        });

        expect(result.current.value).toEqual('');

        act(() => {
            result.current.updateValue('three');
        });

        expect(result.current.value).toEqual('three');
    });
});
