/**
 * Copyright 2022 The Vitess Authors.
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

import { render, screen } from '@testing-library/react';
import { ReadOnlyGate } from './ReadOnlyGate';
import { describe, it, expect, afterEach } from 'vitest';

// Preserve import.meta.env to restore its original values after each test runs.
const ORIGINAL_PROCESS_ENV = { ...import.meta.env };

describe('ReadOnlyGate', () => {
    afterEach(() => {
        import.meta.env = ORIGINAL_PROCESS_ENV;
    });

    it('hides children when in read-only mode', () => {
        (process as any).env.VITE_READONLY_MODE = 'true';

        render(
            <ReadOnlyGate>
                <div data-testid="child">🌶🌶🌶</div>
            </ReadOnlyGate>
        );

        const child = screen.queryByTestId('child');
        expect(child).toBeNull();
    });

    it('shows children when not in read-only mode', () => {
        render(
            <ReadOnlyGate>
                <div data-testid="child">🌶🌶🌶</div>
            </ReadOnlyGate>
        );

        const child = screen.queryByTestId('child');
        expect(child).not.toBeNull();
        expect(child).toHaveTextContent('🌶🌶🌶');
    });
});
