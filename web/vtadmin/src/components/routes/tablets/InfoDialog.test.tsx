/**
 * Copyright 2026 The Vitess Authors.
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

import { describe, expect, it } from 'vitest';
import { render, screen } from '@testing-library/react';
import { UseQueryResult } from '@tanstack/react-query';

import InfoDialog from './InfoDialog';

const queryResult = (overrides: object) => () =>
    ({
        data: undefined,
        error: null,
        isLoading: false,
        refetch: () => undefined,
        ...overrides,
    }) as unknown as UseQueryResult<any, Error>;

describe('InfoDialog', () => {
    it('shows the result once the hook has loaded', async () => {
        render(
            <InfoDialog
                isOpen
                onClose={() => undefined}
                loadingDescription="Pinging"
                successDescription="Pinged the tablet"
                errorDescription="Could not ping the tablet"
                useHook={queryResult({ data: { ok: true } })}
            />
        );

        expect(await screen.findByText('Pinged the tablet')).toBeDefined();
    });

    it('shows the error once the hook has failed', async () => {
        render(
            <InfoDialog
                isOpen
                onClose={() => undefined}
                loadingDescription="Pinging"
                successDescription="Pinged the tablet"
                errorDescription="Could not ping the tablet"
                useHook={queryResult({ error: new Error('no route to tablet') })}
            />
        );

        expect(await screen.findByText(/Could not ping the tablet/)).toBeDefined();
    });
});
