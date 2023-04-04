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

import { ErrorHandler, HttpResponseNotOkError } from './errorTypes';
import * as errorHandler from './errorHandler';
import * as errorHandlers from './errorHandlers';
import { describe, it, expect, vi } from 'vitest';
import { Response } from 'cross-fetch';

describe('errorHandler', () => {
    let mockErrorHandler: ErrorHandler;

    beforeEach(() => {
        mockErrorHandler = {
            initialize: vi.fn(),
            isEnabled: () => true,
            notify: vi.fn(),
        };

        vi.spyOn(errorHandlers, 'getHandlers').mockReturnValue([mockErrorHandler]);
    });

    describe('initialize', () => {
        it('initializes enabled handlers', () => {
            errorHandler.initialize();
            expect(mockErrorHandler.initialize).toHaveBeenCalledTimes(1);
        });
    });

    describe('notify', () => {
        it('notifies enabled ErrorHandlers', () => {
            const err = new Error('testing');
            errorHandler.notify(err);

            expect(mockErrorHandler.notify).toHaveBeenCalledTimes(1);
            expect(mockErrorHandler.notify).toHaveBeenCalledWith(
                err,
                { VITE_VTADMIN_API_ADDRESS: 'http://test-api.com' },
                {
                    errorMetadata: {},
                }
            );
        });

        it("appends metadata from the Error's instance properties", () => {
            const response = new Response('', { status: 500 });
            const err = new HttpResponseNotOkError('/api/test', { ok: false }, response);
            errorHandler.notify(err, { goodbye: 'moon' });

            expect(mockErrorHandler.notify).toHaveBeenCalledTimes(1);
            expect(mockErrorHandler.notify).toHaveBeenCalledWith(
                err,
                { VITE_VTADMIN_API_ADDRESS: 'http://test-api.com' },
                {
                    errorMetadata: {
                        fetchResponse: {
                            ok: false,
                            status: 500,
                            statusText: 'Internal Server Error',
                            type: undefined,
                            url: '',
                        },
                        name: 'HttpResponseNotOkError',
                        response: { ok: false },
                    },
                    goodbye: 'moon',
                }
            );
        });

        it('only includes santizied environment variables', () => {
            import.meta.env = {
                VITE_VTADMIN_API_ADDRESS: 'http://not-secret.example.com',
                VITE_BUGSNAG_API_KEY: 'secret',
            } as NodeJS.ProcessEnv;

            const err = new Error('testing');
            errorHandler.notify(err);

            expect(mockErrorHandler.notify).toHaveBeenCalledTimes(1);
            expect(mockErrorHandler.notify).toHaveBeenCalledWith(
                err,
                {
                    VITE_VTADMIN_API_ADDRESS: 'http://not-secret.example.com',
                },
                {
                    errorMetadata: {},
                }
            );
        });
    });
});
