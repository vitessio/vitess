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
import { rest } from 'msw';

import * as api from './http';
import {
    HttpFetchError,
    HttpResponseNotOkError,
    HTTP_RESPONSE_NOT_OK_ERROR,
    MalformedHttpResponseError,
    MALFORMED_HTTP_RESPONSE_ERROR,
} from '../errors/errorTypes';
import * as errorHandler from '../errors/errorHandler';
import { describe, it, expect, vi } from 'vitest';

vi.mock('../errors/errorHandler');

// mockServerJson configures an HttpOkResponse containing the given `json`
// for all requests made against the given `endpoint`.
const mockServerJson = (endpoint: string, json: object, status: number = 200) => {
    const apiAddr = import.meta.env.VITE_VTADMIN_API_ADDRESS;
    global.server.use(rest.get(`${apiAddr}${endpoint}`, (req, res, ctx) => res(ctx.status(status), ctx.json(json))));
};

describe('api/http', () => {
    describe('vtfetch', () => {
        it('parses and returns JSON, given an HttpOkResponse response', async () => {
            const endpoint = `/api/tablets`;
            const response = { ok: true, result: null };
            mockServerJson(endpoint, response);

            const result = await api.vtfetch(endpoint);
            expect(result).toEqual(response);
        });

        it('throws an error if response.ok is false', async () => {
            const endpoint = `/api/tablets`;
            const response = {
                ok: false,
                error: {
                    code: 'oh_no',
                    message: 'something went wrong',
                },
            };

            // See https://mswjs.io/docs/recipes/mocking-error-responses
            mockServerJson(endpoint, response, 500);

            expect.assertions(5);

            try {
                await api.fetchTablets();
            } catch (error) {
                let e: HttpResponseNotOkError = error as HttpResponseNotOkError;
                /* eslint-disable jest/no-conditional-expect */
                expect(e.name).toEqual(HTTP_RESPONSE_NOT_OK_ERROR);
                expect(e.message).toEqual('[status 500] /api/tablets: oh_no something went wrong');
                expect(e.response).toEqual(response);

                expect(errorHandler.notify).toHaveBeenCalledTimes(1);
                expect(errorHandler.notify).toHaveBeenCalledWith(e);
                /* eslint-enable jest/no-conditional-expect */
            }
        });

        it('throws an error on malformed JSON', async () => {
            errorHandler.notify.mockReset();
            const endpoint = `/api/tablets`;
            global.server.use(
                rest.get(`${import.meta.env.VITE_VTADMIN_API_ADDRESS}${endpoint}`, (req, res, ctx) =>
                    res(ctx.status(504), ctx.body('<html><head><title>504 Gateway Time-out</title></head></html>'))
                )
            );

            expect.assertions(4);

            try {
                await api.vtfetch(endpoint);
            } catch (error) {
                let e: MalformedHttpResponseError = error as MalformedHttpResponseError;
                /* eslint-disable jest/no-conditional-expect */
                expect(e.name).toEqual(MALFORMED_HTTP_RESPONSE_ERROR);
                expect(e.message).toEqual(
                    '[status 504] /api/tablets: invalid json response body at http://test-api.com/api/tablets reason: Unexpected token < in JSON at position 0'
                );

                expect(errorHandler.notify).toHaveBeenCalledTimes(1);
                expect(errorHandler.notify).toHaveBeenCalledWith(e);
                /* eslint-enable jest/no-conditional-expect */
            }
        });

        it('throws an error on malformed response envelopes', async () => {
            const endpoint = `/api/tablets`;
            mockServerJson(endpoint, { foo: 'bar' });

            expect.assertions(1);

            try {
                await api.vtfetch(endpoint);
            } catch (error) {
                let e: MalformedHttpResponseError = error as MalformedHttpResponseError;
                /* eslint-disable jest/no-conditional-expect */
                expect(e.name).toEqual(MALFORMED_HTTP_RESPONSE_ERROR);
                /* eslint-enable jest/no-conditional-expect */
            }
        });

        describe('credentials', () => {
            it('uses the VITE_FETCH_CREDENTIALS env variable if specified', async () => {
                import.meta.env.VITE_FETCH_CREDENTIALS = 'include';

                vi.spyOn(global, 'fetch');

                const endpoint = `/api/tablets`;
                const response = { ok: true, result: null };
                mockServerJson(endpoint, response);

                await api.vtfetch(endpoint);
                expect(global.fetch).toHaveBeenCalledTimes(1);
                expect(global.fetch).toHaveBeenCalledWith(`${import.meta.env.VITE_VTADMIN_API_ADDRESS}${endpoint}`, {
                    credentials: 'include',
                });

                vi.restoreAllMocks();
            });

            it('uses the fetch default `credentials` property by default', async () => {
                vi.spyOn(global, 'fetch');

                const endpoint = `/api/tablets`;
                const response = { ok: true, result: null };
                mockServerJson(endpoint, response);

                await api.vtfetch(endpoint);
                expect(global.fetch).toHaveBeenCalledTimes(1);
                expect(global.fetch).toHaveBeenCalledWith(`${import.meta.env.VITE_VTADMIN_API_ADDRESS}${endpoint}`, {
                    credentials: undefined,
                });

                vi.restoreAllMocks();
            });

            it('throws an error if an invalid value used for `credentials`', async () => {
                (process as any).env.VITE_FETCH_CREDENTIALS = 'nope';

                vi.spyOn(global, 'fetch');

                const endpoint = `/api/tablets`;
                const response = { ok: true, result: null };
                mockServerJson(endpoint, response);

                try {
                    await api.vtfetch(endpoint);
                } catch (error) {
                    let e: HttpFetchError = error as HttpFetchError;
                    /* eslint-disable jest/no-conditional-expect */
                    expect(e.message).toEqual(
                        'Invalid fetch credentials property: nope. Must be undefined or one of omit, same-origin, include'
                    );
                    expect(global.fetch).toHaveBeenCalledTimes(0);

                    expect(errorHandler.notify).toHaveBeenCalledTimes(1);
                    expect(errorHandler.notify).toHaveBeenCalledWith(e);
                    /* eslint-enable jest/no-conditional-expect */
                }

                vi.restoreAllMocks();
            });
        });

        it('allows GET requests when in read only mode', async () => {
            (process as any).env.VITE_READONLY_MODE = 'true';

            const endpoint = `/api/tablets`;
            const response = { ok: true, result: null };
            mockServerJson(endpoint, response);

            const result1 = await api.vtfetch(endpoint);
            expect(result1).toEqual(response);

            const result2 = await api.vtfetch(endpoint, { method: 'get' });
            expect(result2).toEqual(response);
        });

        it('throws an error when executing a write request in read only mode', async () => {
            (process as any).env.VITE_READONLY_MODE = 'true';

            vi.spyOn(global, 'fetch');

            // Endpoint doesn't really matter here since the point is that we don't hit it
            const endpoint = `/api/fake`;
            const response = { ok: true, result: null };
            mockServerJson(endpoint, response);

            const blockedMethods = ['post', 'POST', 'put', 'PUT', 'delete', 'DELETE'];
            for (let i = 0; i < blockedMethods.length; i++) {
                const method = blockedMethods[i];
                try {
                    await api.vtfetch(endpoint, { method });
                } catch (e: any) {
                    /* eslint-disable jest/no-conditional-expect */
                    expect(e.message).toEqual(`Cannot execute write request in read-only mode: ${method} ${endpoint}`);
                    expect(global.fetch).toHaveBeenCalledTimes(0);

                    expect(errorHandler.notify).toHaveBeenCalledTimes(1);
                    expect(errorHandler.notify).toHaveBeenCalledWith(e);
                    /* eslint-enable jest/no-conditional-expect */
                }

                vi.clearAllMocks();
            }

            vi.restoreAllMocks();
        });
    });

    describe('vtfetchEntities', () => {
        it('throws an error if result.tablets is not an array', async () => {
            const endpoint = '/api/foos';
            mockServerJson(endpoint, { ok: true, result: { foos: null } });

            expect.assertions(3);

            try {
                await api.vtfetchEntities({
                    endpoint,
                    extract: (res) => res.result.foos,
                    transform: (e) => null, // doesn't matter
                });
            } catch (error) {
                let e: HttpFetchError = error as HttpFetchError;
                /* eslint-disable jest/no-conditional-expect */
                expect(e.message).toMatch('expected entities to be an array, got null');

                expect(errorHandler.notify).toHaveBeenCalledTimes(1);
                expect(errorHandler.notify).toHaveBeenCalledWith(e);
                /* eslint-enable jest/no-conditional-expect */
            }
        });
    });
});
