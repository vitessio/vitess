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
import { setupServer } from 'msw/node';

import * as api from './http';
import { vtadmin as pb } from '../proto/vtadmin';
import { HTTP_RESPONSE_NOT_OK_ERROR, MALFORMED_HTTP_RESPONSE_ERROR } from './http';

// This test suite uses Mock Service Workers (https://github.com/mswjs/msw)
// to mock HTTP responses from vtadmin-api.
//
// MSW lets us intercept requests at the network level. This decouples the tests from
// whatever particular HTTP fetcher interface we are using, and obviates the need
// to mock `fetch` directly (by using a library like jest-fetch-mock, for example).
//
// MSW gives us full control over the response, including edge cases like errors,
// malformed payloads, and timeouts.
//
// The big downside to mocking or "faking" APIs like vtadmin is that
// we end up re-implementing some (or all) of vtadmin-api in our test environment.
// It is, unfortunately, impossible to completely avoid this kind of duplication
// unless we solely use e2e tests (which have their own trade-offs).
//
// That said, our use of protobufjs to validate and strongly type HTTP responses
// means our fake is more robust than it would be otherwise. Since we are using
// the exact same protos in our fake as in our real vtadmin-api server, we're guaranteed
// to have type parity.
const server = setupServer();

// mockServerJson configures an HttpOkResponse containing the given `json`
// for all requests made against the given `endpoint`.
const mockServerJson = (endpoint: string, json: object) => {
    server.use(rest.get(endpoint, (req, res, ctx) => res(ctx.json(json))));
};

// Since vtadmin uses process.env variables quite a bit, we need to
// do a bit of a dance to clear them out between test runs.
const ORIGINAL_PROCESS_ENV = process.env;
const TEST_PROCESS_ENV = {
    ...process.env,
    REACT_APP_VTADMIN_API_ADDRESS: '',
};

beforeAll(() => {
    process.env = { ...TEST_PROCESS_ENV };

    // Enable API mocking before tests.
    server.listen();
});

afterEach(() => {
    // Reset the process.env to clear out any changes made in the tests.
    process.env = { ...TEST_PROCESS_ENV };

    jest.restoreAllMocks();

    // Reset any runtime request handlers we may add during the tests.
    server.resetHandlers();
});

afterAll(() => {
    process.env = { ...ORIGINAL_PROCESS_ENV };

    // Disable API mocking after the tests are done.
    server.close();
});

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
            const response = { ok: false };
            mockServerJson(endpoint, response);

            expect.assertions(3);

            try {
                await api.fetchTablets();
            } catch (e) {
                /* eslint-disable jest/no-conditional-expect */
                expect(e.name).toEqual(HTTP_RESPONSE_NOT_OK_ERROR);
                expect(e.message).toEqual(endpoint);
                expect(e.response).toEqual(response);
                /* eslint-enable jest/no-conditional-expect */
            }
        });

        it('throws an error on malformed JSON', async () => {
            const endpoint = `/api/tablets`;
            server.use(rest.get(endpoint, (req, res, ctx) => res(ctx.body('this is fine'))));

            expect.assertions(2);

            try {
                await api.vtfetch(endpoint);
            } catch (e) {
                /* eslint-disable jest/no-conditional-expect */
                expect(e.name).toEqual('SyntaxError');
                expect(e.message.startsWith('Unexpected token')).toBeTruthy();
                /* eslint-enable jest/no-conditional-expect */
            }
        });

        it('throws an error on malformed response envelopes', async () => {
            const endpoint = `/api/tablets`;
            mockServerJson(endpoint, { foo: 'bar' });

            expect.assertions(1);

            try {
                await api.vtfetch(endpoint);
            } catch (e) {
                /* eslint-disable jest/no-conditional-expect */
                expect(e.name).toEqual(MALFORMED_HTTP_RESPONSE_ERROR);
                /* eslint-enable jest/no-conditional-expect */
            }
        });

        describe('credentials', () => {
            it('uses the REACT_APP_FETCH_CREDENTIALS env variable if specified', async () => {
                process.env.REACT_APP_FETCH_CREDENTIALS = 'include';

                jest.spyOn(global, 'fetch');

                const endpoint = `/api/tablets`;
                const response = { ok: true, result: null };
                mockServerJson(endpoint, response);

                await api.vtfetch(endpoint);
                expect(global.fetch).toHaveBeenCalledTimes(1);
                expect(global.fetch).toHaveBeenCalledWith(endpoint, { credentials: 'include' });

                jest.restoreAllMocks();
            });

            it('uses the fetch default `credentials` property by default', async () => {
                jest.spyOn(global, 'fetch');

                const endpoint = `/api/tablets`;
                const response = { ok: true, result: null };
                mockServerJson(endpoint, response);

                await api.vtfetch(endpoint);
                expect(global.fetch).toHaveBeenCalledTimes(1);
                expect(global.fetch).toHaveBeenCalledWith(endpoint, { credentials: undefined });

                jest.restoreAllMocks();
            });

            it('throws an error if an invalid value used for `credentials`', async () => {
                (process as any).env.REACT_APP_FETCH_CREDENTIALS = 'nope';

                jest.spyOn(global, 'fetch');

                const endpoint = `/api/tablets`;
                const response = { ok: true, result: null };
                mockServerJson(endpoint, response);

                try {
                    await api.vtfetch(endpoint);
                } catch (e) {
                    /* eslint-disable jest/no-conditional-expect */
                    expect(e.message).toEqual(
                        'Invalid fetch credentials property: nope. Must be undefined or one of omit, same-origin, include'
                    );
                    expect(global.fetch).toHaveBeenCalledTimes(0);
                    /* eslint-enable jest/no-conditional-expect */
                }

                jest.restoreAllMocks();
            });
        });
    });

    describe('vtfetchEntities', () => {
        it('throws an error if result.tablets is not an array', async () => {
            const endpoint = '/api/foos';
            mockServerJson(endpoint, { ok: true, result: { foos: null } });

            expect.assertions(1);

            try {
                await api.vtfetchEntities({
                    endpoint,
                    extract: (res) => res.result.foos,
                    transform: (e) => null, // doesn't matter
                });
            } catch (e) {
                /* eslint-disable jest/no-conditional-expect */
                expect(e.message).toMatch('expected entities to be an array, got null');
                /* eslint-enable jest/no-conditional-expect */
            }
        });
    });
});
