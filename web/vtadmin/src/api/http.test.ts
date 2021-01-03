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
process.env.REACT_APP_VTADMIN_API_ADDRESS = '';
const server = setupServer();

const mockServerJson = (endpoint: string, json: object) => {
    server.use(rest.get(endpoint, (req, res, ctx) => res(ctx.json(json))));
};

// Enable API mocking before tests.
beforeAll(() => server.listen());

// Reset any runtime request handlers we may add during the tests.
afterEach(() => server.resetHandlers());

// Disable API mocking after the tests are done.
afterAll(() => server.close());

describe('api/http', () => {
    describe('vtfetch', () => {
        it('parses and returns JSON, given an HttpOkResponse response', async () => {
            const endpoint = `/api/tablets`;
            const response = { ok: true, result: null };
            mockServerJson(endpoint, response);

            const result = await api.vtfetch(endpoint);
            expect(result).toEqual(response);
        });

        it('parses and returns JSON, given an HttpErrorResponse response', async () => {
            const endpoint = `/api/tablets`;
            const response = { ok: false };
            mockServerJson(endpoint, response);

            const result = await api.vtfetch(endpoint);
            expect(result).toEqual(response);
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
    });

    describe('fetchTablets', () => {
        it('returns a list of Tablets, given a successful response', async () => {
            const t0 = pb.Tablet.create({ tablet: { hostname: 't0' } });
            const t1 = pb.Tablet.create({ tablet: { hostname: 't1' } });
            const t2 = pb.Tablet.create({ tablet: { hostname: 't2' } });
            const tablets = [t0, t1, t2];

            mockServerJson(`/api/tablets`, {
                ok: true,
                result: {
                    tablets: tablets.map((t) => t.toJSON()),
                },
            });

            const result = await api.fetchTablets();
            expect(result).toEqual(tablets);
        });

        it('throws an error if response.ok is false', async () => {
            const response = { ok: false };
            mockServerJson('/api/tablets', response);

            expect.assertions(3);

            try {
                await api.fetchTablets();
            } catch (e) {
                /* eslint-disable jest/no-conditional-expect */
                expect(e.name).toEqual(HTTP_RESPONSE_NOT_OK_ERROR);
                expect(e.message).toEqual('/api/tablets');
                expect(e.response).toEqual(response);
                /* eslint-enable jest/no-conditional-expect */
            }
        });

        it('throws an error if result.tablets is not an array', async () => {
            mockServerJson('/api/tablets', { ok: true, result: { tablets: null } });

            expect.assertions(1);

            try {
                await api.fetchTablets();
            } catch (e) {
                /* eslint-disable jest/no-conditional-expect */
                expect(e.message).toMatch('expected tablets to be an array');
                /* eslint-enable jest/no-conditional-expect */
            }
        });

        it('throws an error if JSON cannot be unmarshalled into Tablet objects', async () => {
            mockServerJson(`/api/tablets`, {
                ok: true,
                result: {
                    tablets: [{ cluster: 'this should be an object, not a string' }],
                },
            });

            expect.assertions(1);

            try {
                await api.fetchTablets();
            } catch (e) {
                /* eslint-disable jest/no-conditional-expect */
                expect(e.message).toEqual('cluster.object expected');
                /* eslint-enable jest/no-conditional-expect */
            }
        });
    });
});
