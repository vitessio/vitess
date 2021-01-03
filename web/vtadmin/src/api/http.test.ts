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

const SERVER_ADDRESS = '';
process.env.REACT_APP_VTADMIN_API_ADDRESS = SERVER_ADDRESS;

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
                expect(e.message).toEqual('invalid http envelope');
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
