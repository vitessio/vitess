/**
 * Copyright 2023 The Vitess Authors.
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
import { setupServer } from 'msw/node'
import { handlers } from './handlers'
// This configures a Service Worker with the given request handlers.
// Our test suites use Mock Service Workers (https://github.com/mswjs/msw)
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
export const server = setupServer(...handlers)