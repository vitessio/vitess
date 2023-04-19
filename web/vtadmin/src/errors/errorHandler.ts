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

import { pick } from 'lodash';
import { getHandlers } from './errorHandlers';
import { env } from '../util/env';

/**
 * Initializes error handling for both unhandled and handled exceptions.
 * This should be called as early as possible.
 */
export const initialize = () => {
    getHandlers().forEach((h) => h.initialize());
};

/**
 * Manually notify error handlers of an error. Also known as
 * a "handled error".
 *
 * @param error - The Error that was thrown/captured.
 * @param metadata - Additional key/value metadata to log. Note that
 * additional metadata from `error` will be added to the metadata
 * object under the key "errorMetadata" before it is passed along
 * to the active ErrorHandler clients(s).
 */
export const notify = (error: Error, metadata?: object) => {
    const env = sanitizeEnv();
    const errorMetadata = Object.getOwnPropertyNames(error).reduce((acc, propertyName) => {
        // Only annotate the `metadata` object with properties beyond the standard instance
        // properties. (Bugsnag, for example, does not log additional `Error` properties:
        // they have to be logged as additional metadata.)
        if (propertyName !== 'stack' && propertyName !== 'message') {
            acc[propertyName] = (error as any)[propertyName];
        }

        return acc;
    }, {} as { [k: string]: any });

    getHandlers().forEach((h) =>
        h.notify(error, env, {
            errorMetadata,
            ...metadata,
        })
    );
};

/**
 * sanitizeEnv serializes import.meta.env into an object that's sent to
 * configured error handlers, for extra debugging context.
 * Implemented as an allow list, rather than as a block list, to avoid
 * leaking sensitive environment variables, like API keys.
 */
const sanitizeEnv = () =>
    pick(env(), [
        'VITE_BUILD_BRANCH',
        'VITE_BUILD_SHA',
        'VITE_ENABLE_EXPERIMENTAL_TABLET_DEBUG_VARS',
        'VITE_FETCH_CREDENTIALS',
        'VITE_VTADMIN_API_ADDRESS',
    ]);
