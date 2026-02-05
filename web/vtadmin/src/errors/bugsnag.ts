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
import BugsnagJS from '@bugsnag/js';
import { ErrorHandler } from './errorTypes';
import { env } from '../util/env';

const { VITE_BUGSNAG_API_KEY } = env();

/**
 * If using Bugsnag, Bugsnag.start() will automatically capture and report
 * unhandled exceptions and unhandled promise rejections, as well as
 * initialize it for capturing handled errors.
 */
export const initialize = () => {
    if (typeof VITE_BUGSNAG_API_KEY === 'string' && VITE_BUGSNAG_API_KEY.length) {
        BugsnagJS.start(VITE_BUGSNAG_API_KEY);
    }
};

export const isEnabled = () => typeof VITE_BUGSNAG_API_KEY === 'string';

export const notify = (error: Error, env: object, metadata?: object) => {
    // See https://docs.bugsnag.com/platforms/javascript/reporting-handled-errors/
    BugsnagJS.notify(error, (event) => {
        event.addMetadata('env', env);

        if (!!metadata) {
            event.addMetadata('metadata', metadata);
        }
    });
};

export const Bugsnag: ErrorHandler = {
    initialize,
    isEnabled,
    notify,
};
