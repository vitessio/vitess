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
import * as dayjs from 'dayjs';
import localizedFormat from 'dayjs/plugin/localizedFormat';
import relativeTime from 'dayjs/plugin/relativeTime';

dayjs.extend(localizedFormat);
dayjs.extend(relativeTime);

export const parse = (timestamp: number | Long | null | undefined): dayjs.Dayjs | null => {
    if (typeof timestamp !== 'number') {
        return null;
    }
    return dayjs.unix(timestamp);
};

export const format = (timestamp: number | Long | null | undefined, template: string | undefined): string | null => {
    const u = parse(timestamp);
    return u ? u.format(template) : null;
};

export const formatDateTime = (timestamp: number | Long | null | undefined): string | null => {
    return format(timestamp, 'YYYY-MM-DD LT Z');
};

export const formatRelativeTime = (timestamp: number | Long | null | undefined): string | null => {
    const u = parse(timestamp);
    return u ? u.fromNow() : null;
};
