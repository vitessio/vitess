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

const BASE = 1024;
const PRECISION = 2;
const UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];

export const formatBytes = (
    bytes: number | Long | string | null | undefined,
    units?: string | null | undefined
): string | null => {
    if (bytes === null || typeof bytes === 'undefined') return null;

    const num = Number(bytes);
    if (isNaN(num)) return null;

    const i = units ? UNITS.indexOf(units) : Math.floor(Math.log(num) / Math.log(BASE));
    if (i < 0) return null;

    return parseFloat((num / Math.pow(BASE, i)).toFixed(PRECISION)).toLocaleString() + ' ' + UNITS[i];
};
