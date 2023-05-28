/* eslint-disable jest/no-conditional-expect */
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

import { formatTimeseriesMap, RATES_INTERVAL, RATES_MAX_SPAN } from './tabletDebugVars';
import { describe, expect, test, vi } from 'vitest';

/*
 * These tests are (at the time of writing) one of the few places we
 * use snapshot testing (or "golden tests"), since hand-writing
 * timeseries data is tedious, and `formatTimeseriesMap`'s implementation
 * is unlikely to change very often.
 */
describe('formatTimeseriesMap', () => {
    const tests: {
        name: string;
        input: Parameters<typeof formatTimeseriesMap>;
    }[] = [
        {
            name: 'empty input',
            input: [{}],
        },
        {
            name: 'timespan < max span',
            input: [
                {
                    All: [1, 2, 3],
                },
            ],
        },
        {
            name: 'multiple series',
            input: [
                {
                    All: [3, 4, 5],
                    First: [1, 2, 3],
                    Second: [4, 5, 6],
                },
            ],
        },
    ];

    test.each(tests.map(Object.values))('%s', (name: string, input: Parameters<typeof formatTimeseriesMap>) => {
        Date.now = vi.fn(() => 1000000000000);

        const result = formatTimeseriesMap(...input);

        // Who wants to define an `expected` array of 180 items inline?
        // This is an ideal situation for snapshot testing.
        expect(result).toMatchSnapshot({}, name);

        // We can trust our snapshots... but additional validation doesn't hurt. B-)
        const [inputData, endAt] = input;
        const endAtExpected = typeof endAt === 'number' ? endAt : Date.now();
        const startAtExpected = endAtExpected - ((179 * 60) / 5) * 1000;

        const inputKeys = Object.keys(inputData);
        const outputKeys = Object.keys(result);

        if (inputKeys.length) {
            expect(outputKeys).toEqual(inputKeys);
        } else {
            expect(outputKeys).toEqual(['All']);
        }

        Object.entries(result).forEach(([seriesName, seriesData]) => {
            expect(seriesData.length).toEqual(RATES_MAX_SPAN / RATES_INTERVAL);
            expect(seriesData[0].x).toEqual(startAtExpected);
            expect(seriesData[seriesData.length - 1].x).toEqual(endAtExpected);
        });
    });
});
