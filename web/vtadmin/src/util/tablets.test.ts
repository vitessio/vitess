/**
 * Copyright 2022 The Vitess Authors.
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

import * as tablets from './tablets';

describe('formatAlias', () => {
    it('pads with leading zeroes', () => {
        expect(
            tablets.formatAlias({
                cell: 'zone1',
                uid: 100,
            })
        ).toEqual('zone1-0000000100');
    });

    it('does not pad a 10-digit uid', () => {
        expect(
            tablets.formatAlias({
                cell: 'zone1',
                uid: 4294967295,
            })
        ).toEqual('zone1-4294967295');
    });

    it('formats a uid of 0', () => {
        expect(
            tablets.formatAlias({
                cell: 'zone1',
                uid: 0,
            })
        ).toEqual('zone1-0000000000');
    });

    it('handles null input', () => {
        expect(tablets.formatAlias(null)).toEqual(null);
    });

    it('handles invalid cell', () => {
        expect(
            tablets.formatAlias({
                cell: null,
                uid: 100,
            })
        ).toEqual(null);
    });

    it('handles invalid uid', () => {
        expect(
            tablets.formatAlias({
                cell: 'zone1',
                uid: null,
            })
        ).toEqual(null);
    });
});
