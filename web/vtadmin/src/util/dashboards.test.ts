/**
 * Copyright 2025 The Vitess Authors.
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

import { describe, expect, it } from 'vitest';
import { formatDashboardUrl } from './dashboards';

describe('formatDashboardUrl', () => {
    it('returns null if template is undefined', () => {
        expect(formatDashboardUrl(undefined, {})).toBeNull();
    });

    it('returns the template as-is if no params are provided', () => {
        const template = 'https://example.com';
        expect(formatDashboardUrl(template, {})).toBe(template);
    });

    it('replaces placeholders with values', () => {
        const template = 'https://example.com/cluster/{cluster}/keyspace/{keyspace}';
        const params = { cluster: 'prod', keyspace: 'commerce' };
        expect(formatDashboardUrl(template, params)).toBe('https://example.com/cluster/prod/keyspace/commerce');
    });

    it('handles multiple occurrences of the same placeholder', () => {
        const template = 'https://example.com/c/{cluster}/d/{cluster}';
        const params = { cluster: 'prod' };
        expect(formatDashboardUrl(template, params)).toBe('https://example.com/c/prod/d/prod');
    });

    it('ignores placeholders that are not in params', () => {
        const template = 'https://example.com/{cluster}/{missing}';
        const params = { cluster: 'prod' };
        expect(formatDashboardUrl(template, params)).toBe('https://example.com/prod/{missing}');
    });

    it('ignores params that are undefined or null', () => {
        const template = 'https://example.com/{cluster}';
        const params = { cluster: undefined };
        expect(formatDashboardUrl(template, params)).toBe('https://example.com/{cluster}');
    });
});
