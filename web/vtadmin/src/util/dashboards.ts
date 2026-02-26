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

/**
 * Replaces placeholders in a template string with values from a params object.
 * Only placeholders present in the template will be replaced.
 * Example: formatDashboardUrl("https://grafana.com/d/123?var-cluster={cluster}", { cluster: "prod" })
 *
 * Supported placeholders: {cluster}, {keyspace}, {shard}, {alias}, {hostname}, {type}, {cell}, {pool}
 */
export const formatDashboardUrl = (
    template: string | undefined,
    params: Record<string, string | undefined | null>
): string | null => {
    if (!template) return null;

    let url = template;
    Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined && value !== null) {
            // Replace all occurrences of {key}
            url = url.replace(new RegExp(`{${key}}`, 'g'), value);
        }
    });

    return url;
};
