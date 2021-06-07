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

/**
 * TabletDebugVars is a best-effort typing of the /debug/vars tablet endpoint.
 * Only fields read by VTAdmin are defined here.
 *
 * A good future enhancement is a proto-typed gRPC endpoint in vtadmin.proto,
 * from which we can generate TypeScript typings vs. having to duplicate them here.
 * This would also offer us actual runtime type safety by way of protobufjs's
 * generated validate() functions. For now, everything in here is optional (hence
 * the Partial<>) and not guaranteed to be defined in every Vitess deployment.
 */
export type TabletDebugVars = Partial<{
    // See https://github.com/vitessio/vitess/blob/main/go/vt/servenv/buildinfo.go
    BuildGitBranch: string;
    BuildGitRev: string;
    BuildHost: string;
    BuildInformation: { [k: string]: number | string };
    BuildNumber: string;
    BuildTimestamp: string;
    BuildUser: string;

    QPS: { [k: string]: number[] };

    // See https://github.com/vitessio/vitess/blob/main/go/vt/vttablet/tabletmanager/vreplication/stats.go
    VReplicationQPS: { [k: string]: number[] };
}>;

export interface TimeseriesPoint {
    x: number;
    y: number;
}

export type TimeseriesMap = { [seriesName: string]: TimeseriesPoint[] };

export const getQPSTimeseries = (d: TabletDebugVars | null | undefined, endAt?: number): TimeseriesMap =>
    formatTimeseriesMap(d?.QPS || {}, endAt);

export const getVReplicationQPSTimeseries = (d: TabletDebugVars | null | undefined, endAt?: number): TimeseriesMap =>
    formatTimeseriesMap(d?.VReplicationQPS || {}, endAt);

/**
 * REFETCH_INTERVAL defines a default for how often charts, etc.
 * should refetch /debug/vars.
 */
export const QPS_REFETCH_INTERVAL = 2500;

export const RATES_INTERVAL = 5; // 5 seconds
export const RATES_MAX_SPAN = 15 * 60; // 15 minutes
export const SERIES_LENGTH = RATES_MAX_SPAN / RATES_INTERVAL;

/**
 * formatTimeseriesMap formats an array of numbers from a Rates stats counter as
 * an array of TimeseriesPoints.
 *
 * For Rates stats, see https://github.com/vitessio/vitess/blob/main/go/stats/rates.go
 *
 * @param rates - An array of numbers, inferred as data points at 5 second intervals.
 *
 * @param endAt - Optional. The timestamp for the last (most recent) data point in the series.
 * The `dataUpdatedAt` property of a query is recommended. Defaults to Date.now() if unspecified.
 */
export const formatTimeseriesMap = (rates: { [k: string]: number[] }, endAt?: number): TimeseriesMap => {
    // Rates stats are (unfortunately) not returned with timestamps, so we infer them here.
    // This behaviour matches that of the vtctld2 UI:
    // https://github.com/vitessio/vitess/blob/main/go/vt/vttablet/tabletserver/status.go#L178
    const _endAt = typeof endAt === 'number' ? endAt : Date.now();

    // The `rates` map returned from the API, keyed by series name, will either:
    //      (a) be empty, or
    //      (b) contain a minimum of two series, one of them named "All".
    // In the first case, inserting an empty "All" series renders more nicely
    // on a Highcharts graph since it will include the axes, etc. So, we add it here.
    const _rates = !!Object.keys(rates).length ? rates : { All: [] };

    const planTypes = Object.keys(_rates);

    const data: TimeseriesMap = {};

    // This logic is borrowed from the vtctld2 UI
    // https://github.com/vitessio/vitess/blob/09e74af/go/vt/vttablet/tabletmanager/vreplication/stats.go#L478-L505
    for (let i = 0; i < RATES_MAX_SPAN / RATES_INTERVAL; i++) {
        const x = _endAt - ((i * 60) / RATES_INTERVAL) * 1000;

        for (let j = 0; j < planTypes.length; j++) {
            // Assume 0.0 QPS for older, non-existent data points.
            let y = 0;

            if (i < _rates[planTypes[j]].length) {
                const idx = _rates[planTypes[j]].length - i - 1;
                y = +_rates[planTypes[j]][idx].toFixed(2);
            }

            if (!Array.isArray(data[planTypes[j]])) {
                data[planTypes[j]] = [];
            }

            data[planTypes[j]].unshift({ x, y });
        }
    }

    return data;
};
