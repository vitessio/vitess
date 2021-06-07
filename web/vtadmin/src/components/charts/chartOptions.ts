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
import Highcharts from 'highcharts';

export const mergeOptions = (
    o: Highcharts.Options | undefined,
    ...opts: (Highcharts.Options | undefined)[]
): Highcharts.Options => {
    return Highcharts.merge({}, DEFAULT_OPTIONS, o, ...opts);
};

/**
 * Default options applicable to all Highcharts charts.
 * Individual chart instances can override any/all of these
 * default options with `mergeOptions`.
 */
export const DEFAULT_OPTIONS: Highcharts.Options = {
    chart: {
        animation: false,
        // Enable styled mode by default, so we can use our existing CSS variables.
        // See https://www.highcharts.com/docs/chart-design-and-style/style-by-css
        styledMode: true,
    },
    credits: {
        enabled: false,
    },
    plotOptions: {
        series: {
            animation: false,
        },
    },
    time: {
        useUTC: false,
    },
    // Using Highcharts' built in `title` property is not recommended.
    // In most cases, using a regular heading element like <h2>
    // adjacent to the chart is more flexible and consistent,
    // since we can't apply layout-type rules like line-height or margin.
    title: {
        text: undefined,
    },
};
