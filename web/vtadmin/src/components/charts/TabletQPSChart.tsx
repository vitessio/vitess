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
import { useMemo } from 'react';

import { useExperimentalTabletDebugVars } from '../../hooks/api';
import { getQPSTimeseries, QPS_REFETCH_INTERVAL } from '../../util/tabletDebugVars';
import { mergeOptions } from './chartOptions';
import { Timeseries } from './Timeseries';

interface Props {
    alias: string;
    clusterID: string;
}

export const TabletQPSChart = ({ alias, clusterID }: Props) => {
    const { data: debugVars, ...query } = useExperimentalTabletDebugVars(
        { alias, clusterID },
        {
            refetchInterval: QPS_REFETCH_INTERVAL,
            refetchIntervalInBackground: true,
        }
    );

    const options = useMemo(() => {
        const tsdata = getQPSTimeseries(debugVars, query.dataUpdatedAt);

        const series: Highcharts.SeriesOptionsType[] = Object.entries(tsdata).map(([name, data]) => ({
            data,
            name,
            type: 'line',
        }));

        return mergeOptions({ series });
    }, [debugVars, query.dataUpdatedAt]);

    return <Timeseries isLoading={query.isLoading} options={options} />;
};
