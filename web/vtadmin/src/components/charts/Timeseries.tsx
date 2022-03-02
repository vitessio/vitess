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
import HighchartsReact from 'highcharts-react-official';
import { useEffect, useMemo, useRef } from 'react';
import { mergeOptions } from './chartOptions';

interface Props {
    isLoading?: boolean;
    options: Highcharts.Options | undefined;
}

export const Timeseries = ({ isLoading, options }: Props) => {
    // See https://github.com/highcharts/highcharts-react/issues/290#issuecomment-802914598
    const ref = useRef<{
        chart: Highcharts.Chart;
        container: React.RefObject<HTMLDivElement>;
    }>(null);

    useEffect(() => {
        if (!ref.current) {
            return;
        }

        if (isLoading) {
            ref.current.chart.showLoading();
        } else {
            ref.current.chart.hideLoading();
        }
    }, [isLoading]);

    const _options = useMemo(() => {
        return mergeOptions(
            {
                tooltip: {
                    hideDelay: 0,
                    shared: true,
                },
                xAxis: {
                    crosshair: true,
                    type: 'datetime',
                },
                yAxis: {
                    crosshair: true,
                    // Setting `softMax` to any positive integer will anchor the y=0 gridline
                    // at the bottom of the chart even when there is no data to show.
                    softMax: 1,
                    softMin: 0,
                    title: {
                        text: undefined,
                    },
                },
            },
            options
        );
    }, [options]);

    return <HighchartsReact highcharts={Highcharts} options={_options} ref={ref} />;
};
