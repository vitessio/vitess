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

import { useMemo } from 'react';

import { useManyExperimentalTabletDebugVars, useWorkflow } from '../../hooks/api';
import { vtadmin } from '../../proto/vtadmin';
import { getStreamVReplicationLagTimeseries, QPS_REFETCH_INTERVAL } from '../../util/tabletDebugVars';
import { formatStreamKey, getStreams, getStreamTablets } from '../../util/workflows';
import { Timeseries } from './Timeseries';

interface Props {
    clusterID: string;
    keyspace: string;
    workflowName: string;
}

// Default min/max values (in seconds) for the y-axis when there is no data to show.
const DEFAULT_Y_MAX = 5;
const DEFAULT_Y_MIN = 0;

export const WorkflowStreamsLagChart = ({ clusterID, keyspace, workflowName }: Props) => {
    const { data: workflow, ...wq } = useWorkflow({ clusterID, keyspace, name: workflowName });

    const queryParams = useMemo(() => {
        const aliases = getStreamTablets(workflow);
        return aliases.map((alias) => ({ alias, clusterID }));
    }, [clusterID, workflow]);

    const tabletQueries = useManyExperimentalTabletDebugVars(queryParams, {
        enabled: !!workflow,
        refetchInterval: QPS_REFETCH_INTERVAL,
        refetchIntervalInBackground: true,
    });

    const anyLoading = wq.isLoading || tabletQueries.some((q) => q.isLoading);

    const chartOptions: Highcharts.Options = useMemo(() => {
        const series = formatSeries(workflow, tabletQueries);
        const allSeriesEmpty = series.every((s) => !s.data?.length);

        return {
            series,
            yAxis: {
                labels: {
                    format: '{text} s',
                },
                // The desired behaviour is to show axes + grid lines
                // even when there is no data to show. Unfortunately, setting
                // softMin/softMax (which is more flexible) doesn't work with showEmpty.
                // Instead, we must set explicit min/max, but only when all series are empty.
                // If at least one series has data, allow min/max to be automatically calculated.
                max: allSeriesEmpty ? DEFAULT_Y_MAX : null,
                min: allSeriesEmpty ? DEFAULT_Y_MIN : null,
            },
        };
    }, [tabletQueries, workflow]);

    return <Timeseries isLoading={anyLoading} options={chartOptions} />;
};

// Internal function, exported only for testing.
export const formatSeries = (
    workflow: vtadmin.Workflow | null | undefined,
    tabletQueries: ReturnType<typeof useManyExperimentalTabletDebugVars>
): Highcharts.SeriesLineOptions[] => {
    if (!workflow) {
        return [];
    }

    // Get streamKeys for streams in this workflow.
    const streamKeys = getStreams(workflow).map((s) => formatStreamKey(s));

    // Initialize the timeseries from the workflow, so that every stream in the workflow
    // is shown in the legend, even if the /debug/vars data isn't (yet) available.
    const seriesByStreamKey: { [streamKey: string]: Highcharts.SeriesLineOptions } = {};

    streamKeys.forEach((streamKey) => {
        if (streamKey) {
            seriesByStreamKey[streamKey] = { data: [], name: streamKey, type: 'line' };
        }
    });

    tabletQueries.forEach((tq) => {
        if (!tq.data) {
            return;
        }

        const tabletAlias = tq.data.params.alias;

        const lagData = getStreamVReplicationLagTimeseries(tq.data.data, tq.dataUpdatedAt);
        Object.entries(lagData).forEach(([streamID, streamLagData]) => {
            // Don't graph aggregate vreplication lag for the tablet, since that
            // can include vreplication lag data for streams running on the tablet
            // that are not in the current workflow.
            if (streamID === 'All') {
                return;
            }

            const streamKey = `${tabletAlias}/${streamID}`;

            // Don't graph series for streams that aren't in this workflow.
            if (!(streamKey in seriesByStreamKey)) {
                return;
            }

            seriesByStreamKey[streamKey].data = streamLagData;
        });
    });

    return Object.values(seriesByStreamKey);
};
