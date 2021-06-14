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
        return {
            series: formatSeries(workflow, tabletQueries),
            yAxis: {
                labels: {
                    format: '{text} s',
                },
            },
        };
    }, [tabletQueries, workflow]);

    return <Timeseries isLoading={anyLoading} options={chartOptions} />;
};

export const formatSeries = (
    workflow: vtadmin.Workflow | null | undefined,
    tabletQueries: ReturnType<typeof useManyExperimentalTabletDebugVars>
): Highcharts.SeriesOptionsType[] => {
    if (!workflow) {
        return [];
    }

    // Get streamKeys for streams in this workflow.
    const streamKeys = getStreams(workflow).map((s) => formatStreamKey(s));

    return tabletQueries.reduce((acc, tq) => {
        if (!tq.data) {
            return acc;
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

            // Don't graph series for streams that aren't in this workflow.
            const streamKey = `${tabletAlias}/${streamID}`;
            if (streamKeys.indexOf(streamKey) < 0) {
                return;
            }

            acc.push({ data: streamLagData, name: streamKey, type: 'line' });
        });

        return acc;
    }, [] as Highcharts.SeriesOptionsType[]);
};
