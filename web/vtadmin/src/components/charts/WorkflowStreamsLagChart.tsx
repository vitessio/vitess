/**
 * Copyright 2024 The Vitess Authors.
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
import { getStreamVReplicationLagTimeseries, QPS_REFETCH_INTERVAL, TimeseriesMap } from '../../util/tabletDebugVars';
import { formatStreamKey, getStreams, getStreamTablets } from '../../util/workflows';
import { D3Timeseries } from './D3Timeseries';

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

    const timeseries = useMemo(() => {
        return getWorkflowTimeseries(workflow, tabletQueries);
    }, [tabletQueries, workflow]);

    return <D3Timeseries isLoading={anyLoading} timeseriesMap={timeseries} />;
};

export const getWorkflowTimeseries = (
    workflow: vtadmin.Workflow | null | undefined,
    tabletQueries: ReturnType<typeof useManyExperimentalTabletDebugVars>
): TimeseriesMap => {
    if (!workflow) {
        return {} as TimeseriesMap;
    }

    // Get streamKeys for streams in this workflow.
    const streamKeys = getStreams(workflow).map((s) => formatStreamKey(s));

    // Initialize the timeseries from the workflow, so that every stream in the workflow
    // is shown in the legend, even if the /debug/vars data isn't (yet) available.
    const seriesByStreamKey: TimeseriesMap = {};
    streamKeys.forEach((streamKey) => {
        if (streamKey) {
            seriesByStreamKey[streamKey] = [];
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

            seriesByStreamKey[streamKey] = streamLagData;
        });
    });

    return seriesByStreamKey;
};
