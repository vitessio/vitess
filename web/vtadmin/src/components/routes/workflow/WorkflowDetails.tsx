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

import { orderBy } from 'lodash-es';
import React, { useMemo } from 'react';
import { Link } from 'react-router-dom';

import { useWorkflow, useWorkflowStatus, useWorkflows } from '../../../hooks/api';
import { formatDateTime } from '../../../util/time';
import {
    TableCopyState,
    formatStreamKey,
    getReverseWorkflow,
    getStreams,
    getTableCopyStates,
} from '../../../util/workflows';
import { DataTable } from '../../dataTable/DataTable';
import { vtctldata } from '../../../proto/vtadmin';
import { DataCell } from '../../dataTable/DataCell';
import { StreamStatePip } from '../../pips/StreamStatePip';
import { ThrottleThresholdSeconds } from '../Workflows';

interface Props {
    clusterID: string;
    keyspace: string;
    name: string;
}

const LOG_COLUMNS = ['Type', 'State', 'Updated At', 'Message', 'Count'];

const TABLE_COPY_STATE_COLUMNS = ['Table Name', 'Total Bytes', 'Bytes Copied', 'Total Rows', 'Rows Copied'];

export const WorkflowDetails = ({ clusterID, keyspace, name }: Props) => {
    const { data } = useWorkflow({ clusterID, keyspace, name });

    const { data: workflowsData = [] } = useWorkflows();

    const { data: workflowStatus } = useWorkflowStatus({
        clusterID,
        keyspace,
        name,
    });

    const streams = useMemo(() => {
        const rows = getStreams(data).map((stream) => ({
            key: formatStreamKey(stream),
            ...stream,
        }));

        return orderBy(rows, 'streamKey');
    }, [data]);

    const renderRows = (rows: vtctldata.Workflow.Stream.ILog[]) => {
        return rows.map((row) => {
            let message = row.message ? `${row.message}` : '-';
            // TODO(@beingnoble03): Investigate how message can be parsed and displayed to JSON in case of "Stream Created"
            if (row.type == 'Stream Created') {
                message = '-';
            }
            return (
                <tr key={`${row.id}`}>
                    <DataCell>{`${row.type}`}</DataCell>
                    <DataCell>{`${row.state}`}</DataCell>
                    <DataCell>{`${formatDateTime(parseInt(`${row.updated_at?.seconds}`, 10))}`}</DataCell>
                    <DataCell>{message}</DataCell>
                    <DataCell>{`${row.count}`}</DataCell>
                </tr>
            );
        });
    };

    const reverseWorkflow = getReverseWorkflow(workflowsData, data);

    const tableCopyStates = getTableCopyStates(workflowStatus);

    const renderTableCopyStateRows = (tableCopyStates: TableCopyState[]) => {
        return tableCopyStates.map((copyState, index) => {
            const tableKey = `${copyState.tableName}/${index}`;
            return (
                <tr key={tableKey}>
                    <DataCell>{`${copyState.tableName}`}</DataCell>
                    <DataCell>{copyState.bytes_total ? `${copyState.bytes_total}` : `N/A`}</DataCell>
                    <DataCell>
                        {copyState.bytes_copied ? `${copyState.bytes_copied}` : `N/A`}{' '}
                        {copyState.bytes_percentage ? `(${copyState.bytes_percentage}%)` : ``}
                    </DataCell>
                    <DataCell>{copyState.rows_total ? `${copyState.rows_total}` : `N/A`}</DataCell>
                    <DataCell>
                        {copyState.rows_copied ? `${copyState.rows_copied}` : `N/A`}{' '}
                        {copyState.rows_percentage ? `(${copyState.rows_percentage}%)` : ``}
                    </DataCell>
                </tr>
            );
        });
    };

    return (
        <div className="mt-12 mb-16">
            {tableCopyStates && (
                <div>
                    <h3 className="my-8">Table Copy State</h3>
                    <DataTable
                        columns={TABLE_COPY_STATE_COLUMNS}
                        data={tableCopyStates}
                        renderRows={renderTableCopyStateRows}
                        pageSize={1000}
                    />
                </div>
            )}
            <h3 className="my-8">Streams</h3>
            {streams.map((stream) => {
                const href =
                    stream.tablet && stream.id
                        ? `/workflow/${clusterID}/${keyspace}/${name}/stream/${stream.tablet.cell}/${stream.tablet.uid}/${stream.id}`
                        : null;

                var isThrottled =
                    Number(stream.throttler_status?.time_throttled?.seconds) >
                    Date.now() / 1000 - ThrottleThresholdSeconds;
                const streamState = isThrottled ? 'Throttled' : stream.state;
                return (
                    <div className="my-8">
                        <div className="text-lg font-bold">
                            <StreamStatePip state={streamState} /> <Link to={href}>{`${stream.key}`}</Link>
                        </div>
                        <p className="text-base">
                            <strong>State</strong> <br />
                            {streamState}
                        </p>
                        {isThrottled && (
                            <p className="text-base">
                                <strong>Component Throttled</strong> <br />
                                {stream.throttler_status?.component_throttled}
                            </p>
                        )}
                        {streamState == 'Running' && data?.workflow?.max_v_replication_lag && (
                            <p className="text-base">
                                <strong>Max VReplication Lag</strong> <br />
                                {`${data?.workflow?.max_v_replication_lag}`}
                            </p>
                        )}
                        <DataTable
                            columns={LOG_COLUMNS}
                            data={stream.logs?.reverse()!}
                            renderRows={renderRows}
                            pageSize={1000}
                            title="Recent Logs"
                        />
                    </div>
                );
            })}
            {reverseWorkflow && (
                <div>
                    <h3 className="my-8">Reverse Workflow</h3>
                    <div className="font-bold text-lg">
                        <Link
                            to={`/workflow/${reverseWorkflow.cluster?.id}/${reverseWorkflow.keyspace}/${reverseWorkflow.workflow?.name}`}
                        >
                            {reverseWorkflow.workflow?.name}
                        </Link>
                    </div>
                    <p className="text-base">
                        <strong>Keyspace</strong> <br />
                        <Link to={`/keyspace/${reverseWorkflow.cluster?.id}/${reverseWorkflow.keyspace}`}>
                            {`${reverseWorkflow.keyspace}`}
                        </Link>
                    </p>
                    {reverseWorkflow.workflow?.max_v_replication_lag && (
                        <p className="text-base">
                            <strong>Max VReplication Lag</strong> <br />
                            {`${reverseWorkflow.workflow?.max_v_replication_lag}`}
                        </p>
                    )}
                </div>
            )}
        </div>
    );
};
