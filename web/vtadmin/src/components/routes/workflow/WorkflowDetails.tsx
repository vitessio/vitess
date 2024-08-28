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

const SUMMARY_COLUMNS = ['Stream Status', 'Traffic Status', 'Max VReplication Lag', 'Reverse Workflow'];

const LOG_COLUMNS = ['Type', 'State', 'Updated At', 'Message', 'Count'];

const TABLE_COPY_STATE_COLUMNS = ['Table Name', 'Total Bytes', 'Bytes Copied', 'Total Rows', 'Rows Copied'];

const STREAM_COLUMNS = ['Stream', 'State', 'Message', 'Transaction Timestamp', 'Database Name'];

export const WorkflowDetails = ({ clusterID, keyspace, name }: Props) => {
    const { data: workflowData } = useWorkflow({ clusterID, keyspace, name });

    const { data: workflowsData = [] } = useWorkflows();

    const { data: workflowStatus } = useWorkflowStatus({
        clusterID,
        keyspace,
        name,
    });

    const reverseWorkflow = getReverseWorkflow(workflowsData, workflowData);

    const tableCopyStates = getTableCopyStates(workflowStatus);

    const streams = useMemo(() => {
        const rows = getStreams(workflowData).map((stream) => ({
            key: formatStreamKey(stream),
            ...stream,
        }));

        return orderBy(rows, 'streamKey');
    }, [workflowData]);

    const getStreamsSummary = (streamList: typeof streams): string => {
        const numStreamsByState: { [key: string]: number } = {
            Copying: 0,
            Throttled: 0,
            Stopped: 0,
            Running: 0,
            Error: 0,
        };
        streamList.forEach((stream) => {
            var isThrottled =
                Number(stream.throttler_status?.time_throttled?.seconds) > Date.now() / 1000 - ThrottleThresholdSeconds;
            const streamState = isThrottled ? 'Throttled' : stream.state;
            if (streamState) {
                numStreamsByState[streamState]++;
            }
        });
        const states = Object.keys(numStreamsByState);
        let message = '';
        states.forEach((state) => {
            if (numStreamsByState[state]) {
                let desc = state;
                if (state === 'Error') {
                    desc = 'Failed';
                }
                desc += numStreamsByState[state] > 1 ? ' Streams' : ' Stream';
                message += `${numStreamsByState[state]} ${desc}. `;
            }
        });
        return message;
    };

    const workflowSummary = {
        streamSummary: getStreamsSummary(streams),
        workflowStatus,
        workflowData,
        reverseWorkflow,
    };

    const renderSummaryRows = (rows: (typeof workflowSummary)[]) => {
        return rows.map((row) => {
            const reverseWorkflow = row.reverseWorkflow;
            return (
                <tr key={reverseWorkflow?.workflow?.name}>
                    <DataCell>{row.streamSummary ? row.streamSummary : '-'}</DataCell>
                    <DataCell>{row.workflowStatus ? row.workflowStatus.traffic_state : '-'}</DataCell>
                    <DataCell>
                        {row.workflowData && row.workflowData.workflow?.max_v_replication_lag
                            ? `${row.workflowData.workflow?.max_v_replication_lag}`
                            : '-'}
                    </DataCell>
                    <DataCell>
                        {reverseWorkflow ? (
                            <Link
                                to={`/workflow/${reverseWorkflow.cluster?.id}/${reverseWorkflow.keyspace}/${reverseWorkflow.workflow?.name}`}
                                className="text-base"
                            >
                                {reverseWorkflow.workflow?.name}
                            </Link>
                        ) : (
                            '-'
                        )}
                    </DataCell>
                </tr>
            );
        });
    };

    const renderStreamRows = (rows: typeof streams) => {
        return rows.map((row) => {
            const href =
                row.tablet && row.id
                    ? `/workflow/${clusterID}/${keyspace}/${name}/stream/${row.tablet.cell}/${row.tablet.uid}/${row.id}`
                    : null;

            var isThrottled =
                Number(row?.throttler_status?.time_throttled?.seconds) > Date.now() / 1000 - ThrottleThresholdSeconds;
            const rowState = isThrottled ? 'Throttled' : row.state;
            return (
                <tr key={row.key}>
                    <DataCell>
                        <StreamStatePip state={rowState} />{' '}
                        <Link className="font-bold" to={href}>
                            {row.key}
                        </Link>
                        <div className="text-sm text-secondary">
                            Updated {formatDateTime(row.time_updated?.seconds)}
                        </div>
                        {isThrottled ? (
                            <div className="text-sm text-secondary">
                                <span className="font-bold text-danger">Throttled: </span>
                                in {row.throttler_status?.component_throttled}
                            </div>
                        ) : null}
                    </DataCell>
                    <DataCell>{rowState}</DataCell>
                    <DataCell>{row.message ? row.message : '-'}</DataCell>
                    <DataCell>
                        {row.transaction_timestamp && row.transaction_timestamp.seconds
                            ? formatDateTime(row.transaction_timestamp.seconds)
                            : '-'}
                    </DataCell>
                    <DataCell>{row.db_name}</DataCell>
                </tr>
            );
        });
    };

    const renderLogRows = (rows: vtctldata.Workflow.Stream.ILog[]) => {
        return rows.map((row) => {
            let message: string = row.message ? `${row.message}` : '-';
            // TODO: Investigate if message needs to be JSON parsed in case of "Stream Created"
            if (row.type === 'Stream Created') {
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
            <DataTable
                columns={SUMMARY_COLUMNS}
                data={[workflowSummary]}
                renderRows={renderSummaryRows}
                pageSize={1}
                title="Summary"
            />
            <DataTable
                columns={STREAM_COLUMNS}
                data={streams}
                renderRows={renderStreamRows}
                pageSize={10}
                title="Streams"
            />
            {tableCopyStates && (
                <DataTable
                    columns={TABLE_COPY_STATE_COLUMNS}
                    data={tableCopyStates}
                    renderRows={renderTableCopyStateRows}
                    pageSize={1000}
                    title="Table Copy State"
                />
            )}
            <h3 className="mt-8 mb-4">Recent Logs</h3>
            {streams.map((stream) => (
                <div className="mt-2" key={stream.key}>
                    <DataTable
                        columns={LOG_COLUMNS}
                        data={orderBy(stream.logs, 'id', 'desc')}
                        renderRows={renderLogRows}
                        pageSize={10}
                        title={stream.key!}
                    />
                </div>
            ))}
        </div>
    );
};
