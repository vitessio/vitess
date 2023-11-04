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

import { orderBy, groupBy } from 'lodash-es';
import React, { useMemo } from 'react';
import { Link } from 'react-router-dom';

import { useWorkflow } from '../../../hooks/api';
import { formatAlias } from '../../../util/tablets';
import { formatDateTime } from '../../../util/time';
import { getStreams, formatStreamKey, getStreamSource, getStreamTarget } from '../../../util/workflows';
import { DataCell } from '../../dataTable/DataCell';
import { DataTable } from '../../dataTable/DataTable';
import { TabletLink } from '../../links/TabletLink';
import { StreamStatePip } from '../../pips/StreamStatePip';
import { WorkflowStreamsLagChart } from '../../charts/WorkflowStreamsLagChart';
import { ShardLink } from '../../links/ShardLink';
import { env } from '../../../util/env';

interface Props {
    clusterID: string;
    keyspace: string;
    name: string;
}

const COLUMNS = ['Stream', 'Source', 'Target', 'Tablet'];

export const WorkflowStreams = ({ clusterID, keyspace, name }: Props) => {
    const { data } = useWorkflow({ clusterID, keyspace, name });

    const streams = useMemo(() => {
        const rows = getStreams(data).map((stream) => ({
            key: formatStreamKey(stream),
            ...stream,
        }));

        return orderBy(rows, 'streamKey');
    }, [data]);

    const streamsByState = groupBy(streams, 'state');

    const renderRows = (rows: typeof streams) => {
        return rows.map((row) => {
            const href =
                row.tablet && row.id
                    ? `/workflow/${clusterID}/${keyspace}/${name}/stream/${row.tablet.cell}/${row.tablet.uid}/${row.id}`
                    : null;

            const source = getStreamSource(row);
            const target = getStreamTarget(row, keyspace);

            return (
                <tr key={row.key}>
                    <DataCell>
                        <StreamStatePip state={row.state} />{' '}
                        <Link className="font-bold" to={href}>
                            {row.key}
                        </Link>
                        <div className="text-sm text-secondary">
                            Updated {formatDateTime(row.time_updated?.seconds)}
                        </div>
                    </DataCell>
                    <DataCell>
                        {source ? (
                            <ShardLink
                                clusterID={clusterID}
                                keyspace={row.binlog_source?.keyspace}
                                shard={row.binlog_source?.shard}
                            >
                                {source}
                            </ShardLink>
                        ) : (
                            <span className="text-secondary">N/A</span>
                        )}
                    </DataCell>
                    <DataCell>
                        {target ? (
                            <ShardLink clusterID={clusterID} keyspace={keyspace} shard={row.shard}>
                                {target}
                            </ShardLink>
                        ) : (
                            <span className="text-secondary">N/A</span>
                        )}
                    </DataCell>
                    <DataCell>
                        <TabletLink alias={formatAlias(row.tablet)} clusterID={clusterID}>
                            {formatAlias(row.tablet)}
                        </TabletLink>
                    </DataCell>
                </tr>
            );
        });
    };

    return (
        <div className="mt-12 mb-16">
            {env().VITE_ENABLE_EXPERIMENTAL_TABLET_DEBUG_VARS && (
                <>
                    <h3 className="my-8">Stream VReplication Lag</h3>
                    <WorkflowStreamsLagChart clusterID={clusterID} keyspace={keyspace} workflowName={name} />
                </>
            )}

            <h3 className="mt-24 mb-8">Streams</h3>
            {/* TODO(doeg): add a protobuf enum for this (https://github.com/vitessio/vitess/projects/12#card-60190340) */}
            {['Error', 'Copying', 'Running', 'Stopped'].map((streamState) => {
                if (!Array.isArray(streamsByState[streamState])) {
                    return null;
                }

                return (
                    <div className="my-12" key={streamState}>
                        <DataTable
                            columns={COLUMNS}
                            data={streamsByState[streamState]}
                            // TODO(doeg): make pagination optional in DataTable https://github.com/vitessio/vitess/projects/12#card-60810231
                            pageSize={1000}
                            renderRows={renderRows}
                            title={streamState}
                        />
                    </div>
                );
            })}
        </div>
    );
};
