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
import { groupBy, orderBy } from 'lodash-es';
import * as React from 'react';
import { Link } from 'react-router-dom';

import style from './Workflows.module.scss';
import { useWorkflows } from '../../hooks/api';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { DataCell } from '../dataTable/DataCell';
import { DataTable } from '../dataTable/DataTable';
import { useSyncedURLParam } from '../../hooks/useSyncedURLParam';
import { filterNouns } from '../../util/filterNouns';
import { getStreams, getTimeUpdated } from '../../util/workflows';
import { formatDateTime, formatRelativeTime } from '../../util/time';
import { StreamStatePip } from '../pips/StreamStatePip';
import { ContentContainer } from '../layout/ContentContainer';
import { WorkspaceHeader } from '../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../layout/WorkspaceTitle';
import { DataFilter } from '../dataTable/DataFilter';
import { Tooltip } from '../tooltip/Tooltip';
import { KeyspaceLink } from '../links/KeyspaceLink';
import { QueryLoadingPlaceholder } from '../placeholders/QueryLoadingPlaceholder';
import { UseQueryResult } from 'react-query';

export const Workflows = () => {
    useDocumentTitle('Workflows');
    const workflowsQuery = useWorkflows();

    const { value: filter, updateValue: updateFilter } = useSyncedURLParam('filter');

    const sortedData = React.useMemo(() => {
        const mapped = (workflowsQuery.data || []).map((workflow) => ({
            clusterID: workflow.cluster?.id,
            clusterName: workflow.cluster?.name,
            keyspace: workflow.keyspace,
            name: workflow.workflow?.name,
            source: workflow.workflow?.source?.keyspace,
            sourceShards: workflow.workflow?.source?.shards,
            streams: groupBy(getStreams(workflow), 'state'),
            target: workflow.workflow?.target?.keyspace,
            targetShards: workflow.workflow?.target?.shards,
            timeUpdated: getTimeUpdated(workflow),
        }));
        const filtered = filterNouns(filter, mapped);
        return orderBy(filtered, ['name', 'clusterName', 'source', 'target']);
    }, [workflowsQuery.data, filter]);

    const renderRows = (rows: typeof sortedData) =>
        rows.map((row, idx) => {
            const href =
                row.clusterID && row.keyspace && row.name
                    ? `/workflow/${row.clusterID}/${row.keyspace}/${row.name}`
                    : null;

            return (
                <tr key={idx}>
                    <DataCell>
                        <div className="font-bold">{href ? <Link to={href}>{row.name}</Link> : row.name}</div>
                        <div className="text-sm text-secondary">{row.clusterName}</div>
                    </DataCell>
                    <DataCell>
                        {row.source ? (
                            <>
                                <KeyspaceLink clusterID={row.clusterID} name={row.source}>
                                    {row.source}
                                </KeyspaceLink>
                                <div className={style.shardList}>{(row.sourceShards || []).join(', ')}</div>
                            </>
                        ) : (
                            <span className="text-secondary">N/A</span>
                        )}
                    </DataCell>
                    <DataCell>
                        {row.target ? (
                            <>
                                <KeyspaceLink clusterID={row.clusterID} name={row.target}>
                                    {row.target}
                                </KeyspaceLink>
                                <div className={style.shardList}>{(row.targetShards || []).join(', ')}</div>
                            </>
                        ) : (
                            <span className="text-secondary">N/A</span>
                        )}
                    </DataCell>

                    <DataCell>
                        <div className={style.streams}>
                            {/* TODO(doeg): add a protobuf enum for this (https://github.com/vitessio/vitess/projects/12#card-60190340) */}
                            {['Error', 'Copying', 'Running', 'Stopped'].map((streamState) => {
                                if (streamState in row.streams) {
                                    const streamCount = row.streams[streamState].length;
                                    const tooltip = [
                                        streamCount,
                                        streamState === 'Error' ? 'failed' : streamState.toLocaleLowerCase(),
                                        streamCount === 1 ? 'stream' : 'streams',
                                    ].join(' ');

                                    return (
                                        <Tooltip key={streamState} text={tooltip}>
                                            <span className={style.stream}>
                                                <StreamStatePip state={streamState} /> {streamCount}
                                            </span>
                                        </Tooltip>
                                    );
                                }
                                return (
                                    <span key={streamState} className={style.streamPlaceholder}>
                                        -
                                    </span>
                                );
                            })}
                        </div>
                    </DataCell>

                    <DataCell>
                        <div className="font-sans whitespace-nowrap">{formatDateTime(row.timeUpdated)}</div>
                        <div className="font-sans text-sm text-secondary">{formatRelativeTime(row.timeUpdated)}</div>
                    </DataCell>
                </tr>
            );
        });

    return (
        <div>
            <WorkspaceHeader>
                <WorkspaceTitle>Workflows</WorkspaceTitle>
            </WorkspaceHeader>
            <ContentContainer>
                <DataFilter
                    autoFocus
                    onChange={(e) => updateFilter(e.target.value)}
                    onClear={() => updateFilter('')}
                    placeholder="Filter workflows"
                    value={filter || ''}
                />

                <DataTable
                    columns={['Workflow', 'Source', 'Target', 'Streams', 'Last Updated']}
                    data={sortedData}
                    renderRows={renderRows}
                />

                <QueryLoadingPlaceholder query={workflowsQuery as UseQueryResult} />
            </ContentContainer>
        </div>
    );
};
