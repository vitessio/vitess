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
import { groupBy, orderBy } from 'lodash-es';
import { Link, useParams } from 'react-router-dom';

import style from './Workflow.module.scss';
import { useWorkflow } from '../../hooks/api';
import { formatStreamKey, getStreams, getStreamSource, getStreamTarget } from '../../util/workflows';
import { DataCell } from '../dataTable/DataCell';
import { DataTable } from '../dataTable/DataTable';
import { ContentContainer } from '../layout/ContentContainer';
import { NavCrumbs } from '../layout/NavCrumbs';
import { WorkspaceHeader } from '../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../layout/WorkspaceTitle';
import { StreamStatePip } from '../pips/StreamStatePip';
import { formatAlias } from '../../util/tablets';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { formatDateTime } from '../../util/time';

interface RouteParams {
    clusterID: string;
    keyspace: string;
    name: string;
}

const COLUMNS = ['Stream', 'Source', 'Target', 'Tablet'];

export const Workflow = () => {
    const { clusterID, keyspace, name } = useParams<RouteParams>();
    useDocumentTitle(`${name} (${keyspace})`);

    const { data } = useWorkflow({ clusterID, keyspace, name }, { refetchInterval: 1000 });

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

            return (
                <tr key={row.key}>
                    <DataCell>
                        <StreamStatePip state={row.state} />{' '}
                        <Link className="font-weight-bold" to={href}>
                            {row.key}
                        </Link>
                        <div className="font-size-small text-color-secondary">
                            Updated {formatDateTime(row.time_updated?.seconds)}
                        </div>
                    </DataCell>
                    <DataCell>{getStreamSource(row) || <span className="text-color-secondary">N/A</span>}</DataCell>
                    <DataCell>
                        {getStreamTarget(row, keyspace) || <span className="text-color-secondary">N/A</span>}
                    </DataCell>
                    <DataCell>{formatAlias(row.tablet)}</DataCell>
                </tr>
            );
        });
    };

    return (
        <div>
            <WorkspaceHeader>
                <NavCrumbs>
                    <Link to="/workflows">Workflows</Link>
                </NavCrumbs>

                <WorkspaceTitle className="font-family-monospace">{name}</WorkspaceTitle>
                <div className={style.headingMeta}>
                    <span>
                        Cluster: <code>{clusterID}</code>
                    </span>
                    <span>
                        Target keyspace: <code>{keyspace}</code>
                    </span>
                </div>
            </WorkspaceHeader>
            <ContentContainer>
                {/* TODO(doeg): add a protobuf enum for this (https://github.com/vitessio/vitess/projects/12#card-60190340) */}
                {['Error', 'Copying', 'Running', 'Stopped'].map((streamState) => {
                    if (!Array.isArray(streamsByState[streamState])) {
                        return null;
                    }

                    return (
                        <div className={style.streamTable} key={streamState}>
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
            </ContentContainer>
        </div>
    );
};
