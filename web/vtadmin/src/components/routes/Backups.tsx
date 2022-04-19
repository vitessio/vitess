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

import { orderBy } from 'lodash';
import { useMemo } from 'react';

import { useBackups } from '../../hooks/api';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { useSyncedURLParam } from '../../hooks/useSyncedURLParam';
import { formatStatus } from '../../util/backups';
import { formatAlias } from '../../util/tablets';
import { formatDateTime, formatRelativeTime } from '../../util/time';
import { DataCell } from '../dataTable/DataCell';
import { DataFilter } from '../dataTable/DataFilter';
import { DataTable } from '../dataTable/DataTable';
import { ContentContainer } from '../layout/ContentContainer';
import { WorkspaceHeader } from '../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../layout/WorkspaceTitle';
import { KeyspaceLink } from '../links/KeyspaceLink';
import { TabletLink } from '../links/TabletLink';
import { BackupStatusPip } from '../pips/BackupStatusPip';
import { filterNouns } from '../../util/filterNouns';

const COLUMNS = ['Started at', 'Directory', 'Backup', 'Tablet', 'Status'];

export const Backups = () => {
    useDocumentTitle('Backups');

    const { value: filter, updateValue: updateFilter } = useSyncedURLParam('filter');

    const { data: backups = [] } = useBackups();

    const rows = useMemo(() => {
        const mapped = backups.map((b) => {
            return {
                clusterID: b.cluster?.id,
                clusterName: b.cluster?.name,
                directory: b.backup?.directory,
                engine: b.backup?.engine,
                keyspace: b.backup?.keyspace,
                name: b.backup?.name,
                shard: b.backup?.shard,
                status: formatStatus(b.backup?.status),
                tablet: formatAlias(b.backup?.tablet_alias),
                time: b.backup?.time?.seconds,
                _status: b.backup?.status,
            };
        });

        const filtered = filterNouns(filter, mapped);
        return orderBy(filtered, ['clusterID', 'keyspace', 'shard', 'name'], ['asc', 'asc', 'asc', 'desc']);
    }, [backups, filter]);

    const renderRows = (rs: typeof rows) => {
        return rs.map((row) => {
            return (
                <tr key={`${row.clusterID}-${row.directory}-${row.name}`}>
                    <DataCell>
                        {formatDateTime(row.time)}
                        <div className="font-size-small text-color-secondary">{formatRelativeTime(row.time)}</div>
                    </DataCell>
                    <DataCell>
                        <KeyspaceLink clusterID={row.clusterID} name={row.keyspace} shard={row.shard}>
                            {row.directory}
                        </KeyspaceLink>
                        <div className="font-size-small text-color-secondary">{row.clusterName}</div>
                    </DataCell>
                    <DataCell>{row.name}</DataCell>
                    <DataCell>
                        <TabletLink alias={row.tablet} clusterID={row.clusterID}>
                            {row.tablet}
                        </TabletLink>
                    </DataCell>
                    <DataCell className="white-space-nowrap">
                        <BackupStatusPip status={row._status} /> {row.status}
                    </DataCell>
                </tr>
            );
        });
    };

    return (
        <div>
            <WorkspaceHeader>
                <WorkspaceTitle>Backups</WorkspaceTitle>
            </WorkspaceHeader>
            <ContentContainer>
                <DataFilter
                    onChange={(e) => updateFilter(e.target.value)}
                    onClear={() => updateFilter('')}
                    placeholder="Filter backups"
                    value={filter || ''}
                />
                <DataTable columns={COLUMNS} data={rows} renderRows={renderRows} />
            </ContentContainer>
        </div>
    );
};
