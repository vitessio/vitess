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
import { useEffect, useState } from 'react';
import { useKeyspaces, useSchemaMigrations } from '../../hooks/api';
import { DataCell } from '../dataTable/DataCell';
import { DataTable } from '../dataTable/DataTable';
import { ContentContainer } from '../layout/ContentContainer';
import { WorkspaceHeader } from '../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../layout/WorkspaceTitle';
import { QueryLoadingPlaceholder } from '../placeholders/QueryLoadingPlaceholder';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { vtadmin } from '../../proto/vtadmin';
import { Select } from '../inputs/Select';
import { ShardLink } from '../links/ShardLink';
import { formatDateTime } from '../../util/time';
import { ReadOnlyGate } from '../ReadOnlyGate';
import { formatSchemaMigrationStatus } from '../../util/schemaMigrations';
import { Link } from 'react-router-dom';
import { TabletLink } from '../links/TabletLink';
import { formatAlias } from '../../util/tablets';
import { useURLQuery } from '../../hooks/useURLQuery';

const COLUMNS = ['UUID', 'Status', 'DDL Action', 'Timestamps', 'Stage', 'Progress'];

export const SchemaMigrations = () => {
    useDocumentTitle('Schema Migrations');

    const { query, replaceQuery } = useURLQuery();
    const urlKeyspace = query['keyspace'];
    const urlCluster = query['cluster'];

    const keyspacesQuery = useKeyspaces();
    const { data: keyspaces = [], ...ksQuery } = keyspacesQuery;

    const [selectedKeyspace, setSelectedKeypsace] = useState<vtadmin.Keyspace | null | undefined>();

    const request: vtadmin.IGetSchemaMigrationsRequest = {
        cluster_requests: [
            {
                cluster_id: selectedKeyspace && selectedKeyspace.cluster?.id,
                request: {
                    keyspace: selectedKeyspace && selectedKeyspace.keyspace?.name,
                },
            },
        ],
    };

    const schemaMigrationsQuery = useSchemaMigrations(request, {
        enabled: !!selectedKeyspace,
    });

    const schemaMigrations = schemaMigrationsQuery.data ? schemaMigrationsQuery.data.schema_migrations : [];

    const handleKeyspaceChange = (ks: vtadmin.Keyspace | null | undefined) => {
        setSelectedKeypsace(ks);

        if (ks) {
            replaceQuery({ keyspace: ks.keyspace?.name, cluster: ks.cluster?.id });
        } else {
            replaceQuery({ keyspace: undefined, cluster: undefined });
        }
    };

    useEffect(() => {
        if (urlKeyspace && urlCluster) {
            const keyspace = keyspaces.find(
                (ks) => ks.cluster?.id === String(urlCluster) && ks.keyspace?.name === String(urlKeyspace)
            );

            if (keyspace) {
                setSelectedKeypsace(keyspace);
            } else if (!ksQuery.isLoading) {
                replaceQuery({ keyspace: undefined, cluster: undefined });
            }
        } else {
            setSelectedKeypsace(undefined);
        }
    }, [urlKeyspace, urlCluster, keyspaces, ksQuery.isLoading, replaceQuery]);

    const renderRows = (rows: vtadmin.ISchemaMigration[]) => {
        return rows.map((row) => {
            const migrationInfo = row.schema_migration;

            if (!migrationInfo) return <></>;

            return (
                <tr key={migrationInfo.uuid}>
                    <DataCell>
                        <div>{migrationInfo.uuid}</div>
                        <div className="text-sm text-secondary">
                            Tablet{' '}
                            <TabletLink alias={formatAlias(migrationInfo.tablet)} clusterID={row.cluster?.id}>
                                {formatAlias(migrationInfo.tablet)}
                            </TabletLink>
                        </div>
                        <div className="text-sm text-secondary">
                            Shard{' '}
                            <ShardLink
                                clusterID={row.cluster?.id}
                                keyspace={migrationInfo.keyspace}
                                shard={migrationInfo.shard}
                            >
                                {`${migrationInfo.keyspace}/${migrationInfo.shard}`}
                            </ShardLink>
                        </div>
                    </DataCell>
                    <DataCell>
                        <div>{formatSchemaMigrationStatus(migrationInfo)}</div>
                    </DataCell>
                    <DataCell>{migrationInfo.ddl_action ? migrationInfo.ddl_action : '-'}</DataCell>
                    <DataCell className="items-end flex flex-col">
                        {migrationInfo.added_at && (
                            <div className="text-sm font-sans whitespace-nowrap">
                                <span className="text-secondary">Added </span>
                                {formatDateTime(migrationInfo.added_at?.seconds)}
                            </div>
                        )}
                        {migrationInfo.requested_at && (
                            <div className="text-sm font-sans whitespace-nowrap">
                                <span className="text-secondary">Requested </span>
                                {formatDateTime(migrationInfo.requested_at?.seconds)}
                            </div>
                        )}
                        {migrationInfo.started_at && (
                            <div className="text-sm font-sans whitespace-nowrap">
                                <span className="text-secondary">Started </span>
                                {formatDateTime(migrationInfo.started_at?.seconds)}
                            </div>
                        )}
                        {migrationInfo.completed_at && (
                            <div className="text-sm font-sans whitespace-nowrap">
                                <span className="text-secondary">Completed </span>
                                {formatDateTime(migrationInfo.completed_at?.seconds)}
                            </div>
                        )}
                    </DataCell>
                    <DataCell>{migrationInfo.stage ? migrationInfo.stage : '-'}</DataCell>
                    <DataCell>{migrationInfo.progress ? `${migrationInfo.progress}%` : '-'}</DataCell>
                </tr>
            );
        });
    };

    return (
        <div>
            <WorkspaceHeader className="mb-0">
                <div className="flex items-top justify-between">
                    <WorkspaceTitle>Schema Migrations</WorkspaceTitle>
                    <ReadOnlyGate>
                        <div>
                            <Link className="btn btn-secondary btn-md" to="/migrations/create">
                                Create Schema Migration Request
                            </Link>
                        </div>
                    </ReadOnlyGate>
                </div>
            </WorkspaceHeader>

            <ContentContainer>
                <div className="max-w-[740px]">
                    <Select
                        className="block grow-1 min-w-[400px]"
                        disabled={keyspacesQuery.isLoading}
                        inputClassName="block w-full"
                        items={keyspaces}
                        label="Keyspace"
                        onChange={(ks) => handleKeyspaceChange(ks)}
                        placeholder={
                            keyspacesQuery.isLoading
                                ? 'Loading keyspaces...'
                                : 'Select a keyspace to view schema migrations'
                        }
                        renderItem={(ks) => `${ks?.keyspace?.name} (${ks?.cluster?.id})`}
                        selectedItem={selectedKeyspace}
                    />
                </div>
                <DataTable columns={COLUMNS} data={schemaMigrations} renderRows={renderRows} />
                <QueryLoadingPlaceholder query={schemaMigrationsQuery} />
            </ContentContainer>
        </div>
    );
};
