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
import { useState } from 'react';
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

const COLUMNS = ['UUID', 'Status', 'Shard', 'Started At', 'Added At'];

export const SchemaMigrations = () => {
    useDocumentTitle('Schema Migrations');

    const keyspacesQuery = useKeyspaces();

    const { data: keyspaces = [] } = keyspacesQuery;

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

    const renderRows = (rows: vtadmin.ISchemaMigration[]) => {
        return rows.map((row) => {
            const migrationInfo = row.schema_migration;

            if (!migrationInfo) return <></>;

            const shard = selectedKeyspace ? `${selectedKeyspace.keyspace?.name}/${migrationInfo.shard}` : '-';

            return (
                <tr key={migrationInfo.uuid}>
                    <DataCell>
                        <div>{migrationInfo.uuid}</div>
                    </DataCell>
                    <DataCell>
                        <div>{formatSchemaMigrationStatus(migrationInfo)}</div>
                    </DataCell>
                    <DataCell>
                        {selectedKeyspace ? (
                            <ShardLink
                                clusterID={selectedKeyspace.cluster?.id}
                                keyspace={selectedKeyspace.keyspace?.name}
                                shard={migrationInfo.shard}
                            >
                                {shard}
                            </ShardLink>
                        ) : (
                            '-'
                        )}
                    </DataCell>
                    <DataCell>
                        <div className="font-sans whitespace-nowrap">
                            {formatDateTime(migrationInfo.started_at?.seconds)}
                        </div>
                    </DataCell>
                    <DataCell>
                        <div className="font-sans whitespace-nowrap">
                            {formatDateTime(migrationInfo.added_at?.seconds)}
                        </div>
                    </DataCell>
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
                        onChange={(ks) => setSelectedKeypsace(ks)}
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
