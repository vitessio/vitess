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
import { useKeyspaces, useTransactions } from '../../hooks/api';
import { DataCell } from '../dataTable/DataCell';
import { DataTable } from '../dataTable/DataTable';
import { ContentContainer } from '../layout/ContentContainer';
import { WorkspaceHeader } from '../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../layout/WorkspaceTitle';
import { QueryLoadingPlaceholder } from '../placeholders/QueryLoadingPlaceholder';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { query } from '../../proto/vtadmin';
import { Select } from '../inputs/Select';
import { FetchTransactionsParams } from '../../api/http';
import { formatTransactionState } from '../../util/transactions';
import { ShardLink } from '../links/ShardLink';
import { formatDateTime, formatRelativeTime } from '../../util/time';
import { orderBy } from 'lodash-es';

const COLUMNS = ['ID', 'State', 'Participants', 'Time Created'];

export const Transactions = () => {
    useDocumentTitle('In Flight Distributed Transactions');

    const keyspacesQuery = useKeyspaces();

    const { data: keyspaces = [] } = keyspacesQuery;

    const [params, setParams] = useState<FetchTransactionsParams>({ clusterID: '', keyspace: '' });

    const selectedKeyspace = keyspaces.find(
        (ks) => ks.keyspace?.name === params.keyspace && ks.cluster?.id === params.clusterID
    );

    const transactionsQuery = useTransactions(params, {
        enabled: !!params.keyspace,
    });

    const transactions =
        (transactionsQuery.data && orderBy(transactionsQuery.data.transactions, ['time_created'], 'asc')) || [];

    const renderRows = (rows: query.ITransactionMetadata[]) => {
        return rows.map((row) => {
            return (
                <tr key={row.dtid}>
                    <DataCell>
                        <div>{row.dtid}</div>
                    </DataCell>
                    <DataCell>
                        <div>{formatTransactionState(row)}</div>
                    </DataCell>
                    <DataCell>
                        {row.participants?.map((participant) => {
                            const shard = `${participant.keyspace}/${participant.shard}`;
                            return (
                                <ShardLink
                                    clusterID={params.clusterID}
                                    keyspace={participant.keyspace}
                                    shard={participant.shard}
                                >
                                    {shard}
                                </ShardLink>
                            );
                        })}
                    </DataCell>
                    <DataCell>
                        <div className="font-sans whitespace-nowrap">{formatDateTime(row.time_created)}</div>
                        <div className="font-sans text-sm text-secondary">{formatRelativeTime(row.time_created)}</div>
                    </DataCell>
                </tr>
            );
        });
    };

    return (
        <div>
            <WorkspaceHeader className="mb-0">
                <WorkspaceTitle>In Flight Distributed Transactions</WorkspaceTitle>
            </WorkspaceHeader>

            <ContentContainer>
                <Select
                    className="block w-full max-w-[740px]"
                    disabled={keyspacesQuery.isLoading}
                    inputClassName="block w-full"
                    items={keyspaces}
                    label="Keyspace"
                    onChange={(ks) => setParams({ clusterID: ks?.cluster?.id!, keyspace: ks?.keyspace?.name! })}
                    placeholder={
                        keyspacesQuery.isLoading
                            ? 'Loading keyspaces...'
                            : 'Select a keyspace to view unresolved transactions'
                    }
                    renderItem={(ks) => `${ks?.keyspace?.name} (${ks?.cluster?.id})`}
                    selectedItem={selectedKeyspace}
                />
                <DataTable columns={COLUMNS} data={transactions} renderRows={renderRows} />
                <QueryLoadingPlaceholder query={transactionsQuery} />
            </ContentContainer>
        </div>
    );
};
