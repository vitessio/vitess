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
import { formatDateTime, formatRelativeTimeInSeconds } from '../../util/time';
import { orderBy } from 'lodash-es';
import { ReadOnlyGate } from '../ReadOnlyGate';
import TransactionActions from './transactions/TransactionActions';
import { isReadOnlyMode } from '../../util/env';
import { TransactionLink } from '../links/TransactionLink';
import * as React from 'react';

export const COLUMNS = ['ID', 'State', 'Participants', 'Time Created', 'Actions'];
export const READ_ONLY_COLUMNS = ['ID', 'State', 'Participants', 'Time Created'];

const ABANDON_AGE_OPTIONS = [
    {
        displayText: '5sec',
        abandonAge: '5',
    },
    {
        displayText: '30sec',
        abandonAge: '30',
    },
    {
        displayText: '1min',
        abandonAge: '60',
    },
    {
        displayText: '5min',
        abandonAge: '300',
    },
    {
        displayText: '15min',
        abandonAge: '900',
    },
    {
        displayText: '1hr',
        abandonAge: '3600',
    },
];

export const Transactions = () => {
    useDocumentTitle('In Flight Distributed Transactions');

    const keyspacesQuery = useKeyspaces();

    const { data: keyspaces = [] } = keyspacesQuery;

    const [params, setParams] = useState<FetchTransactionsParams>({ clusterID: '', keyspace: '', abandonAge: '900' });

    const selectedKeyspace = keyspaces.find(
        (ks) => ks.keyspace?.name === params.keyspace && ks.cluster?.id === params.clusterID
    );

    const selectedAbandonAge = ABANDON_AGE_OPTIONS.find((option) => option.abandonAge === params.abandonAge);

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
                        <TransactionLink clusterID={params.clusterID} dtid={row.dtid}>
                            <div className="font-bold">{row.dtid}</div>
                        </TransactionLink>
                    </DataCell>
                    <DataCell>
                        <div>{formatTransactionState(row)}</div>
                    </DataCell>
                    <DataCell>
                        {row.participants
                            ?.map((participant) => {
                                const shard = `${participant.keyspace}/${participant.shard}`;
                                return (
                                    <ShardLink
                                        clusterID={params.clusterID}
                                        keyspace={participant.keyspace}
                                        shard={participant.shard}
                                        key={shard}
                                    >
                                        {shard}
                                    </ShardLink>
                                );
                            })
                            .reduce((prev, curr) => [prev, ', ', curr])}
                    </DataCell>
                    <DataCell>
                        <div className="font-sans whitespace-nowrap">{formatDateTime(row.time_created)}</div>
                        <div className="font-sans text-sm text-secondary">
                            {formatRelativeTimeInSeconds(row.time_created)}
                        </div>
                    </DataCell>
                    <ReadOnlyGate>
                        <DataCell>
                            <TransactionActions
                                refetchTransactions={transactionsQuery.refetch}
                                clusterID={params.clusterID as string}
                                dtid={row.dtid as string}
                            />
                        </DataCell>
                    </ReadOnlyGate>
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
                <div className="flex flex-row flex-wrap gap-4 max-w-[740px]">
                    <Select
                        className="block grow-1 min-w-[400px]"
                        disabled={keyspacesQuery.isLoading}
                        inputClassName="block w-full"
                        items={keyspaces}
                        label="Keyspace"
                        onChange={(ks) =>
                            setParams((prevParams) => ({
                                ...prevParams,
                                clusterID: ks?.cluster?.id!,
                                keyspace: ks?.keyspace?.name!,
                            }))
                        }
                        placeholder={
                            keyspacesQuery.isLoading
                                ? 'Loading keyspaces...'
                                : 'Select a keyspace to view unresolved transactions'
                        }
                        renderItem={(ks) => `${ks?.keyspace?.name} (${ks?.cluster?.id})`}
                        selectedItem={selectedKeyspace}
                    />
                    <Select
                        className="block grow-1 min-w-[300px]"
                        inputClassName="block w-full"
                        items={ABANDON_AGE_OPTIONS}
                        label="Abandon Age"
                        helpText={
                            'List unresolved transactions which are older than the specified age (Leave empty for default abandon age)'
                        }
                        onChange={(option) =>
                            setParams((prevParams) => ({ ...prevParams, abandonAge: option?.abandonAge }))
                        }
                        placeholder={'Select abandon age'}
                        renderItem={(option) => `${option?.displayText}`}
                        selectedItem={selectedAbandonAge}
                    />
                </div>
                <DataTable
                    columns={isReadOnlyMode() ? READ_ONLY_COLUMNS : COLUMNS}
                    data={transactions}
                    renderRows={renderRows}
                />
                <QueryLoadingPlaceholder query={transactionsQuery} />
            </ContentContainer>
        </div>
    );
};
