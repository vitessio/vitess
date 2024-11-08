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
import { useTransaction } from '../../../hooks/api';
import { DataCell } from '../../dataTable/DataCell';
import { DataTable } from '../../dataTable/DataTable';
import { ContentContainer } from '../../layout/ContentContainer';
import { WorkspaceHeader } from '../../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../../layout/WorkspaceTitle';
import { QueryLoadingPlaceholder } from '../../placeholders/QueryLoadingPlaceholder';
import { useDocumentTitle } from '../../../hooks/useDocumentTitle';
import { query, vtctldata } from '../../../proto/vtadmin';
import { formatTransactionState } from '../../../util/transactions';
import { ShardLink } from '../../links/ShardLink';
import { formatDateTime, formatRelativeTimeInSeconds } from '../../../util/time';
import { ReadOnlyGate } from '../../ReadOnlyGate';
import TransactionActions from '../transactions/TransactionActions';
import { isReadOnlyMode } from '../../../util/env';
import { useParams } from 'react-router';
import style from '../keyspace/Keyspace.module.scss';
import { Link } from 'react-router-dom';
import { NavCrumbs } from '../../layout/NavCrumbs';
import { READ_ONLY_COLUMNS } from '../Transactions';
import { COLUMNS } from '../Transactions';
import * as React from 'react';
import { TransactionLink } from '../../links/TransactionLink';

interface RouteParams {
    clusterID: string;
    dtid: string;
}

export const SHARD_STATE_COLUMNS = ['Shard', 'State', 'Message', 'Time Created', 'Statements'];

export const Transaction = () => {
    const { clusterID, dtid } = useParams<RouteParams>();

    useDocumentTitle(`${dtid} (${clusterID})`);

    const transactionQuery = useTransaction({ clusterID, dtid });

    const { data: transaction } = transactionQuery;

    if (transactionQuery.error) {
        return (
            <div className={style.placeholder}>
                <span className={style.errorEmoji}>😰</span>
                <h1>An error occurred</h1>
                <code>
                    {(transactionQuery.error as any).response?.error?.message || transactionQuery.error?.message}
                </code>
                <p>
                    <Link to="/transactions">← All Unresolved Transactions</Link>
                </p>
            </div>
        );
    }

    if (!transactionQuery.isLoading && !transaction) {
        return (
            <div className={style.placeholder}>
                <span className={style.errorEmoji}>😖</span>
                <h1>Transaction not found</h1>
                <p>
                    <Link to="/transactions">← All Unresolved Transactions</Link>
                </p>
            </div>
        );
    }

    const renderMetadataRow = (rows: query.ITransactionMetadata[]) => {
        return rows.map((row) => {
            return (
                <tr key={row.dtid}>
                    <DataCell>
                        <TransactionLink clusterID={clusterID} dtid={row.dtid}>
                            <div className="font-bold">{row.dtid}</div>
                        </TransactionLink>
                    </DataCell>
                    <DataCell>
                        <div>{formatTransactionState(row)}</div>
                    </DataCell>
                    <DataCell>
                        {row.participants?.map((participant) => {
                            const shard = `${participant.keyspace}/${participant.shard}`;
                            return (
                                <ShardLink
                                    clusterID={clusterID}
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
                        <div className="font-sans text-sm text-secondary">
                            {formatRelativeTimeInSeconds(row.time_created)}
                        </div>
                    </DataCell>
                    <ReadOnlyGate>
                        <DataCell>
                            <TransactionActions
                                refetchTransactions={transactionQuery.refetch}
                                clusterID={clusterID as string}
                                dtid={row.dtid as string}
                            />
                        </DataCell>
                    </ReadOnlyGate>
                </tr>
            );
        });
    };

    const renderShardStateRow = (rows: vtctldata.IShardTransactionState[]) => {
        return rows.map((row) => {
            return (
                <tr key={row.shard}>
                    <DataCell>
                        <div className="font-bold">{row.shard}</div>
                    </DataCell>
                    <DataCell>
                        <div>{row.state ? row.state : 'COMMITTED'}</div>
                    </DataCell>
                    <DataCell>
                        <div>{row.message ? row.message : '-'}</div>
                    </DataCell>
                    <DataCell>
                        <div className="font-sans whitespace-nowrap">
                            {row.time_created ? formatDateTime(row.time_created) : '-'}
                        </div>
                        <div className="font-sans text-sm text-secondary">
                            {row.time_created ? formatRelativeTimeInSeconds(row.time_created) : ''}
                        </div>
                    </DataCell>
                    <DataCell>{row.statements}</DataCell>
                </tr>
            );
        });
    };

    return (
        <div>
            <WorkspaceHeader className="mb-0">
                <NavCrumbs>
                    <Link to="/transactions">← All Unresolved Transactions</Link>
                </NavCrumbs>

                <WorkspaceTitle className="font-mono">{dtid}</WorkspaceTitle>

                <div className={style.headingMeta}>
                    <span>
                        Cluster: <code>{clusterID}</code>
                    </span>
                </div>
            </WorkspaceHeader>

            <ContentContainer>
                <DataTable
                    columns={isReadOnlyMode() ? READ_ONLY_COLUMNS : COLUMNS}
                    data={transaction && transaction.metadata ? [transaction.metadata] : []}
                    renderRows={renderMetadataRow}
                />
                <DataTable
                    columns={SHARD_STATE_COLUMNS}
                    data={transaction && transaction.shard_states ? transaction.shard_states : []}
                    renderRows={renderShardStateRow}
                />
                <QueryLoadingPlaceholder query={transactionQuery} />
            </ContentContainer>
        </div>
    );
};
