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
import { orderBy } from 'lodash-es';
import * as React from 'react';
import { Link } from 'react-router-dom';

import style from './Workflows.module.scss';
import { useWorkflows } from '../../hooks/api';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { Button } from '../Button';
import { DataCell } from '../dataTable/DataCell';
import { DataTable } from '../dataTable/DataTable';
import { Icons } from '../Icon';
import { TextInput } from '../TextInput';
import { useSyncedURLParam } from '../../hooks/useSyncedURLParam';
import { filterNouns } from '../../util/filterNouns';

export const Workflows = () => {
    useDocumentTitle('Workflows');
    const { data } = useWorkflows();
    const { value: filter, updateValue: updateFilter } = useSyncedURLParam('filter');

    const sortedData = React.useMemo(() => {
        const mapped = (data || []).map(({ cluster, keyspace, workflow }) => ({
            clusterID: cluster?.id,
            clusterName: cluster?.name,
            keyspace,
            name: workflow?.name,
            source: workflow?.source?.keyspace,
            sourceShards: workflow?.source?.shards,
            target: workflow?.target?.keyspace,
            targetShards: workflow?.target?.shards,
        }));
        const filtered = filterNouns(filter, mapped);
        return orderBy(filtered, ['name', 'clusterName', 'source', 'target']);
    }, [data, filter]);

    const renderRows = (rows: typeof sortedData) =>
        rows.map((row, idx) => {
            const href =
                row.clusterID && row.keyspace && row.name
                    ? `/workflow/${row.clusterID}/${row.keyspace}/${row.name}`
                    : null;

            return (
                <tr key={idx}>
                    <DataCell>
                        <div className="font-weight-bold">{href ? <Link to={href}>{row.name}</Link> : row.name}</div>
                        <div className="font-size-small text-color-secondary">{row.clusterName}</div>
                    </DataCell>
                    <DataCell>
                        {row.source ? (
                            <>
                                <div>{row.source}</div>
                                <div className="font-size-small text-color-secondary">
                                    {(row.sourceShards || []).join(', ')}
                                </div>
                            </>
                        ) : (
                            <span className="text-color-secondary">N/A</span>
                        )}
                    </DataCell>
                    <DataCell>
                        {row.target ? (
                            <>
                                <div>{row.target}</div>
                                <div className="font-size-small text-color-secondary">
                                    {(row.targetShards || []).join(', ')}
                                </div>
                            </>
                        ) : (
                            <span className="text-color-secondary">N/A</span>
                        )}
                    </DataCell>
                </tr>
            );
        });

    return (
        <div className="max-width-content">
            <h1>Workflows</h1>

            <div className={style.controls}>
                <TextInput
                    autoFocus
                    iconLeft={Icons.search}
                    onChange={(e) => updateFilter(e.target.value)}
                    placeholder="Filter workflows"
                    value={filter || ''}
                />
                <Button disabled={!filter} onClick={() => updateFilter('')} secondary>
                    Clear filters
                </Button>
            </div>

            <DataTable columns={['Workflow', 'Source', 'Target']} data={sortedData} renderRows={renderRows} />
        </div>
    );
};
