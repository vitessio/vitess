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

import { useSchemas } from '../../hooks/api';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { useSyncedURLParam } from '../../hooks/useSyncedURLParam';
import { filterNouns } from '../../util/filterNouns';
import { formatBytes } from '../../util/formatBytes';
import { getTableDefinitions } from '../../util/tableDefinitions';
import { DataCell } from '../dataTable/DataCell';
import { DataFilter } from '../dataTable/DataFilter';
import { DataTable } from '../dataTable/DataTable';
import { ContentContainer } from '../layout/ContentContainer';
import { WorkspaceHeader } from '../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../layout/WorkspaceTitle';
import { KeyspaceLink } from '../links/KeyspaceLink';
import { QueryLoadingPlaceholder } from '../placeholders/QueryLoadingPlaceholder';
import { HelpTooltip } from '../tooltip/HelpTooltip';

const TABLE_COLUMNS = [
    'Keyspace',
    'Table',
    <div className="text-right">
        Approx. Size{' '}
        <HelpTooltip
            text={
                <span>
                    Size is an approximate value derived from <span className="font-mono">INFORMATION_SCHEMA</span>.
                </span>
            }
        />
    </div>,
    <div className="text-right">
        Approx. Rows{' '}
        <HelpTooltip
            text={
                // c.f. https://dev.mysql.com/doc/refman/5.7/en/information-schema-tables-table.html
                <span>
                    Row count is an approximate value derived from <span className="font-mono">INFORMATION_SCHEMA</span>
                    . Actual values may vary by as much as 40% to 50%.
                </span>
            }
        />
    </div>,
];

export const Schemas = () => {
    useDocumentTitle('Schemas');

    const schemasQuery = useSchemas();
    const { value: filter, updateValue: updateFilter } = useSyncedURLParam('filter');

    const filteredData = React.useMemo(() => {
        const tableDefinitions = getTableDefinitions(schemasQuery.data);

        const mapped = tableDefinitions.map((d) => ({
            cluster: d.cluster?.name,
            clusterID: d.cluster?.id,
            keyspace: d.keyspace,
            table: d.tableDefinition?.name,
            _raw: d,
        }));

        const filtered = filterNouns(filter, mapped);
        return orderBy(filtered, ['cluster', 'keyspace', 'table']);
    }, [schemasQuery.data, filter]);

    const renderRows = (rows: typeof filteredData) =>
        rows.map((row, idx) => {
            const href =
                row.clusterID && row.keyspace && row.table
                    ? `/schema/${row.clusterID}/${row.keyspace}/${row.table}`
                    : null;
            return (
                <tr key={idx}>
                    <DataCell>
                        <KeyspaceLink clusterID={row.clusterID} name={row.keyspace}>
                            <div>{row.keyspace}</div>
                            <div className="text-sm text-secondary">{row.cluster}</div>
                        </KeyspaceLink>
                    </DataCell>
                    <DataCell className="font-bold">{href ? <Link to={href}>{row.table}</Link> : row.table}</DataCell>
                    <DataCell className="text-right">
                        <div>{formatBytes(row._raw.tableSize?.data_length)}</div>
                        <div className="text-sm text-secondary">
                            {formatBytes(row._raw.tableSize?.data_length, 'B')}
                        </div>
                    </DataCell>
                    <DataCell className="text-right">{(row._raw.tableSize?.row_count || 0).toLocaleString()}</DataCell>
                </tr>
            );
        });

    return (
        <div>
            <WorkspaceHeader>
                <WorkspaceTitle>Schemas</WorkspaceTitle>
            </WorkspaceHeader>
            <ContentContainer>
                <DataFilter
                    autoFocus
                    onChange={(e) => updateFilter(e.target.value)}
                    onClear={() => updateFilter('')}
                    placeholder="Filter schemas"
                    value={filter || ''}
                />
                <DataTable columns={TABLE_COLUMNS} data={filteredData} renderRows={renderRows} />
                <QueryLoadingPlaceholder query={schemasQuery} />
            </ContentContainer>
        </div>
    );
};
