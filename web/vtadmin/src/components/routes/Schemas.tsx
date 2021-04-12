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
import { filterNouns } from '../../util/filterNouns';
import { formatBytes } from '../../util/formatBytes';
import { getTableDefinitions } from '../../util/tableDefinitions';
import { Button } from '../Button';
import { DataCell } from '../dataTable/DataCell';
import { DataTable } from '../dataTable/DataTable';
import { Icons } from '../Icon';
import { TextInput } from '../TextInput';
import style from './Schemas.module.scss';

const TABLE_COLUMNS = [
    'Keyspace',
    'Table',
    // TODO: add tooltips to explain that "approx." means something
    // along the lines of "information_schema is an eventually correct
    // statistical analysis, past results do not guarantee future performance,
    // please consult an attorney and we are not a licensed medical professional".
    // Namely, these numbers come from `information_schema`, which is never precisely
    // correct for tables that have a high rate of updates. (The only way to get
    // accurate numbers is to run `ANALYZE TABLE` on the tables periodically, which
    // is out of scope for VTAdmin.)
    <div className="text-align-right">Approx. Size</div>,
    <div className="text-align-right">Approx. Rows</div>,
];

export const Schemas = () => {
    useDocumentTitle('Schemas');

    const { data = [] } = useSchemas();
    const [filter, setFilter] = React.useState<string>('');

    const filteredData = React.useMemo(() => {
        const tableDefinitions = getTableDefinitions(data);

        const mapped = tableDefinitions.map((d) => ({
            cluster: d.cluster?.name,
            clusterID: d.cluster?.id,
            keyspace: d.keyspace,
            table: d.tableDefinition?.name,
            _raw: d,
        }));

        const filtered = filterNouns(filter, mapped);
        return orderBy(filtered, ['cluster', 'keyspace', 'table']);
    }, [data, filter]);

    const renderRows = (rows: typeof filteredData) =>
        rows.map((row, idx) => {
            const href =
                row.clusterID && row.keyspace && row.table
                    ? `/schema/${row.clusterID}/${row.keyspace}/${row.table}`
                    : null;
            return (
                <tr key={idx}>
                    <DataCell>
                        <div>{row.keyspace}</div>
                        <div className="font-size-small text-color-secondary">{row.cluster}</div>
                    </DataCell>
                    <DataCell className="font-weight-bold">
                        {href ? <Link to={href}>{row.table}</Link> : row.table}
                    </DataCell>
                    <DataCell className="text-align-right">
                        <div>{formatBytes(row._raw.tableSize?.data_length)}</div>
                        <div className="font-size-small text-color-secondary">
                            {formatBytes(row._raw.tableSize?.data_length, 'B')}
                        </div>
                    </DataCell>
                    <DataCell className="text-align-right">
                        {(row._raw.tableSize?.row_count || 0).toLocaleString()}
                    </DataCell>
                </tr>
            );
        });

    return (
        <div className="max-width-content">
            <h1>Schemas</h1>
            <div className={style.controls}>
                <TextInput
                    autoFocus
                    iconLeft={Icons.search}
                    onChange={(e) => setFilter(e.target.value)}
                    placeholder="Filter schemas"
                    value={filter}
                />
                <Button disabled={!filter} onClick={() => setFilter('')} secondary>
                    Clear filters
                </Button>
            </div>

            <DataTable columns={TABLE_COLUMNS} data={filteredData} renderRows={renderRows} />
        </div>
    );
};
