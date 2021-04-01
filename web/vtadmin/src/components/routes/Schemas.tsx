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
import { TableDefinition, useTableDefinitions } from '../../hooks/api';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { DataTable } from '../dataTable/DataTable';

export const Schemas = () => {
    useDocumentTitle('Schemas');
    const { data = [] } = useTableDefinitions();

    const rows = React.useMemo(() => {
        return orderBy(data, ['cluster.name', 'keyspace', 'tableDefinition.name']);
    }, [data]);

    const renderRows = (rows: TableDefinition[]) =>
        rows.map((row, idx) => {
            const href =
                row.cluster?.id && row.keyspace && row.tableDefinition?.name
                    ? `/schema/${row.cluster.id}/${row.keyspace}/${row.tableDefinition.name}`
                    : null;
            return (
                <tr key={idx}>
                    <td>{row.cluster?.name}</td>
                    <td>{row.keyspace}</td>
                    <td>{href ? <Link to={href}>{row.tableDefinition?.name}</Link> : row.tableDefinition?.name}</td>
                </tr>
            );
        });

    return (
        <div className="max-width-content">
            <h1>Schemas</h1>
            <DataTable columns={['Cluster', 'Keyspace', 'Table']} data={rows} renderRows={renderRows} />
        </div>
    );
};
