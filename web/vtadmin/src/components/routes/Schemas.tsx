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
import { useTableDefinitions } from '../../hooks/api';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';

export const Schemas = () => {
    useDocumentTitle('Schemas');
    const { data = [] } = useTableDefinitions();

    const rows = React.useMemo(() => {
        return orderBy(data, ['cluster.name', 'keyspace', 'tableDefinition.name']);
    }, [data]);

    return (
        <div>
            <h1>Schemas</h1>
            <table>
                <thead>
                    <tr>
                        <th>Cluster</th>
                        <th>Keyspace</th>
                        <th>Table</th>
                    </tr>
                </thead>
                <tbody>
                    {rows.map((row, idx) => (
                        <tr key={idx}>
                            <td>{row.cluster?.name}</td>
                            <td>{row.keyspace}</td>
                            <td>{row.tableDefinition?.name}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
};
