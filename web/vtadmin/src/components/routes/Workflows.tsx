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

import { useWorkflows } from '../../hooks/api';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { DataTable } from '../dataTable/DataTable';

export const Workflows = () => {
    useDocumentTitle('Workflows');
    const { data } = useWorkflows();

    const sortedData = React.useMemo(
        () => orderBy(data, ['workflow.name', 'cluster.name', 'workflow.source.keyspace', 'workflow.target.keyspace']),
        [data]
    );

    const renderRows = (rows: typeof sortedData) =>
        rows.map(({ cluster, workflow }, idx) => {
            return (
                <tr key={idx}>
                    <td>{workflow?.name}</td>
                    <td>{cluster?.name}</td>
                    <td>{workflow?.source?.keyspace || <span className="text-color-secondary">n/a</span>}</td>
                    <td>{workflow?.target?.keyspace || <span className="text-color-secondary">n/a</span>}</td>
                </tr>
            );
        });

    return (
        <div className="max-width-content">
            <h1>Workflows</h1>
            <DataTable
                columns={['Workflow', 'Cluster', 'Source', 'Target']}
                data={sortedData}
                renderRows={renderRows}
            />
        </div>
    );
};
