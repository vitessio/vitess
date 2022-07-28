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

import { useClusters } from '../../hooks/api';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { DataTable } from '../dataTable/DataTable';
import { vtadmin as pb } from '../../proto/vtadmin';
import { DataCell } from '../dataTable/DataCell';
import { ContentContainer } from '../layout/ContentContainer';
import { WorkspaceHeader } from '../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../layout/WorkspaceTitle';
import { QueryLoadingPlaceholder } from '../placeholders/QueryLoadingPlaceholder';

export const Clusters = () => {
    useDocumentTitle('Clusters');
    const clustersQuery = useClusters();

    const rows = React.useMemo(() => {
        return orderBy(clustersQuery.data, ['name']);
    }, [clustersQuery.data]);

    const renderRows = (rows: pb.Cluster[]) =>
        rows.map((cluster, idx) => (
            <tr key={idx}>
                <DataCell>{cluster.name}</DataCell>
                <DataCell>{cluster.id}</DataCell>
            </tr>
        ));

    return (
        <div>
            <WorkspaceHeader>
                <WorkspaceTitle>Clusters</WorkspaceTitle>
            </WorkspaceHeader>

            <ContentContainer>
                <div className="max-w-screen-sm">
                    <DataTable columns={['Name', 'Id']} data={rows} renderRows={renderRows} />
                    <QueryLoadingPlaceholder query={clustersQuery} />
                </div>
            </ContentContainer>
        </div>
    );
};
