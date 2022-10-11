/**
 * Copyright 2022 The Vitess Authors.
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

import { useClusters } from '../../../hooks/api';
import { useDocumentTitle } from '../../../hooks/useDocumentTitle';
import { DataTable } from '../../dataTable/DataTable';
import { vtadmin as pb } from '../../../proto/vtadmin';
import { DataCell } from '../../dataTable/DataCell';
import { ContentContainer } from '../../layout/ContentContainer';
import { WorkspaceHeader } from '../../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../../layout/WorkspaceTitle';
import { Link } from 'react-router-dom';

const TopologyLink: React.FC<{ clusterID: string }> = ({ clusterID, children }) => {
    const to = {
        pathname: `/topology/${clusterID}`,
    };

    return (
        <Link className="font-bold" to={to}>
            {children}
        </Link>
    );
};
export const Topology = () => {
    useDocumentTitle('Topology');
    const { data } = useClusters();

    const rows = React.useMemo(() => {
        return orderBy(data, ['name']);
    }, [data]);

    const renderRows = (rows: pb.Cluster[]) =>
        rows.map((cluster, idx) => (
            <tr key={idx}>
                <DataCell>{cluster.name}</DataCell>
                <DataCell>{cluster.id}</DataCell>
                <DataCell>
                    <TopologyLink clusterID={cluster.id}>View Topology</TopologyLink>
                </DataCell>
            </tr>
        ));

    return (
        <div>
            <WorkspaceHeader>
                <WorkspaceTitle>Topology</WorkspaceTitle>
            </WorkspaceHeader>

            <ContentContainer>
                <div className="max-w-screen-sm">
                    <div className="text-xl font-bold">Clusters</div>
                    <DataTable columns={['Name', 'Id', 'Topology']} data={rows} renderRows={renderRows} />
                </div>
            </ContentContainer>
        </div>
    );
};
