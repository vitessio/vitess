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
import React, { useEffect, useState } from 'react';

import { useTopologyPath } from '../../../hooks/api';
import { useDocumentTitle } from '../../../hooks/useDocumentTitle';
import { ContentContainer } from '../../layout/ContentContainer';
import { NavCrumbs } from '../../layout/NavCrumbs';
import { WorkspaceHeader } from '../../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../../layout/WorkspaceTitle';
import { Link, useParams } from 'react-router-dom';

import { Node } from './Node';
import { vtctldata } from '../../../proto/vtadmin';

export interface TopologyNode {
    name?: string | null;
    data?: string | null;
    path: string;
    children?: TopologyNode[];
}

export const buildChildNodes = (cell: vtctldata.ITopologyCell | null | undefined, path: string) => {
    if (cell) {
        const childNodes: TopologyNode[] | undefined = cell.children
            ? cell.children.map((child) => {
                  return {
                      name: child,
                      path,
                  };
              })
            : undefined;
        return childNodes;
    }
};

export const TopologyTree = () => {
    interface RouteParams {
        clusterID: string;
    }
    useDocumentTitle('Cluster Topolgy');
    const { clusterID } = useParams<RouteParams>();
    const { data } = useTopologyPath({ clusterID, path: '/' });
    const [topologyNode, setTopologyNode] = useState<TopologyNode | undefined>();

    useEffect(() => {
        if (data?.cell) {
            const topologyNode: TopologyNode = {
                path: data.cell.path || '/',
                data: data.cell.data,
                children: buildChildNodes(data.cell, ''),
            };
            setTopologyNode(topologyNode);
        }
    }, [data]);

    if (!data) {
        return (
            <div>
                <WorkspaceHeader>
                    <NavCrumbs>
                        <Link to="/topology">Topology</Link>
                    </NavCrumbs>

                    <WorkspaceTitle className="font-mono">{clusterID}</WorkspaceTitle>
                </WorkspaceHeader>

                <ContentContainer>404</ContentContainer>
            </div>
        );
    }

    return (
        <div>
            <WorkspaceHeader>
                <NavCrumbs>
                    <Link to="/topology">Topology</Link>
                </NavCrumbs>
                <WorkspaceTitle className="font-mono">{clusterID}</WorkspaceTitle>
            </WorkspaceHeader>

            <ContentContainer className="lg:w-[1400px] lg:h-[1200px] md:w-[900px] md:h-[800px]">
                {topologyNode &&
                    topologyNode.children?.map((child, idx) => (
                        <Node key={idx} topologyNode={child} clusterID={clusterID} />
                    ))}
            </ContentContainer>
        </div>
    );
};
