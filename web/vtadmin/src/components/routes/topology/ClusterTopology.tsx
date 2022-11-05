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
import React, { useEffect, useState } from 'react';

import { useTopologyPath } from '../../../hooks/api';
import { useDocumentTitle } from '../../../hooks/useDocumentTitle';
import { ContentContainer } from '../../layout/ContentContainer';
import { NavCrumbs } from '../../layout/NavCrumbs';
import { WorkspaceHeader } from '../../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../../layout/WorkspaceTitle';
import { Link, useParams } from 'react-router-dom';
import { generateGraph, TopologyCell, TopologyCellChild } from './Nodes';

import ReactFlow, {
    addEdge,
    MiniMap,
    Controls,
    Background,
    useNodesState,
    useEdgesState,
    Connection,
} from 'react-flow-renderer';
import { getTopologyPath } from '../../../api/http';

export const ClusterTopology = () => {
    interface RouteParams {
        clusterID: string;
    }
    useDocumentTitle('Cluster Topolgy');
    const { clusterID } = useParams<RouteParams>();
    const { data } = useTopologyPath({ clusterID, path: '/' });
    const [topology, setTopology] = useState<{ cell: TopologyCell }>({ cell: data?.cell as TopologyCell });

    const [nodes, setNodes, onNodesChange] = useNodesState([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState([]);

    const onConnect = (params: Connection) => setEdges((eds) => addEdge(params, eds));
    const onExpand = async (path: string) => {
        const { cell } = await getTopologyPath({ clusterID, path });
        const newTopo = { ...topology };
        newTopo.cell.children = placeCell(newTopo.cell, cell as TopologyCell);
        setTopology(newTopo);
    };

    const placeCell = (currentCell: TopologyCell, newCell: TopologyCell): TopologyCellChild[] => {
        const newChildren: TopologyCellChild[] = [];
        currentCell.children?.forEach((c) => {
            if (typeof c === 'string' && c === newCell?.name) {
                newChildren.push(newCell as TopologyCell);
            }
            if (typeof c == 'string' && c !== newCell?.name) {
                newChildren.push(c);
            }
            if (typeof c !== 'string') {
                c.children = placeCell(c, newCell);
                newChildren.push(c);
            }
        });
        return newChildren;
    };

    useEffect(() => {
        const { nodes: initialNodes, edges: initialEdges } = topology
            ? generateGraph(topology, onExpand)
            : { nodes: [], edges: [] };
        setNodes(initialNodes);
        setEdges(initialEdges);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [topology]);

    useEffect(() => {
        if (data?.cell) {
            setTopology({ cell: data?.cell as TopologyCell });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
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
                <ReactFlow
                    nodes={nodes}
                    edges={edges}
                    onNodesChange={onNodesChange}
                    onEdgesChange={onEdgesChange}
                    onConnect={onConnect}
                    fitView
                    attributionPosition="top-right"
                >
                    <MiniMap
                        nodeStrokeColor={(n) => {
                            if (n.style?.background) return n.style.background as string;
                            if (n.type === 'input') return '#0041d0';
                            if (n.type === 'output') return '#ff0072';
                            if (n.type === 'default') return '#1a192b';

                            return '#eee';
                        }}
                        nodeColor={(n) => {
                            if (n.style?.background) return n.style.background as string;

                            return '#fff';
                        }}
                        nodeBorderRadius={2}
                    />
                    <Controls />
                    <Background color="#aaa" gap={16} />
                </ReactFlow>
            </ContentContainer>
        </div>
    );
};
