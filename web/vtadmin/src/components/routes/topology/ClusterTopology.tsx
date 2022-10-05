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
import React, { useEffect } from 'react';

// import { useTopology } from '../../../hooks/api';
import { useDocumentTitle } from '../../../hooks/useDocumentTitle';
import { ContentContainer } from '../../layout/ContentContainer';
import { NavCrumbs } from '../../layout/NavCrumbs';
import { WorkspaceHeader } from '../../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../../layout/WorkspaceTitle';
import { Link, useParams } from 'react-router-dom';
import { generateGraph } from './Nodes';

import ReactFlow, {
  addEdge,
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  Connection,
} from 'react-flow-renderer';

export const ClusterTopology = () => {
  interface RouteParams {
    clusterID: string;
  }
  useDocumentTitle('Cluster');
  const { clusterID } = useParams<RouteParams>();
  // const { data } = useTopology({ clusterID });
  const data = null
  const { nodes: initialNodes, edges: initialEdges } = data ? generateGraph(data) : { nodes: [], edges: [] };

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  useEffect(() => {
    setNodes(initialNodes);
    setEdges(initialEdges);
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

  const onConnect = (params: Connection) => setEdges((eds) => addEdge(params, eds));

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