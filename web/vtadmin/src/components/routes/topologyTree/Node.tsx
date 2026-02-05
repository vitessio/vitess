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
import { buildChildNodes, TopologyNode } from './TopologyTree';

interface NodeParams {
    topologyNode: TopologyNode;
    clusterID: string;
}

export const Node = ({ topologyNode, clusterID }: NodeParams) => {
    const [isOpen, setIsOpen] = useState<boolean>(false);
    const [node, setNode] = useState<TopologyNode>(topologyNode);

    const childrenPath = `${topologyNode.path}/${topologyNode.name}`;
    const { data } = useTopologyPath(
        { clusterID, path: childrenPath },
        {
            enabled: isOpen,
        }
    );

    useEffect(() => {
        if (data) {
            setNode((prevNode) => ({
                ...prevNode,
                children: buildChildNodes(data.cell, childrenPath),
                data: data.cell?.data,
            }));
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data]);

    const nodeTitle = `${isOpen ? '▼' : '►'} ${node.name}`;
    return (
        <div className="border-l border-x-zinc-300 pl-2 mt-4">
            <div className="w-fit cursor-pointer font-bold text-blue-500" onClick={() => setIsOpen(!isOpen)}>
                {nodeTitle}
            </div>

            {isOpen && (
                <div className="w-fit ml-4">
                    {node.data ? (
                        <div className="max-w-[300px] mt-1 bg-gray-100 p-2 text-[10px] text-left font-mono whitespace-normal">
                            {node.data}
                        </div>
                    ) : (
                        <>
                            {node.children &&
                                node.children.map((child, idx) => {
                                    return <Node key={idx} clusterID={clusterID} topologyNode={child} />;
                                })}
                        </>
                    )}
                </div>
            )}
        </div>
    );
};
