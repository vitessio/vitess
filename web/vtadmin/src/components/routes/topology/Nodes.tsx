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
import React from 'react';
import { MarkerType, Node, Edge } from 'react-flow-renderer';

export interface TopologyCell {
    name?: string;
    data?: string;
    path: string;
    children?: TopologyCellChild[];
}

export type TopologyCellChild = string | TopologyCell;

export const generateGraph = (
    topology: { cell: TopologyCellChild },
    onExpand: (path: string) => void
): { nodes: Array<Node>; edges: Array<Edge> } => {
    return getNodesAndEdges(topology.cell as TopologyCell, '', -1, 0, onExpand);
};

const getNodesAndEdges = (
    cell: TopologyCellChild,
    path: string,
    depth: number,
    width: number,
    onExpand: (path: string) => void
): { nodes: Array<Node>; edges: Array<Edge> } => {
    const isCell = typeof cell !== 'string';
    const isString = !isCell;
    const nodes: Array<Node> = [];
    const edges: Array<Edge> = [];
    if (isString || cell?.name) {
        const parentNode: Node = {
            id: path,
            position: { y: depth * 100, x: width * 150 },
            style: { width: 'min-content' },
            data: {
                label:
                    isCell && cell?.data ? (
                        <div className="w-fit">
                            <div className="font-bold">{cell.name}</div>
                            <div className="mt-1 bg-gray-100 p-2 text-[10px] text-left font-mono whitespace-normal">
                                {cell.data}
                            </div>
                        </div>
                    ) : (
                        <div className="font-bold">
                            {typeof cell === 'string' ? cell : cell.name}
                            <button onClick={() => onExpand(path)} className="btn btn-secondary btn-sm mt-1">
                                Expand
                            </button>
                        </div>
                    ),
            },
        };

        if (depth === 0) {
            parentNode.type = 'input';
        }

        if (isCell && !cell?.children) {
            parentNode.type = 'output';
        }

        nodes.push(parentNode);
    }

    if (isCell && cell?.children) {
        let offset = 0;
        cell.children.forEach((child, i) => {
            const childPath = `${path}/${typeof child == 'string' ? child : child.name}`;
            if (path !== '') {
                edges.push({
                    id: `${path}-${childPath}`,
                    source: path,
                    target: childPath,
                    markerEnd: {
                        type: MarkerType.ArrowClosed,
                    },
                });
            }

            const { nodes: childNodes, edges: childEdges } = getNodesAndEdges(
                child,
                childPath,
                depth + 1,
                width + offset,
                onExpand
            );
            nodes.push(...childNodes);
            edges.push(...childEdges);
            offset += maxWidth(child);
        });
    }

    return {
        nodes,
        edges,
    };
};

const maxWidth = (cell: TopologyCellChild): number => {
    let width = 0;

    if (typeof cell == 'string' || !cell.children || cell.children?.length === 0) {
        return 1;
    }

    cell.children?.forEach((child) => {
        const childWidth = maxWidth(child);
        width += childWidth;
    });

    return width;
};
