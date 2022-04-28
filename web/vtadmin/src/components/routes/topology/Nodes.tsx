import React from 'react';
import { MarkerType, Node, Edge } from 'react-flow-renderer';
import { vtctldata } from '../../../proto/vtadmin';

export const generateGraph = (topology: vtctldata.GetTopologyResponse): { nodes: Array<Node>, edges: Array<Edge> } => {
    const nodes: Array<Node> = []
    const edges: Array<Edge> = []

    let offset = 0
    topology.cells.forEach((cell, i) => {
        const { nodes: childNodes, edges: childEdges } = getNodesAndEdges(cell, cell.name as string, 0, i + offset)
        nodes.push(...childNodes)
        edges.push(...childEdges)
        offset += maxWidth(cell)
    })

    return {
        nodes,
        edges
    }
}

const getNodesAndEdges = (cell: vtctldata.ITopologyCell, path: string, depth: number, width: number): { nodes: Array<Node>, edges: Array<Edge> } => {
    const nodes: Array<Node> = []
    const edges: Array<Edge> = []

    const parentNode: Node = {
        id: path,
        position: { y: depth * 100, x: width * 150 },
        style: { width: 'min-content' },
        data: {
            label: cell.data ? (
                <div className="w-fit">
                    <div className="font-bold">{cell.name}</div>
                    <div className="mt-1 bg-gray-100 p-2 text-[10px] text-left font-mono whitespace-normal">
                        {cell.data}
                    </div>
                </div>
            ) : <div className="font-bold">{cell.name}</div>,
        }
    }

    if (depth === 0) {
        parentNode.type = "input"
    }

    if (!cell.children) {
        parentNode.type = "output"
    }

    nodes.push(parentNode)

    if (cell.children) {
        let offset = 0
        cell.children.forEach((child, i) => {
            const childPath = `${path}/${child.name}`
            edges.push({
                id: `${path}-${childPath}`,
                source: path,
                target: childPath,
                markerEnd: {
                    type: MarkerType.ArrowClosed,
                },
            })
            const { nodes: childNodes, edges: childEdges } = getNodesAndEdges(child, childPath, depth + 1, width + offset)
            nodes.push(...childNodes)
            edges.push(...childEdges)
            offset += maxWidth(child)
        })
    }

    return {
        nodes,
        edges
    }
}

const maxWidth = (cell: vtctldata.ITopologyCell): number => {
    let width = 0

    if (!cell.children) {
        return 1
    }

    cell.children?.forEach(child => {
        const childWidth = maxWidth(child)
        width += childWidth
    })

    return width
}