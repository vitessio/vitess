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

import * as d3 from 'd3';
import { useEffect, useMemo, useRef } from 'react';
import { TimeseriesMap, TimeseriesPoint } from '../../util/tabletDebugVars';

const MARGIN = { top: 30, right: 30, bottom: 50, left: 50 };
const width = 1000;
const height = 500;

type LineChartProps = {
    isLoading: boolean;
    timeseriesMap: TimeseriesMap;
};

export const D3Timeseries = ({ isLoading, timeseriesMap }: LineChartProps) => {
    // bounds = area inside the graph axis = calculated by substracting the margins
    const axesRef = useRef(null);
    const boundsWidth = width - MARGIN.right - MARGIN.left;
    const boundsHeight = height - MARGIN.top - MARGIN.bottom;

    const [xRanges, yRanges] = axisMinsAndMaxes(timeseriesMap, boundsWidth);
    const yMax = yRanges[1];
    const yScale = useMemo(() => {
        return d3
            .scaleLinear()
            .domain([0, yMax || 0])
            .range([boundsHeight, 0]);
    }, [boundsHeight, yMax]);

    const xScale = useMemo(() => {
        return d3.scaleTime().domain(xRanges).range([0, boundsWidth]);
    }, [boundsWidth, xRanges]);

    // Render the X and Y axis using d3.js, not react
    useEffect(() => {
        const svgElement = d3.select(axesRef.current);
        svgElement.selectAll('*').remove();

        // Render X Axis
        const xAxisGenerator = d3.axisBottom<Date>(xScale);
        xAxisGenerator.tickFormat(d3.timeFormat('%H:%M'));
        svgElement
            .append('g')
            .attr('transform', 'translate(0,' + boundsHeight + ')')
            .call(xAxisGenerator)
            .selectAll('text')
            .attr('class', 'fill-gray-500 font-mono text-medium');

        // Render Y Axis
        const yAxisGenerator = d3.axisLeft(yScale);
        svgElement
            .append('g')
            .call(yAxisGenerator)
            .selectAll('text')
            .attr('class', 'fill-gray-500 font-mono text-medium');
        svgElement.selectAll('path').attr('class', '!stroke-gray-200');
        svgElement.selectAll('line').attr('class', '!stroke-gray-200 z-10');
    }, [xScale, yScale, boundsHeight]);

    // Build the line
    const lineBuilder = d3
        .line<TimeseriesPoint>()
        .x((d) => xScale(d.x))
        .y((d) => yScale(d.y));

    const colors = d3.scaleOrdinal(d3.schemePiYG[11]);

    return (
        <div>
            <svg width={width} height={height}>
                <g
                    width={boundsWidth}
                    height={boundsHeight}
                    transform={`translate(${[MARGIN.left, MARGIN.top].join(',')})`}
                >
                    {Object.entries(timeseriesMap).map(([name, ts], i) => (
                        <Line color={colors(name)} key={name} timeseriesPoints={ts} lineBuilder={lineBuilder} />
                    ))}
                </g>
                <g
                    width={boundsWidth}
                    height={boundsHeight}
                    ref={axesRef}
                    transform={`translate(${[MARGIN.left, MARGIN.top].join(',')})`}
                />
            </svg>
            <Legend colors={colors} names={Object.keys(timeseriesMap)} />
        </div>
    );
};

const Legend: React.FC<{ names: string[]; colors: d3.ScaleOrdinal<string, string, never> }> = ({ names, colors }) => {
    return (
        <div className="width-full flex items-center justify-center">
            {names.map((name) => (
                <div className="mr-6 font-mono text-sm flex items-center justify-center" key={`${name}_legend`}>
                    <div className="h-5 w-5 mr-2" style={{ backgroundColor: colors(name) }} />
                    {name}
                </div>
            ))}
        </div>
    );
};

type LineProps = {
    color: string;
    timeseriesPoints: TimeseriesPoint[];
    lineBuilder: d3.Line<TimeseriesPoint>;
};

const Line: React.FC<LineProps> = ({ color, timeseriesPoints, lineBuilder }) => {
    const linePath = lineBuilder(timeseriesPoints);
    if (!linePath) {
        return null;
    }

    return <path d={linePath} opacity={1} stroke={color} fill="none" strokeWidth={2} className="z-100" />;
};

const axisMinsAndMaxes = (timeseriesMap: TimeseriesMap, boundsWidth: number): [[Date, Date], [number, number]] => {
    const x_values: number[] = Object.values(timeseriesMap)
        .map((points) => points.map((point) => point.x))
        .flat();
    const y_values = Object.values(timeseriesMap)
        .map((points) => points.map((point) => point.y))
        .flat();
    const y_max = d3.max(y_values) as number;
    const x_ranges = d3.extent(x_values.map((x) => new Date(x))) as [Date, Date];
    return [x_ranges, [0, y_max || 1]];
};
