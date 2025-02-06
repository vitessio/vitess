/**
 * Copyright 2025 The Vitess Authors.
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
import * as React from 'react';
import { useLocation } from 'react-router-dom';

import { useURLPagination } from '../../hooks/useURLPagination';
import { useURLQuery } from '../../hooks/useURLQuery';
import { stringify } from '../../util/queryString';
import { PaginationNav } from './PaginationNav';
import { useCallback, useMemo, useState } from 'react';
import { Icon, Icons } from '../Icon';

export interface ColumnProps {
    // Coulmn display name string | JSX.Element
    display: string | JSX.Element;
    // Column data accessor
    accessor: string;
}

interface Props<T> {
    // When passing a JSX.Element, note that the column element
    // will be rendered *inside* a <th> tag. (Note: I don't love this
    // abstraction + we'll likely want to revisit this when we add
    // table sorting.)
    columns: Array<ColumnProps>;
    data: T[];
    pageSize?: number;
    renderRows: (rows: T[]) => JSX.Element[];
    title?: string;
    // Pass a unique `pageKey` for each DataTable, in case multiple
    // DataTables access the same URL. This will be used to
    // access page number from the URL.
    pageKey?: string;
}

type SortOrder = 'asc' | 'desc';

// Generally, page sizes of ~100 rows are fine in terms of performance,
// but anything over ~50 feels unwieldy in terms of UX.
const DEFAULT_PAGE_SIZE = 50;

export const SortedDataTable = <T extends object>({
    columns,
    data,
    pageSize = DEFAULT_PAGE_SIZE,
    renderRows,
    title,
    pageKey = '',
}: Props<T>) => {
    const { pathname } = useLocation();
    const urlQuery = useURLQuery();

    const pageQueryKey = `${pageKey}page`;

    const totalPages = Math.ceil(data.length / pageSize);
    const { page } = useURLPagination({ totalPages, pageQueryKey });

    const startIndex = (page - 1) * pageSize;
    const endIndex = startIndex + pageSize;

    const startRow = startIndex + 1;
    const lastRow = Math.min(data.length, startIndex + pageSize);

    const formatPageLink = (p: number) => ({
        pathname,
        search: stringify({ ...urlQuery.query, [pageQueryKey]: p === 1 ? undefined : p }),
    });

    const [sortColumn, setSortColumn] = useState<null | string>(null);
    const [sortOrder, setSortOrder] = useState<SortOrder>('asc');

    const handleSort = useCallback(
        (column: any) => {
            if (sortColumn === column) {
                setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
            } else {
                setSortColumn(column);
                setSortOrder('asc');
            }
        },
        [sortColumn, sortOrder]
    );

    const sortedData = useMemo(() => {
        if (!sortColumn) return data;

        const compare = (a: { [x: string]: any }, b: { [x: string]: any }) => {
            const valueA = a[sortColumn];
            const valueB = b[sortColumn];

            if (valueA < valueB) {
                return sortOrder === 'asc' ? -1 : 1;
            } else if (valueA > valueB) {
                return sortOrder === 'asc' ? 1 : -1;
            } else {
                return 0;
            }
        };

        return [...data].sort(compare);
    }, [data, sortColumn, sortOrder]);

    const dataPage = sortedData.slice(startIndex, endIndex);

    return (
        <div>
            <table className="table-fixed">
                {title && <caption>{title}</caption>}
                <thead>
                    <tr>
                        {columns.map((col, cdx) => (
                            <SortTableHeader
                                col={col}
                                cdx={cdx}
                                sortColumn={sortColumn}
                                sortOrder={sortOrder}
                                handleSort={handleSort}
                            ></SortTableHeader>
                        ))}
                    </tr>
                </thead>
                <tbody>{renderRows(dataPage)}</tbody>
            </table>

            <PaginationNav currentPage={page} formatLink={formatPageLink} totalPages={totalPages} />
            {!!data.length && (
                <p className="text-secondary">
                    Showing {startRow} {lastRow > startRow ? `- ${lastRow}` : null} of {data.length}
                </p>
            )}
        </div>
    );
};

type SortTableHeaderProps = {
    col: ColumnProps;
    cdx: number;
    sortOrder: SortOrder;
    sortColumn: null | string;
    handleSort: (column: any) => void;
};

const SortTableHeader: React.FC<SortTableHeaderProps> = ({ col, cdx, sortOrder, sortColumn, handleSort }) => {
    const upFillColor = sortOrder === 'asc' && sortColumn === col.accessor ? 'fill-current' : 'fill-gray-300';
    const downFillColor = sortOrder !== 'asc' && sortColumn === col.accessor ? 'fill-current' : 'fill-gray-300';
    return (
        <th key={cdx} onClick={() => handleSort(col.accessor)}>
            <div className="flex cursor-pointer items-center">
                <div className="mr-1">{col.display}</div>
                <div>
                    <Icon className={`${upFillColor} -mb-1`} icon={Icons.chevronUp} />
                    <Icon className={`${downFillColor} -mt-1`} icon={Icons.chevronDown} />
                </div>
            </div>
        </th>
    );
};
