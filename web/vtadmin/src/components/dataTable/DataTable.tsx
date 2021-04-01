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
import * as React from 'react';
import { useLocation } from 'react-router-dom';

import { useURLPagination } from '../../hooks/useURLPagination';
import { useURLQuery } from '../../hooks/useURLQuery';
import { stringify } from '../../util/queryString';
import { PaginationNav } from './PaginationNav';

interface Props<T> {
    columns: string[];
    data: T[];
    pageSize?: number;
    renderRows: (rows: T[]) => JSX.Element[];
}

// Generally, page sizes of ~100 rows are fine in terms of performance,
// but anything over ~50 feels unwieldy in terms of UX.
const DEFAULT_PAGE_SIZE = 50;

export const DataTable = <T extends object>({ columns, data, pageSize = DEFAULT_PAGE_SIZE, renderRows }: Props<T>) => {
    const { pathname } = useLocation();
    const urlQuery = useURLQuery();

    const totalPages = Math.ceil(data.length / pageSize);
    const { page } = useURLPagination({ totalPages });

    const startIndex = (page - 1) * pageSize;
    const endIndex = startIndex + pageSize;
    const dataPage = data.slice(startIndex, endIndex);

    const startRow = startIndex + 1;
    const lastRow = Math.min(data.length, startIndex + pageSize);

    const formatPageLink = (p: number) => ({
        pathname,
        search: stringify({ ...urlQuery.query, page: p === 1 ? undefined : p }),
    });

    return (
        <div>
            <table>
                <thead>
                    <tr>
                        {columns.map((col, cdx) => (
                            <th key={cdx}>{col}</th>
                        ))}
                    </tr>
                </thead>
                <tbody>{renderRows(dataPage)}</tbody>
            </table>

            <PaginationNav currentPage={page} formatLink={formatPageLink} totalPages={totalPages} />
            {!!data.length && (
                <p className="text-color-secondary">
                    Showing {startRow} {lastRow > startRow ? `- ${lastRow}` : null} of {data.length}
                </p>
            )}
        </div>
    );
};
