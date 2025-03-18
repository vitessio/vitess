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

import { describe, expect, it } from 'vitest';
import { JSX } from 'react/jsx-runtime';
import { SortedDataTable } from './SortedDataTable';
import { fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';

describe('SortedDataTable', () => {
    it('SortedDataTable renders successfully', () => {
        const columnProps = [
            { display: 'col1', accessor: 'col1' },
            { display: <div>col2</div>, accessor: 'a_col2' },
        ];
        const testData = [
            { col1: 'dcol1', a_col2: '20' },
            { col1: 'dcol11', a_col2: '10' },
        ];

        render(
            <MemoryRouter initialEntries={[{ pathname: '/schemas', totalPages: 10, pageQueryKey: 'page' }]}>
                <SortedDataTable
                    columns={columnProps}
                    data={testData}
                    renderRows={function (rows: any[]): JSX.Element[] {
                        return rows.map((item, idx) => {
                            return (
                                <tr key={idx}>
                                    <td> {item.col1} </td> <td> {item.col2} </td>
                                </tr>
                            );
                        });
                    }}
                />
            </MemoryRouter>
        );
        expect(screen.getAllByRole('table').length).toBe(1);
        expect(screen.getAllByRole('row').length).toBe(3);
        expect(screen.getAllByRole('columnheader').length).toBe(2);

        // Check onClick on column
        const column1 = screen.getByText('col1');
        fireEvent.click(column1);
        // dependency on vite-jest to check useState sortColumn if col1 gets set.
    });
});
