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
import { render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { PaginationNav, Props } from './PaginationNav';

const formatLink = (page: number) => ({
    pathname: '/test',
    search: `?hello=world&page=${page}`,
});

describe('PaginationNav', () => {
    const tests: {
        name: string;
        props: Props;
        expected: null | Array<number | null>;
    }[] = [
        {
            name: 'renders without breaks',
            props: { currentPage: 1, formatLink, maxVisible: 3, totalPages: 2 },
            expected: [1, 2],
        },
        {
            name: 'renders breaks on the right',
            props: { currentPage: 1, formatLink, maxVisible: 5, totalPages: 11 },
            expected: [1, 2, 3, null, 11],
        },
        {
            name: 'renders breaks on the left',
            props: { currentPage: 11, formatLink, maxVisible: 5, totalPages: 11 },
            expected: [1, null, 9, 10, 11],
        },
        {
            name: 'renders breaks in the middle',
            props: { currentPage: 6, formatLink, maxVisible: 5, totalPages: 11 },
            expected: [1, null, 6, null, 11],
        },
        {
            name: 'renders widths according to the minWidth prop',
            props: { currentPage: 6, formatLink, maxVisible: 9, minWidth: 2, totalPages: 100 },
            expected: [1, 2, null, 5, 6, 7, null, 99, 100],
        },
        {
            name: 'does not render if totalPages == 0',
            props: { currentPage: 1, formatLink, totalPages: 0 },
            expected: null,
        },
        {
            name: 'renders even if page > totalPages',
            props: { currentPage: 100000, formatLink, maxVisible: 5, totalPages: 11 },
            expected: [1, null, 9, 10, 11],
        },
    ];

    test.each(tests.map(Object.values))('%s', (name: string, props: Props, expected: Array<number | null>) => {
        render(<PaginationNav {...props} />, { wrapper: MemoryRouter });

        const nav = screen.queryByRole('navigation');
        if (expected === null) {
            expect(nav).toBeNull();
            return;
        }

        const lis = screen.getAllByRole('listitem');
        expect(lis).toHaveLength(expected.length);

        lis.forEach((li, idx) => {
            const e = expected[idx];
            const link = within(li).queryByRole('link');

            if (e === null) {
                // Placeholders don't render links
                expect(link).toBeNull();
            } else {
                expect(link).toHaveAttribute('href', `/test?hello=world&page=${e}`);
                expect(link).toHaveTextContent(`${e}`);

                if (e === props.currentPage) {
                    expect(link).toHaveClass('activeLink');
                } else {
                    expect(link).not.toHaveClass('activeLink');
                }
            }
        });
    });
});
