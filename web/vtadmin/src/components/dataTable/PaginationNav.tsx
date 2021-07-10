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
import cx from 'classnames';
import * as React from 'react';
import { Link, LinkProps } from 'react-router-dom';

import style from './PaginationNav.module.scss';

export interface Props {
    currentPage: number;
    formatLink: (page: number) => LinkProps['to'];
    // The maximum number of pagination elements to show. Note that this includes any placeholders.
    // It's recommended for this value to be >= 5 to handle the case where there are
    // breaks on either side of the list.
    maxVisible?: number;
    // The minimum number of pagination elements to show at the beginning/end of a sequence,
    // adjacent to any sequence breaks.
    minWidth?: number;
    // The total number of pages
    totalPages: number;
}

const DEFAULT_MAX_VISIBLE = 8;
const DEFAULT_MIN_WIDTH = 1;

// This assumes we always want to 1-index our pages, where "page 1" is the first page.
// If we find a need for zero-indexed pagination, we can make this configurable.
const FIRST_PAGE = 1;

// PageSpecifiers with a numeric value are links. `null` is used
// to signify a break in the sequence.
type PageSpecifier = number | null;

export const PaginationNav = ({
    currentPage,
    formatLink,
    maxVisible = DEFAULT_MAX_VISIBLE,
    minWidth = DEFAULT_MIN_WIDTH,
    totalPages,
}: Props) => {
    if (totalPages <= 1) {
        return null;
    }

    // This rather magical solution is borrowed, with gratitude, from StackOverflow
    // https://stackoverflow.com/a/46385144
    const leftWidth = (maxVisible - minWidth * 2 - 3) >> 1;
    const rightWidth = (maxVisible - minWidth * 2 - 2) >> 1;

    let numbers: PageSpecifier[] = [];
    if (totalPages <= maxVisible) {
        // No breaks in list
        numbers = range(FIRST_PAGE, totalPages);
    } else if (currentPage <= maxVisible - minWidth - 1 - rightWidth) {
        // No break on left side of page
        numbers = range(FIRST_PAGE, maxVisible - minWidth - 1).concat(
            null,
            range(totalPages - minWidth + 1, totalPages)
        );
    } else if (currentPage >= totalPages - minWidth - 1 - rightWidth) {
        // No break on right of page
        numbers = range(FIRST_PAGE, minWidth).concat(
            null,
            range(totalPages - minWidth - 1 - rightWidth - leftWidth, totalPages)
        );
    } else {
        // Breaks on both sides
        numbers = range(FIRST_PAGE, minWidth).concat(
            null,
            range(currentPage - leftWidth, currentPage + rightWidth),
            null,
            range(totalPages - minWidth + 1, totalPages)
        );
    }

    return (
        <nav>
            <ul className={style.links}>
                {numbers.map((num: number | null, idx) =>
                    num === null ? (
                        <li key={`placeholder-${idx}`}>
                            <div className={style.placeholder} />
                        </li>
                    ) : (
                        <li key={num}>
                            <Link
                                className={cx(style.link, { [style.activeLink]: num === currentPage })}
                                to={formatLink(num)}
                            >
                                {num}
                            </Link>
                        </li>
                    )
                )}
            </ul>
        </nav>
    );
};

// lodash-es has a `range` function but it doesn't play nice
// with the PageSpecifier[] return type (since it's a mixed array
// of numbers and nulls).
const range = (start: number, end: number): PageSpecifier[] => {
    if (isNaN(start) || isNaN(end)) return [];
    return Array.from(Array(end - start + 1), (_, i) => i + start);
};
