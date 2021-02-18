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
import { useEffect } from 'react';

import { useHistory, useLocation } from 'react-router-dom';
import { useURLQuery } from './useURLQuery';

export interface PaginationOpts {
    totalPages: number;
}

export interface PaginationParams {
    page: number;
}

// This assumes we always want to 1-index our pages, where "page 1" is the first page.
// If we find a need for zero-indexed pagination, we can make this configurable.
const FIRST_PAGE = 1;

/**
 * useURLPagination is a hook for components that:
 * 	- use pagination in some way
 * 	- encode pagination state in the URL (e.g., /some/route?page=123)
 */
export const useURLPagination = ({ totalPages }: PaginationOpts): PaginationParams => {
    const history = useHistory();
    const location = useLocation();
    const { query, replaceQuery } = useURLQuery();

    // A slight nuance here -- if `page` is not in the URL at all, then we can assume
    // it's the first page. This makes for slightly nicer URLs for the first/default page:
    // "/foo" instead of "/foo?page=1". No redirect required.
    const page = !('page' in query) || query.page === null ? FIRST_PAGE : query.page;

    useEffect(() => {
        // If the value in the URL *is* defined but is negative, non-numeric,
        // too big, or otherwise Weird, then we *do* want to redirect to the first page.
        const isPageTooBig = typeof page === 'number' && totalPages > 0 && page > totalPages;
        const isPageTooSmall = typeof page === 'number' && page < FIRST_PAGE;

        if (isPageTooBig || isPageTooSmall || typeof page !== 'number') {
            // Replace history so the invalid value is not persisted in browser history
            replaceQuery({ page: FIRST_PAGE });
        }
    }, [page, totalPages, history, location.pathname, query, replaceQuery]);

    return {
        page,
    } as PaginationParams;
};
