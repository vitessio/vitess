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

import { UseQueryResult } from 'react-query';

interface Props {
    query: UseQueryResult;
    title?: React.ReactNode;
}

/**
 * QueryErrorPlaceholder is a straightforward component that displays
 * errors returned from a useQuery hook. To simplify its use, this component
 * takes care of hiding itself when `props.query` is in any state other than an error.
 * It's perfectly fine to render it this:
 *
 *      <QueryErrorPlaceholder query={query} ... />
 *
 * ...conversely, it is NOT necessary (although also fine!) to do a check like this:
 *
 *      {query.isError && <QueryErrorPlaceholder query={query} .../>}
 */
export const QueryErrorPlaceholder: React.FC<Props> = ({ query, title = 'An error occurred' }) => {
    if (!query.isError || query.isLoading) {
        return null;
    }

    const message = (query.error as any).response?.error?.message || (query.error as any).message;

    return (
        <div aria-live="polite" className="my-12 text-center" role="status">
            <span className="text-[6rem]">😰</span>
            <div className="text-xl font-bold my-4">{title}</div>
            <code data-testid="error-message">{message}</code>

            <div className="my-12">
                <button className="btn btn-secondary" onClick={() => query.refetch()} type="button">
                    Try again
                </button>
            </div>
        </div>
    );
};
