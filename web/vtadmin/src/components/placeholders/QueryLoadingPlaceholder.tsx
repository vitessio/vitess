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
import { Spinner } from '../Spinner';

interface Props {
    query: UseQueryResult;
}

/**
 * QueryLoadingPlaceholder is a straightforward component that displays a loading
 * message when given a query from a useQueryHook. To simplify its use, this component
 * takes care of hiding itself when `props.query` is in any state other than "loading".
 * It's perfectly fine to render it this:
 *
 *      <QueryLoadingPlaceholder query={query} ... />
 *
 * ...conversely, it is NOT necessary (although also fine!) to do a check like this:
 *
 *      {query.isLoading && <QueryLoadingPlaceholder query={query} ... />}
 */
export const QueryLoadingPlaceholder: React.FC<Props> = (props) => {
    if (!props.query.isLoading) {
        return null;
    }

    return (
        <div aria-busy="true" aria-live="polite" className="text-center my-12" role="status">
            <Spinner />
            <div className="my-4 text-secondary">
                {props.query.failureCount > 2 ? 'Still loading...' : 'Loading...'}
            </div>
        </div>
    );
};
