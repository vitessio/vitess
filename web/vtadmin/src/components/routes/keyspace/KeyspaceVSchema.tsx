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

import { useVSchema } from '../../../hooks/api';
import { Code } from '../../Code';
import { QueryErrorPlaceholder } from '../../placeholders/QueryErrorPlaceholder';
import { QueryLoadingPlaceholder } from '../../placeholders/QueryLoadingPlaceholder';

interface Props {
    clusterID: string;
    name: string;
}

export const KeyspaceVSchema = ({ clusterID, name }: Props) => {
    const query = useVSchema({ clusterID, keyspace: name });
    return (
        <div>
            <QueryLoadingPlaceholder query={query} />
            <QueryErrorPlaceholder query={query} title="Couldn't load VSchema" />
            {query.isSuccess && <Code code={JSON.stringify(query.data, null, 2)} />}
        </div>
    );
};
