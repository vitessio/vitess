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

import { isEmpty } from 'lodash-es';
import { useShardReplicationPositions } from '../../../hooks/api';
import { vtadmin as pb } from '../../../proto/vtadmin';
import { formatAlias, formatPaddedAlias } from '../../../util/tablets';
import { Code } from '../../Code';
import { QueryErrorPlaceholder } from '../../placeholders/QueryErrorPlaceholder';
import { QueryLoadingPlaceholder } from '../../placeholders/QueryLoadingPlaceholder';

interface Props {
    tablet?: pb.Tablet;
}

export const TabletReplication: React.FC<Props> = ({ tablet }) => {
    const clusterID = tablet?.cluster?.id;
    const keyspace = tablet?.tablet?.keyspace;
    const shard = tablet?.tablet?.shard;
    const keyspaceShard = keyspace && shard ? `${keyspace}/${shard}` : null;
    const alias = formatAlias(tablet?.tablet?.alias);
    const paddedAlias = tablet ? formatPaddedAlias(tablet.tablet?.alias) : null;

    const q = useShardReplicationPositions({
        clusterIDs: [clusterID],
        keyspaces: [keyspace],
        keyspaceShards: [keyspaceShard],
    });

    // Loading of the tablet itself is (for now) handled by the parent component,
    // so allow it to handle the loading display, too.
    if (!tablet) {
        return null;
    }

    const positionsForShard = q.data?.replication_positions.find(
        (p) => p.cluster?.id === clusterID && p.keyspace === keyspace && p.shard === shard
    );

    const replicationStatuses = positionsForShard?.position_info?.replication_statuses;
    const replicationStatus = replicationStatuses && paddedAlias ? replicationStatuses[paddedAlias] : null;

    let content = null;
    if (q.isSuccess) {
        content = isEmpty(replicationStatus) ? (
            <div className="text-center text-secondary my-12">No replication status for {alias}</div>
        ) : (
            <Code code={JSON.stringify(replicationStatus, null, 2)} />
        );
    }

    return (
        <div>
            <QueryLoadingPlaceholder query={q} />
            <QueryErrorPlaceholder query={q} />
            {content}
        </div>
    );
};
