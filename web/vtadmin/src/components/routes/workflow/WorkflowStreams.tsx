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

import React from 'react';

import { WorkflowStreamsLagChart } from '../../charts/WorkflowStreamsLagChart';
import { env } from '../../../util/env';

interface Props {
    clusterID: string;
    keyspace: string;
    name: string;
}

export const WorkflowStreams = ({ clusterID, keyspace, name }: Props) => {
    return (
        <div className="mt-12 mb-16">
            {env().VITE_ENABLE_EXPERIMENTAL_TABLET_DEBUG_VARS && (
                <>
                    <h3 className="my-8">Stream VReplication Lag</h3>
                    <WorkflowStreamsLagChart clusterID={clusterID} keyspace={keyspace} workflowName={name} />
                </>
            )}
        </div>
    );
};
