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
import { Link, Redirect, Route, Switch, useParams, useRouteMatch } from 'react-router-dom';
import { useState } from 'react';

import style from './Workflow.module.scss';

import { useWorkflow } from '../../../hooks/api';
import { NavCrumbs } from '../../layout/NavCrumbs';
import { WorkspaceHeader } from '../../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../../layout/WorkspaceTitle';
import { useDocumentTitle } from '../../../hooks/useDocumentTitle';
import { KeyspaceLink } from '../../links/KeyspaceLink';
import { WorkflowStreams } from './WorkflowStreams';
import { WorkflowDetails } from './WorkflowDetails';
import { ContentContainer } from '../../layout/ContentContainer';
import { TabContainer } from '../../tabs/TabContainer';
import { Tab } from '../../tabs/Tab';
import { getStreams } from '../../../util/workflows';
import { ShardLink } from '../../links/ShardLink';
import { WorkflowVDiff } from './WorkflowVDiff';
import { Select } from '../../inputs/Select';
import { formatDateTimeShort } from '../../../util/time';
import JSONViewTree from '../../jsonViewTree/JSONViewTree';
import { Code } from '../../Code';

interface RouteParams {
    clusterID: string;
    keyspace: string;
    name: string;
}

const REFETCH_OPTIONS = [
    {
        displayText: '10s',
        interval: 10 * 1000,
    },
    {
        displayText: '30s',
        interval: 30 * 1000,
    },
    {
        displayText: '1m',
        interval: 60 * 1000,
    },
    {
        displayText: '10m',
        interval: 600 * 1000,
    },
    {
        displayText: 'Never',
        interval: 0,
    },
];

export const Workflow = () => {
    const { clusterID, keyspace, name } = useParams<RouteParams>();
    const { path, url } = useRouteMatch();

    useDocumentTitle(`${name} (${keyspace})`);

    const [refetchInterval, setRefetchInterval] = useState(60 * 1000);

    const { data, ...workflowQuery } = useWorkflow({ clusterID, keyspace, name }, { refetchInterval });
    const streams = getStreams(data);

    let isReshard = false;
    if (data && data.workflow) {
        isReshard = data.workflow.workflow_type === 'Reshard';
    }

    return (
        <div>
            <WorkspaceHeader>
                <div className="w-full flex flex-row justify-between">
                    <div>
                        <NavCrumbs>
                            <Link to="/workflows">Workflows</Link>
                        </NavCrumbs>
                        <WorkspaceTitle className="font-mono">{name}</WorkspaceTitle>
                    </div>

                    <div className="float-right">
                        <Select
                            className="block w-full"
                            inputClassName="block w-full"
                            itemToString={(option) => option?.displayText || ''}
                            items={REFETCH_OPTIONS}
                            label="Refresh Interval"
                            onChange={(option) => setRefetchInterval(option?.interval || 0)}
                            renderItem={(option) => option?.displayText || ''}
                            placeholder={'Select Interval'}
                            helpText={'Automatically refreshes workflow status after selected intervals'}
                            selectedItem={REFETCH_OPTIONS.find((option) => option.interval === refetchInterval)}
                            disableClearSelection
                        />
                        <div className="text-sm mt-2">{`Last updated: ${formatDateTimeShort(
                            workflowQuery.dataUpdatedAt / 1000
                        )}`}</div>
                    </div>
                </div>
                {isReshard && (
                    <div className={style.headingMetaContainer}>
                        <div className={style.headingMeta}>
                            <span>
                                {data?.workflow?.source?.shards?.length! > 1 ? 'Source Shards: ' : 'Source Shard: '}
                                {data?.workflow?.source?.shards?.map((shard) => (
                                    <code key={`${keyspace}/${shard}`}>
                                        <ShardLink
                                            className="mr-1"
                                            clusterID={clusterID}
                                            keyspace={keyspace}
                                            shard={shard}
                                        >
                                            {`${keyspace}/${shard}`}
                                        </ShardLink>
                                    </code>
                                ))}
                            </span>
                            <span>
                                {data?.workflow?.target?.shards?.length! > 1 ? 'Target Shards: ' : 'Target Shard: '}
                                {data?.workflow?.target?.shards?.map((shard) => (
                                    <code key={`${keyspace}/${shard}`}>
                                        <ShardLink
                                            className="mr-1"
                                            clusterID={clusterID}
                                            keyspace={keyspace}
                                            shard={shard}
                                        >
                                            {`${keyspace}/${shard}`}
                                        </ShardLink>
                                    </code>
                                ))}
                            </span>
                        </div>
                    </div>
                )}
                <div className={style.headingMetaContainer}>
                    <div className={style.headingMeta} style={{ float: 'left' }}>
                        <span>
                            Cluster: <code>{clusterID}</code>
                        </span>
                        <span>
                            Target Keyspace:{' '}
                            <KeyspaceLink clusterID={clusterID} name={keyspace}>
                                <code>{keyspace}</code>
                            </KeyspaceLink>
                        </span>
                    </div>
                </div>
            </WorkspaceHeader>

            <ContentContainer>
                <TabContainer>
                    <Tab text="Streams" to={`${url}/streams`} count={streams.length} />
                    <Tab text="Details" to={`${url}/details`} />
                    <Tab text="VDiff" to={`${url}/vdiff`} />
                    <Tab text="JSON" to={`${url}/json`} />
                    <Tab text="JSON Tree" to={`${url}/json_tree`} />
                </TabContainer>

                <Switch>
                    <Route path={`${path}/streams`}>
                        <WorkflowStreams clusterID={clusterID} keyspace={keyspace} name={name} />
                    </Route>

                    <Route path={`${path}/details`}>
                        <WorkflowDetails
                            clusterID={clusterID}
                            keyspace={keyspace}
                            name={name}
                            refetchInterval={refetchInterval}
                        />
                    </Route>

                    <Route path={`${path}/vdiff`}>
                        <WorkflowVDiff clusterID={clusterID} keyspace={keyspace} name={name} />
                    </Route>

                    <Route path={`${path}/json`}>
                        <Code code={JSON.stringify(data, null, 2)} />
                    </Route>

                    <Route path={`${path}/json_tree`}>
                        <JSONViewTree data={data} />
                    </Route>

                    <Redirect exact from={path} to={`${path}/streams`} />
                </Switch>
            </ContentContainer>
        </div>
    );
};
