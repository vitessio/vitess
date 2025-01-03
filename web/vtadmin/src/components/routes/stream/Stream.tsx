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

import { useWorkflow } from '../../../hooks/api';
import { useDocumentTitle } from '../../../hooks/useDocumentTitle';
import { formatStreamKey, getStream } from '../../../util/workflows';
import { ContentContainer } from '../../layout/ContentContainer';
import { NavCrumbs } from '../../layout/NavCrumbs';
import { WorkspaceHeader } from '../../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../../layout/WorkspaceTitle';
import style from './Stream.module.scss';
import JSONViewTree from '../../jsonViewTree/JSONViewTree';
import { TabContainer } from '../../tabs/TabContainer';
import { Tab } from '../../tabs/Tab';
import { Code } from '../../Code';

interface RouteParams {
    clusterID: string;
    keyspace: string;
    streamID: string;
    tabletCell: string;
    tabletUID: string;
    workflowName: string;
}

export const Stream = () => {
    const params = useParams<RouteParams>();
    const { path, url } = useRouteMatch();
    const { data: workflow } = useWorkflow({
        clusterID: params.clusterID,
        keyspace: params.keyspace,
        name: params.workflowName,
    });

    const streamID = parseInt(params.streamID, 10);
    const tabletUID = parseInt(params.tabletUID, 10);
    const tabletAlias = { cell: params.tabletCell, uid: tabletUID };
    const streamKey = formatStreamKey({ id: streamID, tablet: tabletAlias });

    useDocumentTitle(`${streamKey} (${params.workflowName})`);

    const stream = getStream(workflow, streamKey);

    return (
        <div>
            <WorkspaceHeader>
                <NavCrumbs>
                    <Link to="/workflows">Workflows</Link>
                    <Link to={`/workflow/${params.clusterID}/${params.keyspace}/${params.workflowName}`}>
                        {params.workflowName}
                    </Link>
                </NavCrumbs>

                <WorkspaceTitle className="font-mono">{streamKey}</WorkspaceTitle>
                <div className={style.headingMeta}>
                    <span>
                        Cluster: <code>{params.clusterID}</code>
                    </span>
                    <span>
                        Target keyspace: <code>{params.keyspace}</code>
                    </span>
                </div>
            </WorkspaceHeader>
            <ContentContainer>
                <TabContainer>
                    <Tab text="JSON" to={`${url}/json`} />
                    <Tab text="JSON Tree" to={`${url}/json_tree`} />
                </TabContainer>

                <Switch>
                    <Route path={`${path}/json`}>{stream && <Code code={JSON.stringify(stream, null, 2)} />}</Route>
                    <Route path={`${path}/json_tree`}>{stream && <JSONViewTree data={stream} />}</Route>

                    <Redirect exact from={path} to={`${path}/json`} />
                </Switch>
            </ContentContainer>
        </div>
    );
};
