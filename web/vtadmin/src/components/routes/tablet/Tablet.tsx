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
import { useExperimentalTabletDebugVars, useTablet } from '../../../hooks/api';
import { useDocumentTitle } from '../../../hooks/useDocumentTitle';
import { formatDisplayType, formatState } from '../../../util/tablets';
import { Code } from '../../Code';
import { ContentContainer } from '../../layout/ContentContainer';
import { NavCrumbs } from '../../layout/NavCrumbs';
import { WorkspaceHeader } from '../../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../../layout/WorkspaceTitle';
import { ExternalTabletLink } from '../../links/ExternalTabletLink';
import { TabletServingPip } from '../../pips/TabletServingPip';
import { Tab } from '../../tabs/Tab';
import { TabContainer } from '../../tabs/TabContainer';
import style from './Tablet.module.scss';
import { TabletCharts } from './TabletCharts';

interface RouteParams {
    alias: string;
    clusterID: string;
}

export const Tablet = () => {
    const { clusterID, alias } = useParams<RouteParams>();
    const { path, url } = useRouteMatch();

    useDocumentTitle(alias);

    const { data: tablet, ...tq } = useTablet({ alias, clusterID });
    const { data: debugVars } = useExperimentalTabletDebugVars({ alias, clusterID });

    if (tq.error) {
        return (
            <div className={style.placeholder}>
                <span className={style.errorEmoji}>😰</span>
                <h1>An error occurred</h1>
                <code>{(tq.error as any).response?.error?.message || tq.error?.message}</code>
                <p>
                    <Link to="/tablets">← All tablets</Link>
                </p>
            </div>
        );
    }

    if (!tq.isLoading && !tablet) {
        return (
            <div className={style.placeholder}>
                <span className={style.errorEmoji}>😖</span>
                <h1>Tablet not found</h1>
                <p>
                    <Link to="/tablets">← All tablets</Link>
                </p>
            </div>
        );
    }

    return (
        <div>
            <WorkspaceHeader>
                <NavCrumbs>
                    <Link to="/tablets">Tablets</Link>
                </NavCrumbs>

                <WorkspaceTitle className="font-family-monospace">{alias}</WorkspaceTitle>

                <div className={style.headingMeta}>
                    <span>
                        Cluster: <code>{clusterID}</code>
                    </span>
                    {!!tablet && (
                        <>
                            <span className="font-family-monospace">
                                <TabletServingPip state={tablet.state} /> {formatDisplayType(tablet)}
                            </span>
                            <span className="font-family-monospace">{formatState(tablet)}</span>
                            <span>
                                <ExternalTabletLink className="font-family-monospace" fqdn={tablet.FQDN}>
                                    {tablet.tablet?.hostname}
                                </ExternalTabletLink>
                            </span>
                        </>
                    )}
                </div>
            </WorkspaceHeader>

            <ContentContainer>
                <TabContainer>
                    <Tab text="QPS" to={`${url}/qps`} />
                    <Tab text="JSON" to={`${url}/json`} />
                </TabContainer>

                <Switch>
                    <Route path={`${path}/qps`}>
                        <TabletCharts alias={alias} clusterID={clusterID} />
                    </Route>

                    <Route path={`${path}/json`}>
                        <div>
                            <Code code={JSON.stringify(tablet, null, 2)} />

                            {process.env.REACT_APP_ENABLE_EXPERIMENTAL_TABLET_DEBUG_VARS && (
                                <Code code={JSON.stringify(debugVars, null, 2)} />
                            )}
                        </div>
                    </Route>
                    <Redirect from={path} to={`${path}/qps`} />
                </Switch>
            </ContentContainer>

            {/* TODO skeleton placeholder */}
            {!!tq.isLoading && <div className={style.placeholder}>Loading</div>}
        </div>
    );
};
