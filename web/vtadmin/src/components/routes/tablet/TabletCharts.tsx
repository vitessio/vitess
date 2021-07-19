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

import style from './TabletCharts.module.scss';
import { TabletQPSChart } from '../../charts/TabletQPSChart';
import { TabletVReplicationQPSChart } from '../../charts/TabletVReplicationQPSChart';

interface Props {
    alias: string;
    clusterID: string;
}

export const TabletCharts = ({ alias, clusterID }: Props) => {
    return (
        <div className={style.container}>
            <div>
                <h3>QPS</h3>
                <div className={style.chartContainer}>
                    <TabletQPSChart alias={alias} clusterID={clusterID} />
                </div>
            </div>

            <div>
                <h3>VReplication QPS</h3>
                <p>VReplication operations aggregated across all streams.</p>
                <div className={style.chartContainer}>
                    <TabletVReplicationQPSChart alias={alias} clusterID={clusterID} />
                </div>
            </div>
        </div>
    );
};
