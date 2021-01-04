/**
 * Copyright 2020 The Vitess Authors.
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
import { vtadmin as pb, topodata } from '../proto/vtadmin';

interface Props {
    tablets: pb.Tablet[];
}

const SERVING_STATES = Object.keys(pb.Tablet.ServingState);
const TABLET_TYPES = Object.keys(topodata.TabletType);

export const TabletList = ({ tablets }: Props) => {
    return (
        <table>
            <thead>
                <tr>
                    <th>Cluster</th>
                    <th>Hostname</th>
                    <th>Type</th>
                    <th>State</th>
                </tr>
            </thead>
            <tbody>
                {tablets.map((t, i) => (
                    <tr key={i}>
                        <td>{t.cluster?.name}</td>
                        <td>
                            <code>{t.tablet?.hostname}</code>
                        </td>
                        <td>{t.tablet?.type && TABLET_TYPES[t.tablet?.type]}</td>
                        <td>{SERVING_STATES[t.state]}</td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
};
