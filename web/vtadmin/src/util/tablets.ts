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
import { invertBy, padStart } from 'lodash-es';
import { topodata, vtadmin as pb } from '../proto/vtadmin';

/**
 * TABLET_TYPES maps numeric tablet types back to human readable strings.
 * Note that topodata.TabletType allows duplicate values: specifically,
 * both RDONLY (new name) and BATCH (old name) share the same numeric value.
 * So, we make the assumption that if there are duplicate keys, we will
 * always take the first value.
 */
export const TABLET_TYPES = Object.entries(invertBy(topodata.TabletType)).reduce((acc, [k, vs]) => {
    acc[k] = vs[0];
    return acc;
}, {} as { [k: string]: string });

/**
 * formatAlias formats a tablet.alias object as a single string, The Vitess Way™.
 */
export const formatAlias = <A extends topodata.ITabletAlias>(alias: A | null | undefined) =>
    alias?.uid ? `${alias.cell}-${alias.uid}` : null;

/**
 * formatAlias formats a tablet.alias object as a single string,
 * with the uid left-padded with zeroes.
 *
 * This function can be removed once https://github.com/vitessio/vitess/issues/8751 is complete.
 */
export const formatPaddedAlias = <A extends topodata.ITabletAlias>(alias: A | null | undefined) =>
    alias?.cell && alias?.uid ? `${alias.cell}-${padStart(alias.uid.toString(), 10, '0')}` : null;

export const formatType = (t: pb.Tablet) => t.tablet?.type && TABLET_TYPES[t.tablet?.type];

export const formatDisplayType = (t: pb.Tablet) => {
    const tt = formatType(t);
    return tt === 'MASTER' ? 'PRIMARY' : tt;
};

export const SERVING_STATES = Object.keys(pb.Tablet.ServingState);

export const formatState = (t: pb.Tablet) => t.state && SERVING_STATES[t.state];

export const isPrimary = (t: pb.Tablet | undefined) =>
    Boolean(t?.tablet?.type) && t?.tablet?.type === topodata.TabletType.PRIMARY;
