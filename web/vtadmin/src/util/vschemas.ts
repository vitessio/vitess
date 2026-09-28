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
import { vtadmin as pb } from '../proto/vtadmin';

/**
 * getVindexesForTable returns Vindexes + Vindex metadata for the given table.
 */
export const getVindexesForTable = (vschema: pb.VSchema, tableName: string) => {
    const table = (vschema.v_schema?.tables || {})[tableName];
    if (!table) return [];

    return (table.column_vindexes || []).map((cv) => ({
        ...cv,
        meta: cv.name ? (vschema.v_schema?.vindexes || {})[cv.name] : null,
    }));
};
