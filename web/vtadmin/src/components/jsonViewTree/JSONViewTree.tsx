/**
 * Copyright 2025 The Vitess Authors.
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

import React, { useState } from 'react';
import { JSONTree } from 'react-json-tree';

const vtAdminTheme = {
    scheme: 'vtadmin',
    author: 'custom',
    base00: '#ffffff',
    base01: '#f6f8fa',
    base02: '#e6e8eb',
    base03: '#8c8c8c',
    base04: '#3c3c3c',
    base05: '#2c2c2c',
    base06: '#0057b8',
    base07: '#000000',
    base08: '#00875a',
    base09: '#2c2c2c',
    base0A: '#e44d26',
    base0B: '#2c2c2c',
    base0C: '#1a73e8',
    base0D: '#3d5afe',
    base0E: '#3cba54',
    base0F: '#ff6f61',
};

interface JSONViewTreeProps {
    data: any;
}

const JSONViewTree: React.FC<JSONViewTreeProps> = ({ data }) => {
    const [expandAll, setExpandAll] = useState(false);
    const [treeKey, setTreeKey] = useState(0);

    const handleExpand = () => {
        setExpandAll(true);
        setTreeKey((prev) => prev + 1);
    };

    const handleCollapse = () => {
        setExpandAll(false);
        setTreeKey((prev) => prev + 1);
    };

    const getItemString = (type: string, data: any) => {
        if (Array.isArray(data)) {
            return `${type}[${data.length}]`;
        }
        return type;
    };

    if (!data) return null;
    return (
        <div className="p-1">
            <div className="flex mt-2 gap-2">
                <button onClick={handleExpand} className="btn btn-secondary btn-sm">
                    Expand All
                </button>
                <button onClick={handleCollapse} className="btn btn-danger bg-transparent text-red-500 btn-sm">
                    Collapse All
                </button>
            </div>
            <JSONTree
                key={treeKey}
                data={data}
                theme={{
                    extend: vtAdminTheme,
                    nestedNodeItemString: {
                        color: vtAdminTheme.base0C,
                    },
                }}
                invertTheme={false}
                hideRoot={true}
                getItemString={getItemString}
                shouldExpandNodeInitially={() => expandAll}
            />
        </div>
    );
};

export default JSONViewTree;
