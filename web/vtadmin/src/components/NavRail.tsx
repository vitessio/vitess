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
import * as React from 'react';
import { Link, NavLink } from 'react-router-dom';

import style from './NavRail.module.scss';
import { useClusters, useGates, useKeyspaces, useSchemas, useTablets, useVtctlds, useWorkflows } from '../hooks/api';
import { Icon, Icons } from './Icon';
import { getTableDefinitions } from '../util/tableDefinitions';
import VitessLogo from './VitessLogo';

export const NavRail = () => {
    const { data: clusters = [] } = useClusters();
    const { data: keyspaces = [] } = useKeyspaces();
    const { data: gates = [] } = useGates();
    const { data: schemas = [] } = useSchemas();
    const { data: tablets = [] } = useTablets();
    const { data: vtctlds = [] } = useVtctlds();
    const { data: workflows = [] } = useWorkflows();

    const tds = React.useMemo(() => getTableDefinitions(schemas), [schemas]);

    return (
        <div className={style.container}>
            <Link className={style.logoContainer} to="/">
                <VitessLogo className="h-[40px]" />
            </Link>

            <div className={style.navLinks}>
                <ul className={style.navList}>
                    <li>
                        <NavRailLink hotkey="C" text="Clusters" to="/clusters" count={clusters.length} />
                    </li>
                    <li>
                        <NavRailLink hotkey="G" text="Gates" to="/gates" count={gates.length} />
                    </li>
                    <li>
                        <NavRailLink hotkey="K" text="Keyspaces" to="/keyspaces" count={keyspaces.length} />
                    </li>
                    <li>
                        <NavRailLink hotkey="S" text="Schemas" to="/schemas" count={tds.length} />
                    </li>
                    <li>
                        <NavRailLink hotkey="T" text="Tablets" to="/tablets" count={tablets.length} />
                    </li>
                    <li>
                        <NavRailLink hotkey="V" text="vtctlds" to="/vtctlds" count={vtctlds.length} />
                    </li>
                    <li>
                        <NavRailLink hotkey="W" text="Workflows" to="/workflows" count={workflows.length} />
                    </li>
                </ul>

                <ul className={style.navList}>
                    <li>
                        <NavRailLink icon={Icons.download} text="Backups" to="/backups" />
                    </li>
                    <li>
                        <NavRailLink icon={Icons.runQuery} text="VTExplain" to="/vtexplain" />
                    </li>
                    <li>
                        <NavRailLink icon={Icons.topology} text="Topology" to="/topology" />
                    </li>
                </ul>
            </div>
        </div>
    );
};

interface LinkProps {
    count?: number;
    hotkey?: string;
    icon?: Icons;
    text: string;
    to: string;
}

const NavRailLink = ({ count, hotkey, icon, text, to }: LinkProps) => {
    return (
        <NavLink activeClassName={style.navLinkActive} className={style.navLink} to={to}>
            {icon && <Icon className={style.icon} icon={icon} />}
            {hotkey && !icon && <div className={style.hotkey} data-hotkey={hotkey.toUpperCase()} />}
            <div className="ml-4">{text}</div>
            {typeof count === 'number' && <div className={style.badge}>{count}</div>}
        </NavLink>
    );
};
