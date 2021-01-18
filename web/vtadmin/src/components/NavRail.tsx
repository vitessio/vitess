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
import logo from '../img/vitess-icon-color.svg';
import { useTablets } from '../hooks/api';

export const NavRail = () => {
    const { data: tabletData } = useTablets();

    return (
        <div className={style.container}>
            <Link className={style.logoContainer} to="/">
                <img alt="Vitess logo" className={style.logo} src={logo} height={40}></img>
            </Link>

            <div className={style.navLinks}>
                <ul className={style.navList}>
                    <li>
                        <NavRailLink text="Dashboard" to="/dashboard" count={0} />
                    </li>
                    <li>
                        <NavRailLink text="Workflows" to="/workflows" count={0} />
                    </li>
                </ul>

                <ul className={style.navList}>
                    <li>
                        <NavRailLink text="Clusters" to="/clusters" count={0} />
                    </li>
                    <li>
                        <NavRailLink text="Gates" to="/gates" count={0} />
                    </li>
                    <li>
                        <NavRailLink text="Keyspaces" to="/keyspaces" count={0} />
                    </li>
                    <li>
                        <NavRailLink text="Schemas" to="/schemas" count={0} />
                    </li>
                    <li>
                        <NavRailLink text="Tablets" to="/tablets" count={(tabletData || []).length} />
                    </li>
                </ul>

                <ul className={style.navList}>
                    <li>
                        <NavRailLink text="VTExplain" to="/vtexplain" />
                    </li>
                    <li>
                        <NavRailLink text="Settings" to="/settings" />
                    </li>
                </ul>
            </div>

            <div className={style.footerContainer}>
                <ul className={style.navList}>
                    <li>
                        <NavRailLink text="Debug" to="/debug" />
                    </li>
                    <li>
                        <NavRailLink text="Shortcuts" to="/shortcuts" />
                    </li>
                </ul>
            </div>
        </div>
    );
};

const NavRailLink = ({ count, text, to }: { count?: number; text: string; to: string }) => {
    return (
        <NavLink activeClassName={style.navLinkActive} className={style.navLink} to={to}>
            <span>{text}</span>
            {typeof count === 'number' && <div className={style.badge}>{count}</div>}
        </NavLink>
    );
};
