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
import cx from 'classnames';
import { LinkProps, NavLink } from 'react-router-dom';
import { Pip, PipState } from '../pips/Pip';

import style from './Tab.module.scss';

interface Props {
    activeClassName?: string;
    className?: string;
    count?: number | null | undefined;
    status?: PipState;
    text: string;
    to: LinkProps['to'];
}

export const Tab = ({ activeClassName, className, count, status, text, to }: Props) => {
    return (
        <NavLink
            activeClassName={cx(style.active, activeClassName)}
            className={cx(style.tab, className)}
            role="tab"
            to={to}
        >
            {!!status && <Pip className={style.pip} state={status} />}
            {text}
            {typeof count === 'number' && <span className={style.count}>{count}</span>}
        </NavLink>
    );
};
