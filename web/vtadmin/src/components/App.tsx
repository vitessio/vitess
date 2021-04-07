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
import * as React from 'react';

import style from './App.module.scss';
import logo from '../img/vitess-icon-color.svg';
import { TabletList } from './TabletList';
import { useTablets } from '../hooks/api';

export const App = () => {
    const { data, error, isError, isSuccess } = useTablets();

    // Placeholder UI :D
    let content = <div>Loading...</div>;
    if (isError) {
        content = (
            <div>
                {error?.name}: {error?.message}
            </div>
        );
    } else if (isSuccess) {
        content = <TabletList tablets={data || []} />;
    }

    return (
        <div className={style.container}>
            <img src={logo} alt="logo" height={40} />
            <h1>VTAdmin</h1>

            {content}
        </div>
    );
};
