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

import style from './Code.module.scss';

interface Props {
    code?: string | null | undefined;
}

export const Code = ({ code }: Props) => {
    if (typeof code !== 'string') return null;

    const codeLines = code.split('\n');
    return (
        <table className={style.table}>
            <tbody>
                {codeLines.map((line, idx) => {
                    return (
                        <tr key={idx}>
                            <td id={`L${idx}`} className={style.lineNumber} data-line-number={idx} />
                            <td className={style.code}>
                                <code>{line}</code>
                            </td>
                        </tr>
                    );
                })}
            </tbody>
        </table>
    );
};
