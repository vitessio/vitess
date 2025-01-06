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

import style from './Label.module.scss';
import { Tooltip } from '../tooltip/Tooltip';
import { Icon, Icons } from '../Icon';

type NativeLabelProps = React.DetailedHTMLProps<React.LabelHTMLAttributes<HTMLLabelElement>, HTMLLabelElement>;

interface Props extends NativeLabelProps {
    label: string;
    required?: boolean;
    helpText?: string | JSX.Element;
}

export const Label: React.FunctionComponent<Props> = ({ children, required, label, helpText, ...props }) => {
    return (
        <label {...props}>
            <span className={style.label}>{label}</span>
            {required && <span className="text-danger"> *</span>}
            {helpText && (
                <Tooltip text={helpText}>
                    <span>
                        <Icon className={style.icon} icon={Icons.info} />
                    </span>
                </Tooltip>
            )}
            {children}
        </label>
    );
};
