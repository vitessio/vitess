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
import { RadioGroup } from '@headlessui/react';

import style from './RadioCard.module.scss';

interface RadioCardProps {
    description: string;
    label: string;
    value: string;
}

export const RadioCard: React.FC<RadioCardProps> = ({ description, label, value }) => {
    return (
        <RadioGroup.Option className="outline-none" value={value}>
            {({ active, checked }) => (
                <div className={cx(style.card, checked && style.checked, active && style.active)}>
                    <div className={style.circle} />
                    <div>
                        <RadioGroup.Label className="font-semibold">{label}</RadioGroup.Label>
                        <RadioGroup.Description className="mb-0 mt-2 text-sm">{description}</RadioGroup.Description>
                    </div>
                </div>
            )}
        </RadioGroup.Option>
    );
};
