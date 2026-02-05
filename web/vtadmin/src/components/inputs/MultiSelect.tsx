/**
 * Copyright 2024 The Vitess Authors.
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
import React from 'react';
import { Icon, Icons } from '../Icon';
import cx from 'classnames';
import style from './MultiSelect.module.scss';
import { Listbox } from '@headlessui/react';
import { Label } from './Label';

interface Props<T> {
    className?: string;
    inputClassName?: string;
    disabled?: boolean;
    items: T[];
    itemToString?: (item: T) => string;
    label: string;
    helpText?: string | JSX.Element;
    onChange: (selectedItems: T[]) => void;
    placeholder: string;
    renderItem?: (item: T) => JSX.Element | string;
    selectedItems: T[];
    description?: string;
    required?: boolean;
}

export const MultiSelect = <T,>({
    className,
    inputClassName,
    disabled,
    items,
    itemToString = (item) => String(item),
    label,
    helpText,
    placeholder,
    renderItem,
    selectedItems,
    onChange,
    description,
    required,
}: Props<T>) => {
    const _renderItem = (item: T) => {
        if (typeof renderItem === 'function') {
            return renderItem(item);
        }
        return itemToString(item);
    };

    const selectedItemsText = `Selected (${selectedItems.length}): ${selectedItems
        .map((item) => _renderItem(item))
        .join(', ')}`;

    return (
        <div className={cx(style.container, className)}>
            <Label label={label} required={required} helpText={helpText} />
            {description && <div className="mt-[-4px] mb-4">{description}</div>}

            <Listbox value={selectedItems} onChange={onChange} disabled={disabled} multiple>
                {({ open }) => (
                    <>
                        <Listbox.Button
                            className={cx(style.toggle, inputClassName, {
                                [style.open]: open,
                            })}
                        >
                            {selectedItems.length > 0 ? selectedItemsText : placeholder}
                            <Icon className={style.chevron} icon={open ? Icons.chevronUp : Icons.chevronDown} />
                        </Listbox.Button>
                        <Listbox.Options className={cx(style.dropdown, { [style.hidden]: !open })}>
                            {items.map((item, index) => (
                                <Listbox.Option
                                    key={index}
                                    value={item}
                                    className={({ active, selected }) =>
                                        cx(style.listItem, {
                                            [style.active]: active,
                                            [style.selected]: selected,
                                        })
                                    }
                                >
                                    <div>{_renderItem(item)}</div>
                                </Listbox.Option>
                            ))}
                        </Listbox.Options>
                    </>
                )}
            </Listbox>
        </div>
    );
};
