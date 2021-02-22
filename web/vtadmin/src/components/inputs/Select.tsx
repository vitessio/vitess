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
import { useSelect, UseSelectStateChange } from 'downshift';
import * as React from 'react';

import { Label } from './Label';
import style from './Select.module.scss';
import { Icon, Icons } from '../Icon';

interface Props<T> {
    disabled?: boolean;
    items: T[];
    itemToString?: (item: T | null) => string;
    label: string;
    onChange: (selectedItem: T | null | undefined) => void;
    placeholder: string;
    emptyPlaceholder?: string | (() => JSX.Element | string);
    renderItem?: (item: T) => JSX.Element | string;
    selectedItem: T | null;
    size?: 'large';
}

/**
 * Select performs exactly the same as the native HTML <select> in terms
 * of accessibility and functionality... but it looks much prettier,
 * and allows for fine-grained rendering control. :)
 */
export const Select = <T,>({
    disabled,
    itemToString,
    items,
    label,
    onChange,
    placeholder,
    emptyPlaceholder,
    renderItem,
    selectedItem,
    size,
}: Props<T>) => {
    const _itemToString = React.useCallback(
        (item: T | null): string => {
            if (typeof itemToString === 'function') return itemToString(item);
            return item ? String(item) : '';
        },
        [itemToString]
    );

    const onSelectedItemChange = React.useCallback(
        (changes: UseSelectStateChange<T>) => {
            onChange(changes.selectedItem);
        },
        [onChange]
    );

    const {
        getItemProps,
        getLabelProps,
        getMenuProps,
        getToggleButtonProps,
        highlightedIndex,
        isOpen,
        selectItem,
    } = useSelect({
        itemToString: _itemToString,
        items,
        onSelectedItemChange,
        selectedItem,
    });

    const containerClass = cx(style.container, {
        [style.large]: size === 'large',
        [style.open]: isOpen,
        [style.placeholder]: !selectedItem,
    });

    const _renderItem = React.useCallback(
        (item: T): string | JSX.Element | null => {
            if (typeof item === 'string') {
                return item;
            }

            if (typeof renderItem === 'function') {
                return renderItem(item);
            }

            return null;
        },
        [renderItem]
    );

    let content = null;
    if (items.length) {
        content = (
            <ul {...getMenuProps()} className={style.menu}>
                {items.map((item, index) => {
                    const itemClass = cx({ [style.active]: highlightedIndex === index });
                    return (
                        <li key={index} className={itemClass} {...getItemProps({ item, index })}>
                            {_renderItem(item)}
                        </li>
                    );
                })}
            </ul>
        );
    } else {
        let emptyContent = typeof emptyPlaceholder === 'function' ? emptyPlaceholder() : emptyPlaceholder;
        if (typeof emptyContent === 'string' || !emptyContent) {
            emptyContent = <div className={style.emptyPlaceholder}>{emptyContent || 'No items'}</div>;
        }
        content = (
            <div className={style.emptyContainer} {...getMenuProps()}>
                {emptyContent}
            </div>
        );
    }

    return (
        <div className={containerClass}>
            <Label {...getLabelProps()} label={label} />
            <button type="button" {...getToggleButtonProps()} className={style.toggle} disabled={disabled}>
                {selectedItem ? _renderItem(selectedItem) : placeholder}
                <Icon className={style.chevron} icon={isOpen ? Icons.chevronUp : Icons.chevronDown} />
            </button>
            <div className={style.dropdown} hidden={!isOpen}>
                {content}
                {selectedItem && (
                    <button className={style.clear} onClick={() => selectItem(null as any)} type="button">
                        Clear selection
                    </button>
                )}
            </div>
        </div>
    );
};
