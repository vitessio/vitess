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
import React from 'react';
import { Popover } from 'react-tiny-popover';

import style from './Tooltip.module.scss';

export interface TooltipProps {
    children: React.ReactElement;
    text: string | JSX.Element;
}

/**
 * Tooltips display informative text when users hover over or focus on
 * the child element.
 *
 * Tooltips are used:
 *  - To show secondary information
 *  - To show keyboard shortcuts
 *  - To provide context for visual controls like icon buttons
 *
 * Tooltips are (generally) NOT used:
 *  - To show secondary information that's longer than a sentence or two
 *  - To render interactive elements, like links
 *  - To render imagery, like images or SVGs
 *
 * See the Material documentation for helpful usage guidelines:
 * https://material.io/components/tooltips
 *
 * Note that custom components (like <Button> and <Icon> -- namely, any
 * component that is not a native component like <div> and <span>)
 * must support ref forwarding for the tooltip to position itself correctly.
 * See https://reactjs.org/docs/forwarding-refs.html
 */
export const Tooltip = ({ children, text }: TooltipProps) => {
    const [isOpen, setIsOpen] = React.useState<boolean>(false);

    const hideTooltip = () => setIsOpen(false);
    const showTooltip = () => setIsOpen(true);

    const content = <div className={style.tooltip}>{text}</div>;

    // React.cloneElement is used to attach event listeners to the child component in order
    // to show/hide the tooltip on hover and focus, without resorting to wrapping
    // the child component in a surrounding element like a <span>. For another
    // example of this, see https://github.com/alexkatz/react-tiny-popover/pull/10.
    //
    // Note that cloneElement will (by default) preserve the ref forwarding between
    // the Popover component and the child element, so we happily don't need to do
    // anything special there.
    const cloneChildren = React.cloneElement(children, {
        onBlur: hideTooltip,
        onFocus: showTooltip,
        onMouseEnter: showTooltip,
        onMouseLeave: hideTooltip,
        // For accessibility, tooltips are shown on focus (as well as on hover),
        // which means the child element needs to have a tabIndex.
        tabIndex: 0,
    });

    return (
        <Popover content={content} isOpen={isOpen}>
            {cloneChildren}
        </Popover>
    );
};
