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
import * as icons from '../icons';

interface Props {
    className?: string;
    icon: Icons;
    tabIndex?: number;
}

// All icons are from the VTAdmin Figma icon library:
// https://www.figma.com/file/By3SoETBRHpOirv3Ctfxdq/Designs
export const Icon = React.forwardRef<any, Props>(({ icon, ...props }, ref) => {
    const componentName = icon.charAt(0).toUpperCase() + icon.slice(1);

    const IconComponent = (icons as any)[componentName];
    if (!IconComponent) {
        console.warn(`Invalid icon: ${icon}`);
        return null;
    }

    return <IconComponent {...props} ref={ref} />;
});

export enum Icons {
    alertFail = 'alertFail',
    bug = 'bug',
    checkSuccess = 'checkSuccess',
    chevronDown = 'chevronDown',
    chevronUp = 'chevronUp',
    circleAdd = 'circleAdd',
    delete = 'delete',
    download = 'download',
    info = 'info',
    open = 'open',
    question = 'question',
    runQuery = 'runQuery',
    search = 'search',
}
