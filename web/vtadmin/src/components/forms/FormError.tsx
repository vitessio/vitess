/**
 * Copyright 2022 The Vitess Authors.
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

import { Icon, Icons } from '../Icon';

interface Props {
    title: string;
    error: Error | null | undefined;
}

export const FormError: React.FC<Props> = ({ error, title }) => {
    return (
        <div className="bg-red-50 p-8 rounded-md my-12" role="alert">
            <div className="text-md font-bold mb-4">
                <Icon className="inline fill-red-700 h-[20px] align-text-top" icon={Icons.alertFail} /> {title}
            </div>
            <div className="font-mono">{error?.message}</div>
        </div>
    );
};
