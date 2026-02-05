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

interface Props {
    className?: string;
    fqdn?: string | null | undefined;
}

export const ExternalTabletLink: React.FunctionComponent<Props> = ({ children, className, fqdn }) => {
    if (!fqdn) {
        return <span className={className}>{children}</span>;
    }

    return (
        <a className={className} href={`${fqdn}`} rel="noreferrer" target="_blank">
            {children}
        </a>
    );
};
