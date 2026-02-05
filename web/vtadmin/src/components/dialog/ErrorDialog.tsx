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
import Dialog from './Dialog';
import { Icon, Icons } from '../Icon';

export interface ErrorDialogProps {
    errorTitle?: string;
    errorDescription: string;
    isOpen: boolean;
    onClose: () => void;
}

const ErrorDialog: React.FC<ErrorDialogProps> = ({ errorTitle, errorDescription, isOpen, onClose }) => {
    return (
        <Dialog isOpen={isOpen} onConfirm={onClose} onClose={onClose} hideCancel={true} confirmText="Close">
            <div className="w-full flex flex-col justify-center items-center">
                <span className="flex h-12 w-12 relative items-center justify-center">
                    <Icon className="fill-current text-red-500" icon={Icons.alertFail} />
                </span>
                <div className="text-lg mt-3 font-bold">{errorTitle || 'Error'}</div>
                <div className="text-sm">{errorDescription}</div>
            </div>
        </Dialog>
    );
};

export default ErrorDialog;
