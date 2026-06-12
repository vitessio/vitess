/**
 * Copyright 2026 The Vitess Authors.
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
import js from '@eslint/js';
import { defineConfig } from 'eslint/config'
import tseslint from 'typescript-eslint';
import react from 'eslint-plugin-react';
import reactHooks from 'eslint-plugin-react-hooks';
import jsxA11y from 'eslint-plugin-jsx-a11y';
import globals from 'globals';

export default defineConfig(
    {
        ignores: ['build/**', 'node_modules/**', 'src/proto/**'],
    },
    js.configs.recommended,
    ...tseslint.configs.recommended,
    react.configs.flat.recommended,
    react.configs.flat['jsx-runtime'],
    jsxA11y.flatConfigs.recommended,
    {
        plugins: {
            'react-hooks': reactHooks,
        },
        rules: reactHooks.configs.recommended.rules,
    },
    {
        languageOptions: {
            globals: {
                ...globals.browser,
                ...globals.es2020,
            },
            parserOptions: {
                ecmaFeatures: {
                    jsx: true,
                },
            },
        },
        settings: {
            react: {
                version: 'detect',
            },
        },
        rules: {
            // Carried over from eslint-config-react-app
            '@typescript-eslint/no-unused-vars': ['error', { args: 'none', ignoreRestSiblings: true }],
            '@typescript-eslint/no-explicit-any': 'off',
            '@typescript-eslint/no-empty-object-type': 'off',
            '@typescript-eslint/no-unsafe-function-type': 'off',
            '@typescript-eslint/no-non-null-asserted-optional-chain': 'off',

            'no-restricted-globals': [
                'error',
                'addEventListener', 'blur', 'close', 'closed', 'confirm', 'defaultStatus',
                'defaultstatus', 'event', 'external', 'find', 'focus', 'frameElement', 'frames',
                'history', 'innerHeight', 'innerWidth', 'length', 'location', 'locationbar',
                'menubar', 'moveBy', 'moveTo', 'name', 'onblur', 'onerror', 'onfocus', 'onload',
                'onresize', 'onunload', 'open', 'opener', 'opera', 'outerHeight', 'outerWidth',
                'pageXOffset', 'pageYOffset', 'parent', 'print', 'removeEventListener', 'resizeBy',
                'resizeTo', 'screen', 'screenLeft', 'screenTop', 'screenX', 'screenY', 'scroll',
                'scrollbars', 'scrollBy', 'scrollTo', 'scrollX', 'scrollY', 'self', 'status',
                'statusbar', 'stop', 'toolbar', 'top',
            ],

            'prefer-const': 'error',
            'no-extra-boolean-cast': 'error',
            'no-var': 'error',
            'no-case-declarations': 'error',

            // React
            'react/prop-types': 'off', // TypeScript handles prop validation
            'react/display-name': 'off',
            'react/jsx-key': 'error',
            'react/jsx-no-target-blank': 'error',
            'react/no-unescaped-entities': 'off',

            // Accessibility: match eslint-config-react-app (warn, not error)
            'jsx-a11y/no-autofocus': 'error',
            'jsx-a11y/click-events-have-key-events': 'error',
            'jsx-a11y/no-static-element-interactions': 'error',

            // react-hooks plugin v7 added these; not in eslint-config-react-app
            'react-hooks/immutability': 'off',
            'react-hooks/set-state-in-effect': 'off',
        },
    },
);
