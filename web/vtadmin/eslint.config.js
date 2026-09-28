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
import eslintReact from '@eslint-react/eslint-plugin';
import reactHooks from 'eslint-plugin-react-hooks';
import jsxA11yX from 'eslint-plugin-jsx-a11y-x';
import globals from 'globals';

export default defineConfig(
    {
        ignores: ['build/**', 'node_modules/**', 'src/proto/**'],
    },
    js.configs.recommended,
    ...tseslint.configs.recommended,
    jsxA11yX.configs.recommended,
    {
        plugins: {
            '@eslint-react': eslintReact,
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
            'react-x': {
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

            // React: the rules from eslint-plugin-react's recommended preset that still
            // apply under TypeScript, the automatic JSX runtime and React 19, mapped onto
            // their @eslint-react equivalents. No preset, so this list is the full set.
            '@eslint-react/no-missing-key': 'error',
            '@eslint-react/dom-no-unsafe-target-blank': 'error',
            '@eslint-react/jsx-no-comment-textnodes': 'error',
            '@eslint-react/jsx-no-children-prop': 'error',
            '@eslint-react/dom-no-dangerously-set-innerhtml-with-children': 'error',
            '@eslint-react/no-direct-mutation-state': 'error',
            '@eslint-react/dom-no-find-dom-node': 'error',
            '@eslint-react/dom-no-render-return-value': 'error',
            '@eslint-react/dom-no-unknown-property': 'error',
            '@eslint-react/no-unsafe-component-will-mount': 'error',
            '@eslint-react/no-unsafe-component-will-receive-props': 'error',
            '@eslint-react/no-unsafe-component-will-update': 'error',

            // Accessibility: match eslint-config-react-app (warn, not error)
            'jsx-a11y-x/no-autofocus': 'error',
            'jsx-a11y-x/click-events-have-key-events': 'error',
            'jsx-a11y-x/no-static-element-interactions': 'error',

            // react-hooks plugin v7 added these; not in eslint-config-react-app
            'react-hooks/immutability': 'off',
            'react-hooks/set-state-in-effect': 'off',
        },
    },
);
