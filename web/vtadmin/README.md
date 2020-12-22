# VTAdmin

## Getting started

1. Install node v.15.14.3. (Using a Node version manager like [nvm](https://github.com/nvm-sh/nvm) is highly recommended.)
1. From the `vitess/web/vtadmin/` directory, install dependencies with `npm install`.
1. Run `npm start` to start vtadmin-web on [http://localhost:3000](http://localhost:3000) 

Have fun! 🎉

# Developer guide

This section contains notes for those that want to build and run vtadmin-web locally.

## Available scripts

Scripts for common and not-so-common tasks. These are always run from the `vitess/web/vtadmin` directory (although some of them have counterparts in `vitess/Makefile`):

| Command | Description |
|---|---|
| `npm start` | Start vtadmin-web on [http://localhost:3000](http://localhost:3000) with the development server. Changes you make will be automatically picked up in the browser, no need to refresh. |
| `npm run test` | Launches the test runner in the interactive watch mode. See the create-react-app documentation about [running tests](https://facebook.github.io/create-react-app/docs/running-tests) for more information. |
| `npm run lint` | Run all of the linters and formatters. The `package.json` file defines a bunch more scripts to run individual linters, if you prefer, like `npm run lint:eslint`. |
| `npm run lint:fix` | Run all of the linters and fix errors (where possible) in place. Note that this will overwrite your files so you may want to consider committing your work beforehand! |
| `npm run build` | Generates a build of vtadmin-web for production and outputs the files to the `vitess/web/vtadmin/build` folder. In most cases, you won't need to run this locally, but it _can_ be useful for debugging production-specific issues. See the create-react-app documentation about [deployment](https://facebook.github.io/create-react-app/docs/deployment) for more information. |

## Toolchain

- [React](https://reactjs.org/)
- [create-react-app](https://create-react-app.dev/) (generated with v.4.0.1)
- [TypeScript](http://typescriptlang.org/)
