# VTAdmin

## Prerequisites

- [node](https://nodejs.org) >= 18.16.0 LTS
- npm >= 9.7.1 (comes with node)

## Available scripts

Scripts for common and not-so-common tasks. These are always run from the `vitess/web/vtadmin` directory (although some of them have counterparts in `vitess/Makefile`):

| Command | Description |
|---|---|
| `npm local` | Start vtadmin-web in development mode on [http://localhost:3000](http://localhost:3000), pointed at a vtadmin-api server running on [http://localhost:14200](http://localhost:14200). This is most useful when running against a [local Vitess cluster](https://vitess.io/docs/get-started/local/). |
| `npm start` | Start vtadmin-web in development mode on [http://localhost:3000](http://localhost:3000). Additional environment variables can be specified on the command line or in a .env file; see [Environment Variables](#environment-variables). |
| `npm run test` | Launches the test runner in the interactive watch mode. See the vitest documentation about [running tests](https://vitest.dev/guide/cli.html#vitest-run) for more information. |
| `npm run lint` | Run all of the linters and formatters. The `package.json` file defines a bunch more scripts to run individual linters, if you prefer, like `npm run lint:eslint`. |
| `npm run lint:fix` | Run all of the linters and fix errors (where possible) in place. Note that this will overwrite your files so you may want to consider committing your work beforehand! |
| `npm run build` | Generates a build of vtadmin-web for production and outputs the files to the `vitess/web/vtadmin/build` folder. In most cases, you won't need to run this locally, but it _can_ be useful for debugging production-specific issues. See the vite documentation about [testing the build locally](https://vitejs.dev/guide/static-deploy.html#testing-the-app-locally) for more information. |

## Toolchain

- [React](https://reactjs.org/)
- [vite](https://vitejs.dev/)
- [vitest](https://vitest.dev/)
- [TypeScript](http://typescriptlang.org/)
- [protobufjs](https://github.com/protobufjs)
- [tailwindcss](https://tailwindcss.com/)

## Environment Variables

Under the hood, we use vite's environment variable set-up which is very well documented: https://vitejs.dev/guide/env-and-mode.html#env-variables-and-modes. 

All of our environment variables are enumerated and commented in [vite-env.d.ts](./vite-env.d.ts). This also gives us type hinting on `import.meta.env`, for editors that support it. 

## Linters and Formatters

We use three libraries for consistent formatting and linting across the front-end codebase. (Not quite as streamlined as Go, alas!) These can be run individually, as noted below, or all in sequence with `npm run lint`.

| Library | Commands | What it's for |
|---------|----------|---------------|
| [eslint](https://eslint.org/) | `npm run lint:eslint`<br/><br/>`npm run lint:eslint:fix` | ESLint identifies potential bugs and other issues. vtadmin-web uses the default ESLint configuration [built into vite-plugin-eslint](https://www.npmjs.com/package/vite-plugin-eslint). |
| [prettier](https://prettier.io/) | `npm run lint:prettier`<br/><br/>`npm run lint:prettier:fix` | Prettier is an "opinionated code formatter" run against our JavaScript, TypeScript, and (S)CSS code to ensure a consistent style. Prettier is not a linter, and so it complements (rather than replaces) eslint/stylelint. | 
| [stylelint](https://stylelint.io/) | `npm run lint:stylelint`<br/><br/>`npm run lint:stylelint:fix` | Stylelint is a linter for CSS/SCSS. Whereas prettier's CSS/SCSS support is largely focused on formatting (newlines, spaces vs. tabs), stylelint is able to flag possible errors, limit language features, and surface stylistic issues that prettier isn't intended to catch. |

## Configuring your editor

### VS Code

To set up automatic formatting on save:

1. Install the [Prettier VS Code plugin](https://marketplace.visualstudio.com/items?itemName=esbenp.prettier-vscode).
2. Add the following to your VS Code workspace:

```js
{
	// ... other workspace config ...

	"settings": {
		// ... other settings ..

		"prettier.useEditorConfig": false,

		// You may have to adjust this depending on which folder is the root of your workspace.
		// By default, this configuration assumes that you have your workspace settings 
		// at `vitess/.vscode/settings.json`. 
		"prettier.configPath": "web/vtadmin/.prettiercc",
		
		"[typescriptreact]": {
			"editor.defaultFormatter": "esbenp.prettier-vscode",
			"editor.formatOnSave": true,
		},
		
		"[typescript]": {
			"editor.defaultFormatter": "esbenp.prettier-vscode",
			"editor.formatOnSave": true,
		},
		
		"[javascript]": {
			"editor.defaultFormatter": "esbenp.prettier-vscode",
			"editor.formatOnSave": true,
		},

		"[css]": {
			"editor.defaultFormatter": "esbenp.prettier-vscode",
			"editor.formatOnSave": true,
			"editor.codeActionsOnSave": {
				"source.fixAll.stylelint": true
			}
		},

		"[scss]": {
			"editor.defaultFormatter": "esbenp.prettier-vscode",
			"editor.formatOnSave": true,
			"editor.codeActionsOnSave": {
				"source.fixAll.stylelint": true
			}
		}
	}
}
```

