# VTAdmin

## Prerequisites

- [node](https://nodejs.org) >= 16.13.0 LTS
- npm >= 8.1.0 (comes with node)

## Available scripts

Scripts for common and not-so-common tasks. These are always run from the `vitess/web/vtadmin` directory (although some of them have counterparts in `vitess/Makefile`):

| Command | Description |
|---|---|
| `npm start` | Start vtadmin-web on [http://localhost:3000](http://localhost:3000) with the development server. Changes you make will be automatically picked up in the browser, no need to refresh. |
| `npm run test` | Launches the test runner in the interactive watch mode. See the create-react-app documentation about [running tests](https://facebook.github.io/create-react-app/docs/running-tests) for more information. |
| `npm run lint` | Run all of the linters and formatters. The `package.json` file defines a bunch more scripts to run individual linters, if you prefer, like `npm run lint:eslint`. |
| `npm run lint:fix` | Run all of the linters and fix errors (where possible) in place. Note that this will overwrite your files so you may want to consider committing your work beforehand! |
| `npm run build` | Generates a build of vtadmin-web for production and outputs the files to the `vitess/web/vtadmin/build` folder. In most cases, you won't need to run this locally, but it _can_ be useful for debugging production-specific issues. See the create-react-app documentation about [deployment](https://facebook.github.io/create-react-app/docs/deployment) for more information. |
| `npm run analyze` | Analyze and debug JavaScript build size using [source-map-explorer](https://create-react-app.dev/docs/analyzing-the-bundle-size/). In most cases, you'll first want to run `npm run build` to update the `build/` directory. |

## Toolchain

- [React](https://reactjs.org/)
- [create-react-app](https://create-react-app.dev/) (generated with v.4.0.1)
- [TypeScript](http://typescriptlang.org/)
- [protobufjs](https://github.com/protobufjs)

## Environment Variables

Under the hood, we use create-react-app's environment variable set-up which is very well documented: https://create-react-app.dev/docs/adding-custom-environment-variables. 

All of our environment variables are enumerated and commented in [react-app-env.d.ts](./src/react-app-env.d.ts). This also gives us type hinting on `process.env`, for editors that support it. 

## Linters and Formatters

We use three libraries for consistent formatting and linting across the front-end codebase. (Not quite as streamlined as Go, alas!) These can be run individually, as noted below, or all in sequence with `npm run lint`.

| Library | Commands | What it's for |
|---------|----------|---------------|
| [eslint](https://eslint.org/) | `npm run lint:eslint`<br/><br/>`npm run lint:eslint:fix` | ESLint identifies potential bugs and other issues. vtadmin-web uses the default ESLint configuration [built in to create-react-app](https://create-react-app.dev/docs/setting-up-your-editor/#extending-or-replacing-the-default-eslint-config). |
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

For more, check out ["Setting Up Your Editor"](https://create-react-app.dev/docs/setting-up-your-editor/) in the create-react-app docs.
