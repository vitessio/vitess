# VTAdmin

## Getting started

1. Install node v.14.15.3. (Using a Node version manager like [nvm](https://github.com/nvm-sh/nvm) is highly recommended.)
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
- [protobufjs](https://github.com/protobufjs)

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
			"editor.codeActionsOnSave": {
				"source.fixAll.stylelint": true
			}
		},

		"[scss]": {
			"editor.codeActionsOnSave": {
				"source.fixAll.stylelint": true
			}
		}
	}
}
```

For more, check out ["Setting Up Your Editor"](https://create-react-app.dev/docs/setting-up-your-editor/) in the create-react-app docs.
