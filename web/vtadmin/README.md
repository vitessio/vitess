# VTAdmin

## Running vtadmin-web locally

In this section, we'll get vtadmin-web, vtadmin-api, and Vitess all running locally. This process is still somewhat... cumbersome, apologies. 😰 

1. Run Vitess locally [with Docker](https://vitess.io/docs/get-started/local-docker/) (or another way, if you prefer):

	```bash
	make docker_local
	./docker/local/run.sh
	```

1. Create an empty vtgate credentials file to avoid the gRPC dialer bug mentioned in https://github.com/vitessio/vitess/pull/7187. Location and filename don't matter since you'll be passing this in as a flag; I put mine at ` /Users/sarabee/vtadmin-creds.json`:

	```json
	{
		"Username": "",
		"Password": ""
	}
	```

1. Create a cluster configuration file for the local Vitess you started up in step 1. Again, filename and location don't matter since we'll be passing in the path as a flag; I put mine at `/Users/sarabee/vtadmin-cluster1.json`. Here it is with default values for the [local Vitess/Docker example](https://vitess.io/docs/get-started/local-docker/) we're following: 

	```json
	{
		"vtgates": [
			{
				"host": {
					"hostname": "127.0.0.1:15991"
				}
			}
		],
		"vtctlds": [
			{
				"host": {
					"hostname": "127.0.0.1:15999"
				}
			}
		]
	}
	```

1. Start up vtadmin-api but **make sure to update the filepaths** for the vtgate creds file and static service discovery file you created above!

	```bash
	make build

	./bin/vtadmin \
		--addr ":14200" \
		--cluster-defaults "vtctld-credentials-path-tmpl=/Users/sarabee/vtadmin-creds.json,vtsql-credentials-path-tmpl=/Users/sarabee/vtadmin-creds.json" \
		--cluster "name=cluster1,id=id1,discovery=staticFile,discovery-staticFile-path=/Users/sarabee/vtadmin-cluster1.json" \
		--http-origin=http://localhost:3000
	```

1. Finally! Start up vtadmin-web on [http://localhost:3000](http://localhost:3000), pointed at the vtadmin-api server you started in the last step. 

	```bash
	cd web/vtadmin
	npm install
	REACT_APP_VTADMIN_API_ADDRESS="http://127.0.0.1:14200" npm start
	```

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

## Environment Variables

Under the hood, we use create-react-app's environment variable set-up which is very well documented: https://create-react-app.dev/docs/adding-custom-environment-variables. 

All of our environment variables are enumerated and commented in [react-app-env.d.ts](./src/react-app-env.d.ts). This also gives us type hinting on `process.env`, for editors that support it. 

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
