# Vitess Control Panel

This project was generated with [angular-cli](https://github.com/angular/angular-cli) version 1.0.0-beta.11-webpack.2.

## Local development

**⚠️ Warning! This project relies on very out-of-date node dependencies, many with significant security vulnerabilities. Install at your own risk.** 

### Prerequisites

You will need (very old!) installations of npm and node to run the front-end dev server. To check your versions:

```bash
node -v # v8.0.0 required
npm -v # 5.0.0 required
```

Using a node version manager like [nvm](https://github.com/nvm-sh/nvm) is strongly recommended, as the versions required by this project are several years out of date.

1. Install nvm using the [installation guide](https://github.com/nvm-sh/nvm#installing-and-updating).
1. Install node (and npm): `nvm install 8.0.0`
1. In the shell you're using for vtctld UI development: `nvm use 8.0.0`

### Starting the dev server

From the root `vitess/` directory:

1. Run Vitess locally using Docker and start a vtctld API on `http://localhost:15000/`. For more on this, see the guide on [Local Installation via Docker](https://vitess.io/docs/get-started/local-docker/).

	```bash
	source dev.env
	make docker_local && ./docker/local/run.sh
	```

2. Start a local Angular server on `http://localhost:4200`. The UI will automatically refresh every time you save a change. Note that you will _probably_ see build errors and security warnings during the `npm install` step; this is, unfortunately, to be expected due to out-of-date dependencies. :)

	```bash
	make web_start
	```

Note: the local docker install will also start a vtctld admin UI on http://localhost:15000/app. This instance will use the statically embedded files, and will not reflect any changes you make unless you run `make web_build`.

### Building for production

In production, the vtctld UI is hosted with [go.rice](https://github.com/GeertJohan/go.rice). All front-end assets must be built, minified, and embedded in the executable.

If you're ready to open a pull request for your changes, or if you want to do a production build just for fun... :)

```bash
make web_build
```

This will regenerate a bunch of files in the `web/vtctld2/dist/` directory, as well as update the embedded files in `rice-box.go`. Make sure you commit these generated files to your branch when opening your pull request.

To verify your changes, run Vitess locally. It is recommended to [use Docker](https://vitess.io/docs/get-started/local-docker/), which will make the vtctld UI accessible at `http://localhost:15000/app`.


## Troubleshooting

If you run into issues or have questions, we recommend posting in our [Slack channel](https://vitess.slack.com/). Click the Slack icon in the top right to join.
