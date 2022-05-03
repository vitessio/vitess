# VTAdmin Web
VTAdmin Web is a user interface that allows Vitess users to easily manage and view state of their Vitess components. VTAdmin Web should be used with VTAdmin API (shipped within the Vitess monorepo).

## Usage
### Client-side (Rails example)
You can directly import the .js files and .css files necessary to use in a Rails-Webpacker asset pipeline, like so:
```javascript
import "@planetscale/vtadmin"
```

Then in your .erb file, import the styles and js as a pack:
```
<%= javascript_pack_tag "vtadmin", "data-turbolinks-track": "reload" %>
<%= stylesheet_pack_tag "vtadmin", media: "all", "data-turbolinks-track": "reload" %>
```
## Scripts
**File structure**
```
- vtadmin
    - planetscale-vtadmin-template
        - index.ts // Index file that is copied into package directory as the entrypoint of npm package
        - README.md // README for @planetscale/vtadmin npm package
        - package.json // Base package.json for @planetscale/vtadmin npm package
    - scripts
        - verify-pscale-build.ts // Script to copy vtadmin's package.json and build folder into package folder
        - build-pscale-vtadmin.ts // Webpacker stuff
    - planetscale-vtadmin // Directory containing npm module @planetscale/vtadmin
```

`web/vtadmin/package.json` includes a build script run by `npm run build:planetscale-vtadmin` that:
1. Compiles contents of `web/vtadmin/planetscale-vtadmin-template` to a new directory `web/vtadmin/planetscale-vtadmin`
2. Builds vtadmin and copies its output to `web/vtadmin/planetscale-vtadmin`
## Release NPM Package
To release new versions of the vtadmin npm package:
1. Run `npm run build:planetscale` to create package directory for `@planetscale/vtadmin` npm module
2. `cd planetscale-vtadmin` to navigate into the folder for module `@planetscale/vtadmin`
3. Bump the version of vtadmin at `web/vtadmin/planetscale-vtadmin/package.json`
4. `npm publish --access public` to publish new version to npm

