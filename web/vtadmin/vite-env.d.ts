/// <reference types="vite/client" />
declare namespace Vite {
  interface ImportMetaEnv {
      MODE: 'development' | 'production' | 'development';
      BASE_URL: string;

      PROD: boolean;
      DEV: boolean;

      /* REQUIRED */

      // Required. The full address of vtadmin-api's HTTP interface.
      // Example: "http://127.0.0.1:12345"
      VITE_VTADMIN_API_ADDRESS: string;

      /* OPTIONAL */

      // Optional. An API key for https://bugsnag.com. If defined,
      // the @bugsnag/js client will be initialized. Your Bugsnag API key
      // can be found in your Bugsnag Project Settings.
      VITE_BUGSNAG_API_KEY?: string;

      // Optional. Build variables.
      VITE_BUILD_BRANCH?: string;
      VITE_BUILD_SHA?: string;

      // Optional, but recommended. When true, enables front-end components that query
      // vtadmin-api's /api/experimental/tablet/{tablet}/debug/vars endpoint.
      VITE_ENABLE_EXPERIMENTAL_TABLET_DEBUG_VARS?: boolean | string;

      // Optional. Configures the `credentials` property for fetch requests.
      // made against vtadmin-api. If unspecified, uses fetch defaults.
      // See https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API/Using_Fetch#sending_a_request_with_credentials_included
      VITE_FETCH_CREDENTIALS?: RequestCredentials;

      // Optional.  Used for the document.title property. The default is "VTAdmin".
      // Overriding this can be useful to differentiate between multiple VTAdmin deployments,
      // e.g., "VTAdmin (staging)".
      VITE_DOCUMENT_TITLE?: string;

      // Optional. Defaults to "false". If "true", UI controls that correspond to write actions (PUT, POST, DELETE) will be hidden.
      // Note that this *only* affects the UI. If write actions are a concern, Vitess operators are encouraged
      // to also configure vtadmin-api for role-based access control (RBAC) if needed;
      // see https://github.com/vitessio/vitess/blob/main/go/vt/vtadmin/rbac/rbac.go
      VITE_READONLY_MODE?: string;
  }
}

interface Window {
  env: Vite.ImportMetaEnv;
}
