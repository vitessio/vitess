/// <reference types="react-scripts" />
declare namespace NodeJS {
    interface ProcessEnv {
        NODE_ENV: 'development' | 'production' | 'test';
        PUBLIC_URL: string;

        // Required. The full address of vtadmin-api's HTTP interface.
        // Example: "http://127.0.0.1:12345"
        REACT_APP_VTADMIN_API_ADDRESS: string;
    }
}

interface Window {}
