/**
 * Copyright 2022 The Vitess Authors.
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

export const env: () => Vite.ImportMetaEnv = () => {
    return { ...window.env, ...import.meta.env };
};

// import.meta.env variables are always strings, hence this tiny helper function
// to transmute it into a boolean. It is a function, rather than a constant,
// to support dynamic updates to import.meta.env in tests.
export const isReadOnlyMode = (): boolean => env().VITE_READONLY_MODE === 'true';

// Monitoring templates.
// These functions retrieve the URL templates for various monitoring dashboards from the environment variables.
export const getVTClusterMonitoringTemplate = (): string | undefined => env().VITE_VITESS_MONITORING_CLUSTER_TEMPLATE;
export const getVTTabletMonitoringTemplate = (): string | undefined => env().VITE_VITESS_MONITORING_VTTABLET_TEMPLATE;
export const getVTGateMonitoringTemplate = (): string | undefined => env().VITE_VITESS_MONITORING_VTGATE_TEMPLATE;
export const getMysqlMonitoringTemplate = (): string | undefined => env().VITE_MYSQL_MONITORING_TEMPLATE;

// Column titles for monitoring dashboards.
export const getVitessMonitoringDashboardTitle = (): string =>
    env().VITE_VITESS_MONITORING_DASHBOARD_TITLE || 'Vt Monitoring Dashboard';
export const getMysqlMonitoringDashboardTitle = (): string =>
    env().VITE_MYSQL_MONITORING_DASHBOARD_TITLE || 'DB Monitoring Dashboard';
