// This file contains config that may need to be changed
// on a site-local basis.
vtconfig = {
  k8s_proxy_re: /(\/api\/v1\/proxy\/.*)\/services\/vtctld/,
  tabletLinks: function(tablet) {
    status_href = 'http://'+tablet.hostname+':'+tablet.port_map.vt+'/debug/status'

    // If we're in Kubernetes, route through the proxy.
    var match = window.location.pathname.match(vtconfig.k8s_proxy_re);
    if (match) {
      status_href = match[1] + '/pods/vttablet-' + tablet.alias.uid + ':' + tablet.port_map.vt + '/debug/status';
    }

    return [
      {
        title: 'Status',
        href: status_href
      }
    ];
  }
};
