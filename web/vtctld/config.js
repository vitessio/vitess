// This file contains config that may need to be changed
// on a site-local basis.
vtconfig = {
  tabletLinks: function(tablet) {
    return [
      {
        title: 'Status',
        href: 'http://'+tablet.Hostname+':'+tablet.Portmap.vt+'/debug/status'
      }
    ];
  }
};
