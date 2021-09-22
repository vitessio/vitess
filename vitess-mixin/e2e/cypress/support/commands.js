const http = require("http")

Cypress.Commands.overwrite('visit', (orig, url, options) => {
  options = options || {}
  options.auth = {
    username: 'admin',
    password: 'admin',
  }
  return orig(url, options)
})

Cypress.Commands.add('createDashboard', function(dashboardJSON) {

  const payload = JSON.stringify({
    dashboard: dashboardJSON,
    overwrite: true
  })

  const options = {
    auth: 'admin:admin',
    hostname: 'grafana',
    port: 3000,
    path: '/api/dashboards/db',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    }
  }

  const req = http.request(options)
  req.write(payload)
  req.end()
})
