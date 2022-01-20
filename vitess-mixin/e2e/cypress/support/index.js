require('./commands')

const fs = require('fs')

// This does not use the usual Cypress.Commands.add registration so that it's
// performed synchronously and we're able to return the panelTitles variable.
cy.createDashboardFromUnitTests = function(testDir, uid, excludePanels=[]) {
  let panelTitles = []
  cy.readFile(testDir).then(function(str) {
    let panels = []
    for (let [i, [name, panel]] of Object.entries(Object.entries(str))) {
      if (excludePanels.includes(name)) {
        continue
      }
      panel['id'] = parseInt(i)
      panel['gridPos'] = {'w': 6, 'h': 4, 'x': i * 6 % 24 }
      panelTitles.push(panel.title)
      panels.push(panel)
    }
    let dashboardJSON = {
      'uid': uid,
      'title': uid,
      'panels': panels
    }
    cy.createDashboard(dashboardJSON)
  })
  return panelTitles
}
