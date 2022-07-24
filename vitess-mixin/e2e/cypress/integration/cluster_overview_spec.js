const fs = require('fs')

describe('vitess-mixin: Cluster Overview Dashboard Test', function () {

  let panelTitles = []

  before(function () {
    cy.readFile('./test/cluster_overview.json').then((data) => {
      cy.createDashboard(data)
    })
  })
  it('renders cluster overview dashboard', function () {
    cy.visit('/d/vitess-cluster-overview/cluster-overview-auto-generated')
  })
  //TEMPLATES
  it('contains the Interval Template', function () {
    cy.get('.submenu-controls').contains('Interval');
  })
  //LINKS
  it('contains vitess-mixin Dashboard links dropdown', function () {
    cy.get('.submenu-controls').get('.gf-form').contains('vitess-mixin')
  })
  // INIT PANELS
  it('checks all panels in the cluster overview dashboard exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(22)
      expect(titles).to.deep.eq([
        '',
        'Query success - vtgate',
        'Query success - vttablet',
        'Query latency p99',
        'QPS - vtgate',
        'QPS - MySQL',
        'vtgate',
        'vttablet',
        'QPS - vttablet',
        'keyspace',
        'shard',
        'vtctld',
        'Requests',
        'Error rate',
        'Duration 99th quantile',
        'Requests (by keyspace)',
        'Error rate (by keyspace)',
        'Duration 99th quantile (by keyspace)',
        '# of serving tablets',
        'Slow queries',
        'Query/Transaction killed'
      ])
    })
  })
})
