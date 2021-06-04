const fs = require('fs')

describe('vitess-mixin: Vttablet Host View Dashboard Test', function () {

  let panelTitles = []

  before(function () {
    cy.readFile('./test/vttablet_host_view.json').then((data) => {
      cy.createDashboard(data)
    })
  })
  it('renders vttablet hostview dashboard', function () {
    cy.visit('/d/vitess-vttablet-host-view/vttablet - host view (auto-generated)')
  })
  //SUB-MENU CONTROLS
  it('contains the Host(s) Template', function () {
    cy.get('.submenu-controls').contains('Host(s)');
  })
  it('contains vitess-mixin Dashboard links dropdown', function () {
    cy.get('.submenu-controls').get('.gf-form').contains('vitess-mixin')
  })
  //All Rows are loaded at start time
  it('contains 12 rows', function () {
    cy.get('.dashboard-row').should('have.length', 12)
  })

  // RED ROW
  it('contains the RED row', function () {
    cy.get('.dashboard-row').contains('RED - Requests / Error rate / Duration')
  })

  it('checks all panels in the RED row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(4)
      expect(titles).to.deep.eq([
        '',
        'Requests',
        'Error rate',
        'Duration (p99)'
      ])
    })
  })

  it('collapses the RED row', function () {
    cy.get('.dashboard-row__title.pointer').contains('RED - Requests / Error rate / Duration').click();
  })
  // RED BY PLAN TYPE ROW

  it('contains the RED (by plan type) row', function () {
    cy.get('.dashboard-row').contains('RED (by plan type)')
  })
  it('RED (by plan type) row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains('RED (by plan type)')
  })
  it('expands the RED (by plan type) row', function () {
    cy.get('.dashboard-row__title.pointer').contains('RED (by plan type)').click();
  })

  it('checks all panels in the RED (by plan type) row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(4)
      expect(titles).to.deep.eq([
        '',
        'Requests (by plan type)',
        'Error rate (by plan type)',
        'Duration p99 (by plan type)'
      ])
    })
  })

  it('collapses the RED (by plan type) row', function () {
    cy.get('.dashboard-row__title.pointer').contains('RED (by plan type)').click();
    cy.get('.dashboard-row--collapsed').contains('RED (by plan type)');
  })

  // RED BY TABLE ROW

  it('contains the RED (by table) row', function () {
    cy.get('.dashboard-row').contains('RED (by table)')
  })
  it('RED (by table) row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains('RED (by table)')
  })
  it('expands the RED (by table) row', function () {
    cy.get('.dashboard-row__title.pointer').contains('RED (by table)').click();
  })

  it('checks all panels in the RED (by table) row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(3)
      expect(titles).to.deep.eq([
        '',
        'Requests (by table)',
        'Error rate (by table)'
      ])
    })
  })

  it('collapses the RED (by table) row', function () {
    cy.get('.dashboard-row__title.pointer').contains('RED (by table)').click();
    cy.get('.dashboard-row--collapsed').contains('RED (by table)');
  })

  // Rows Returned
  it('contains the Rows Returned row', function () {
    cy.get('.dashboard-row').contains(/^Rows returned/)
  })
  it('Rows returned row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains(/^Rows returned/)
  })
  it('expands the Rows returned row', function () {
    cy.get('.dashboard-row__title.pointer').contains(/^Rows returned/).click();
  })

  it('checks all panels in the Rows returned row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(3)
      expect(titles).to.deep.eq([
        '',
        'Rows Returned (by table)',
        'Rows Returned (by plan)'
      ])
    })
  })

  it('collapses the Rows returned row', function () {
    cy.get('.dashboard-row__title.pointer').contains(/^Rows returned/).click();
    cy.get('.dashboard-row--collapsed').contains(/^Rows returned/);
  })

  // Queries/Errors
  it('contains the Queries/Errors row', function () {
    cy.get('.dashboard-row').contains(/^Queries\/Errors/)
  })
  it('Queries/Errors row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains(/^Queries\/Errors/)
  })
  it('expands the Queries/Errors row', function () {
    cy.get('.dashboard-row__title.pointer').contains(/^Queries\/Errors/).click();
  })

  it('checks all panels in the Queries/Errors row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(3)
      expect(titles).to.deep.eq([
        '',
        'Queries Killed',
        'Query errors (by error code)'
      ])
    })
  })

  it('collapses the Queries/Errors row', function () {
    cy.get('.dashboard-row__title.pointer').contains(/^Queries\/Errors/).click();
    cy.get('.dashboard-row--collapsed').contains(/^Queries\/Errors/);
  })

  // Vitess Query pool
  it('contains the Vitess - Query pool row', function () {
    cy.get('.dashboard-row').contains('Vitess - Query pool')
  })
  it('Vitess - Query pool row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains('Vitess - Query pool')
  })
  it('expands the Vitess - Query pool row', function () {
    cy.get('.dashboard-row__title.pointer').contains('Vitess - Query pool').click();
  })

  it('checks all panels in the Query pool row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(6)
      expect(titles).to.deep.eq([
        '',
        'Available Connections',
        'Active Connections',
        'Idle Closed Rate',
        'Wait count',
        'Avg wait time'
      ])
    })
  })

  it('collapses the Vitess - Query pool row', function () {
    cy.get('.dashboard-row__title.pointer').contains('Vitess - Query pool').click();
    cy.get('.dashboard-row--collapsed').contains('Vitess - Query pool');
  })

  // Vitess Transaction pool
  it('contains the Vitess - Transaction pool row', function () {
    cy.get('.dashboard-row').contains('Vitess - Transaction pool')
  })
  it('Vitess - Transaction pool row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains('Vitess - Transaction pool')
  })
  it('expands the Vitess - Transaction pool row', function () {
    cy.get('.dashboard-row__title.pointer').contains('Vitess - Transaction pool').click();
  })

  it('checks all panels in the Transaction row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(6)
      expect(titles).to.deep.eq([
        '',
        'Available Connections',
        'Active Connections',
        'Idle Closed Rate',
        'Wait count',
        'Avg wait time'
      ])
    })
  })

  it('collapses the Vitess - Transaction pool row', function () {
    cy.get('.dashboard-row__title.pointer').contains('Vitess - Transaction pool').click();
    cy.get('.dashboard-row--collapsed').contains('Vitess - Transaction pool');
  })

  //Vitess timings
  it('contains the Vitess Timings row', function () {
    cy.get('.dashboard-row').contains(/^Vitess Timings/)
  })
  it('Vitess Timings row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains(/^Vitess Timings/)
  })
  it('Vitess Timings row has 8 panels', function () {
    cy.get('.dashboard-row').contains(/^Vitess Timings/).find('.dashboard-row__panel_count').contains('(8 panels)')
  })
  it('expands the Vitess Timings row', function () {
    cy.get('.dashboard-row__title.pointer').contains(/^Vitess Timings/).click();
  })

  it('checks all panels in the Vitess Timings row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(9)
      expect(titles).to.deep.eq([
        '',
        'Query Duration (avg)',
        'Query Duration (p50)',
        'Query Duration (p95)',
        'VtGate -> VtTablet Call Time (avg)',
        'Query Time Distribution (Heatmap)',
        'Transaction Duration (avg)',
        'Transaction Duration (p50)',
        'Transaction Duration (p95)'
      ])
    })
  })

  it('collapses the Vitess Timings row', function () {
    cy.get('.dashboard-row__title.pointer').contains(/^Vitess Timings/).click();
    cy.get('.dashboard-row--collapsed').contains(/^Vitess Timings/);
  })


  //MYSQL ROW
  it('contains the MySQL row', function () {
    cy.get('.dashboard-row').contains('MySQL');
  })
  it('MySQL row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains('MySQL');
  })
  it('expands the MySQL row', function () {
    cy.get('.dashboard-row__title.pointer').contains('MySQL').click();
  })
  it('checks all panels in the MySQL row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(2)
      expect(titles).to.deep.eq([
        '',
        'Slow Queries'
      ])
    })
  })
  it('collapses the MySQL row', function () {
    cy.get('.dashboard-row__title.pointer').contains('MySQL').click();
    cy.get('.dashboard-row--collapsed').contains('MySQL');
  })

  //MYSQL Timings ROW
  it('contains the MySQL Timings row', function () {
    cy.get('.dashboard-row').contains(/MySQL Timings/);
  })
  it('MySQL Timings row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains(/MySQL Timings/);
  })
  it('expands the MySQL Timings row', function () {
    cy.get('.dashboard-row__title.pointer').contains(/MySQL Timings/).click();
  })

  it('checks all panels in the MySQL row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(4)
      expect(titles).to.deep.eq([
        '',
        'MySQL time (avg)',
        'MySQL Exec Time P50',
        'MySQL Exec Time P95'
      ])
    })
  })

  it('collapses the MySQL Timings row', function () {
    cy.get('.dashboard-row__title.pointer').contains(/MySQL Timings/).click();
    cy.get('.dashboard-row--collapsed').contains(/MySQL Timings/);
  })

  //OS ROW
  it('contains the OS row', function () {
    cy.get('.dashboard-row').contains('OS');
  })
  it('OS row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains('OS');
  })
  it('expands the OS row', function () {
    cy.get('.dashboard-row__title.pointer').contains('OS').scrollIntoView().click();
  })
  it('checks all panels in the OS row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(6)
      expect(titles).to.deep.eq([
        '',
        'CPU Usage',
        'Memory Usage',
        '/mnt disk free',
        'Network Tx Bytes',
        'Network Rx Bytes'
      ])
    })
  })
  it('collapses the OS row', function () {
    cy.get('.dashboard-row__title.pointer').contains('OS').click();
    cy.get('.dashboard-row--collapsed').contains('OS');
  })

  //Misc ROW
  it('contains the Misc row', function () {
    cy.get('.dashboard-row').contains(/^Misc/);
  })
  it('Misc row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains(/^Misc/);
  })
  it('expands the Misc row', function () {
    cy.get('.dashboard-row__title.pointer').contains(/^Misc/).click();
  })
  it('checks all panels in the Misc row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(4)
      expect(titles).to.deep.eq([
        '',
        'GC Count',
        'GC Duration total per second',
        'GC Duration quantiles (all hosts)'
      ])
    })
  })
  it('collapses the Misc row', function () {
    cy.get('.dashboard-row__title.pointer').contains(/^Misc/).click();
    cy.get('.dashboard-row--collapsed').contains(/^Misc/);
  })

})
