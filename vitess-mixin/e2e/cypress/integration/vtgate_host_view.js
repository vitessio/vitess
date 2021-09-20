const fs = require('fs')

describe('vitess-mixin: Vtgate Host View Dashboard Test', function() {

  let panelTitles = []

  before(function() {
    cy.readFile('./test/vtgate_host_view.json').then((data) => {
      cy.createDashboard(data)
    })
  })
  it('renders vtgate hostview dashboard', function() {
    cy.visit('/d/vitess-vtgate-host-view/vtgate - host view (auto-generated)')
  })
  //SUB-MENU CONTROLS
  it('contains the Host(s) Template', function() {
    cy.get('.submenu-controls').contains('Host(s)');
  })
  it('contains vitess-mixin Dashboard links dropdown', function() {
    cy.get('.submenu-controls').get('.gf-form').contains('vitess-mixin')
  })
  //Rows are loaded at start time
  it('contains 5 rows', function() {
    cy.get('.dashboard-row').should('have.length',5)
  })
  // RED ROW
  it('contains the RED row', function() {
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
        'Duration 99th quantile'
      ])
    })
  })

  it('collapses the RED row', function(){
    cy.get('.dashboard-row__title.pointer').contains('RED - Requests / Error rate / Duration').click();
  })

  // ROW (BY TABLET TYPE)
  it('contains the  RED (by tablet type) row', function() {
    cy.get('.dashboard-row').contains('RED (by tablet type)');
  })
  it('RED (by tablet type) row is collapsed', function() {
    cy.get('.dashboard-row--collapsed').contains('RED (by tablet type)');
  })
  it('expands the RED (by tablet type) row', function(){
    cy.get('.dashboard-row__title.pointer').contains('RED (by tablet type)').click();
  })

  it('checks all panels in the RED (by tablet type) row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(4)
      expect(titles).to.deep.eq([
        '',
        'Requests (by db_type)',
        'Error rate (by db_type)',
        'Duration 99th quantile (by db_type)'
      ])
    })
  })

  it('collapses the RED (by tablet type) row', function(){
    cy.get('.dashboard-row__title.pointer').contains('RED (by tablet type)').click();
    cy.get('.dashboard-row--collapsed').contains('RED (by tablet type)');
  })

  //ERRORS ROW
  it('contains the Errors row', function() {
    cy.get('.dashboard-row').contains('Errors');
  })
  it('Errors row is collapsed', function() {
    cy.get('.dashboard-row--collapsed').contains('Errors');
  })
  it('expands the Errors row', function(){
    cy.get('.dashboard-row__title.pointer').contains('Errors').scrollIntoView().click();
  })
  it('checks all panels in the Errors Row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(3)
      expect(titles).to.deep.eq([
        '',
        'Errors (by keyspace)',
        'Errors (by code)'
      ])
    })
  })
  it('collapses the Errors row', function(){
    cy.get('.dashboard-row__title.pointer').contains('Errors').click();
    cy.get('.dashboard-row--collapsed').contains('Errors');
  })
  //DURATION ROW
  it('contains the Duration row', function() {
    cy.get('.dashboard-row').contains(/^Duration/)
  })
  it('Duration row is collapsed', function() {
    cy.get('.dashboard-row--collapsed').contains(/^Duration/)
  })
  it('expands the Duration row', function(){
    cy.get('.dashboard-row__title.pointer').contains(/^Duration/).click();
  })

  it('checks all panels in the Duration row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(4)
      expect(titles).to.deep.eq([
        '',
        'Duration (Avg)',
        'Duration 50th quantile',
        'Duration 95th quantile'
      ])
    })
  })

  it('collapses the Duration row', function(){
    cy.get('.dashboard-row__title.pointer').contains(/^Duration/).click();
    cy.get('.dashboard-row--collapsed').contains(/^Duration/);
  })
  //OS ROW
  it('contains the OS row', function() {
    cy.get('.dashboard-row').contains('OS')
  })
  it('OS row is collapsed', function() {
    cy.get('.dashboard-row--collapsed').contains('OS')
  })
  it('expands the OS row', function(){
    cy.get('.dashboard-row__title.pointer').contains('OS').click();
  })

  it('checks all panels in the OS row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(5)
      expect(titles).to.deep.eq([
        '',
        'CPU Usage',
        'Memory Usage',
        'Network Usage',
        'TCP Retransmissions'
      ])
    })
  })

  it('collapses the OS row', function(){
    cy.get('.dashboard-row__title.pointer').contains('OS').click();
    cy.get('.dashboard-row--collapsed').contains('OS');
  })
})
