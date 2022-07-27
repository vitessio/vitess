const fs = require('fs')

describe('vitess-mixin: Vtgate Overview Dashboard Test', function() {

  let panelTitles = []

  before(function() {
    cy.readFile('./test/vtgate_overview.json').then((data) => {
      cy.createDashboard(data)
    })
  })
  it('renders vtgate overview dashboard', function() {
    cy.visit('/d/vitess-vtgate-overview/vtgate - overview (auto-generated)')
  })
  //SUB-MENU CONTROLS
  it('contains vitess-mixin Dashboard links dropdown', function() {
    cy.get('.submenu-controls').get('.gf-form').contains('vitess-mixin');
  })
  // RED ROW
  it('contains the RED row', function() {
    cy.get('.dashboard-row').contains('RED - Requests / Error rate / Duration');
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

  // RED (BY KEYSPACE) ROW
  it('contains the RED (by keyspace) row', function() {
    cy.get('.dashboard-row').contains('RED (by keyspace)');
  })
  it('RED (by keyspace) row is collapsed', function() {
    cy.get('.dashboard-row--collapsed').contains('RED (by keyspace)');
  })
  it('expands the RED (by keyspace) row', function(){
    cy.get('.dashboard-row__title.pointer').contains('RED (by keyspace)').click();
  })

  it('checks all panels in the RED (by keyspace) row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(4)
      expect(titles).to.deep.eq([
        '',
        'Requests (by keyspace)',
        'Error rate (by keyspace)',
        'Duration 99th quantile (by keyspace)'
      ])
    })
  })

  it('collapses the RED (by keyspace) row', function(){
    cy.get('.dashboard-row__title.pointer').contains('RED (by keyspace)').click();
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

  it('checks all panels in the RED (By tablet type) row exist',function() {
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
    cy.get('.dashboard-row__title.pointer').contains('Errors').click();
  })
  
  it('checks all panels in the Errors row exist',function() {
    cy.get('.panel-title').should(($p) => {
      let titles = $p.map((i,el) => {
        return Cypress.$(el).text()
      })
      titles = titles.get()
      expect(titles).to.have.length(4)
      expect(titles).to.deep.eq([
        '',
        'Errors (by code)',
        'Errors (by operation)',
        'Errors (by db_type)'
      ])
    })
  })
  
  it('collapses the Errors row', function(){
    cy.get('.dashboard-row__title.pointer').contains('Errors').click();
  })

  //DURATION ROW
  it('contains the Duration row', function() {
    cy.get('.dashboard-row').contains(/^Duration/);
  })
  it('Duration row is collapsed', function() {
    cy.get('.dashboard-row--collapsed').contains(/^Duration/);
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
  })

  //OS ROW
  it('contains the OS row', function() {
    cy.get('.dashboard-row').contains('OS');
  })
  it('OS row is collapsed', function() {
    cy.get('.dashboard-row--collapsed').contains('OS');
  })
  it('expands the OS row', function(){
    cy.get('.dashboard-row__title.pointer').contains('OS').click();
  })

  it('checks all panels in the row exist',function() {
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
