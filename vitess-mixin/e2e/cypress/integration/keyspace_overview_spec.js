const fs = require('fs')

describe('vitess-mixin: Keyspace Overview Dashboard Test', function () {

  before(function () {
    cy.readFile('./test/keyspace_overview.json').then((data) => {
      cy.createDashboard(data)
    })
  })
  it('renders keyspace overview dashboard', function () {
    cy.visit('/d/vitess-keyspace-overview/keyspace - overview (auto-generated)')
  })
  //SUB-MENU CONTROLS
  it('contains the Interval Template', function () {
    cy.get('.submenu-controls').contains('Interval');
  })
  it('contains the Region Template', function () {
    cy.get('.submenu-controls').contains('Region');
  })
  it('contains the Keyspace Template', function () {
    cy.get('.submenu-controls').contains('Keyspace');
  })
  it('contains the Table Template', function () {
    cy.get('.submenu-controls').contains('Table')
  })
  it('contains vitess-mixin Dashboard links dropdown', function () {
    cy.get('.submenu-controls').get('.gf-form').contains('vitess-mixin');
  })
  //RED
  it('contains the RED row', function () {
    cy.get('.dashboard-row').contains('RED - Requests / Error rate / Duration');
  })
  it('contains the Requests panel', function () {
    cy.get('.panel-title').contains('Requests');
  })
  it('contains the Error rate panel', function () {
    cy.get('.panel-title').contains('Error rate');
  })
  it('contains the Duration p99 panel', function () {
    cy.get('.panel-title').contains('Duration (p99)');
  })
  // VTGATE REQUESTS
  it('vtgate - Requests (by table / by plan / by keyspace ) row has 3 panels', function () {
    cy.get('.dashboard-row').contains('vtgate - Requests (by table / by plan / by keyspace )').find('.dashboard-row__panel_count').contains('(3 panels)');
  })
  it('vtgate - Requests (by table / by plan / by keyspace ) row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains('vtgate - Requests (by table / by plan / by keyspace )');
  })
  it('expands the vtgate - Requests (by table / by plan / by keyspace ) row', function () {
    cy.get('.dashboard-row__title.pointer').contains('vtgate - Requests (by table / by plan / by keyspace )').click();
  })
  it('contains the Requests (by table) panel', function () {
    cy.get('.panel-title').contains('Requests (by table)');
  })
  it('contains the Requests (by plan type) panel', function () {
    cy.get('.panel-title').contains('Requests (by plan type)');
  })
  it('contains the Requests success rate (by keyspace)', function () {
    cy.get('.panel-title').contains('Requests success rate (by keyspace)');
  })
  it('collapses the vtgate - Requests (by table / by plan / by keyspace ) row', function () {
    cy.get('.dashboard-row__title.pointer').contains('vtgate - Requests (by table / by plan / by keyspace )').click();
    cy.get('.dashboard-row--collapsed').contains('vtgate - Requests (by table / by plan / by keyspace )');
  })
  // VTTABLET REQUESTS  
  it('contains the  vttablet requests row', function () {
    cy.get('.dashboard-row').contains('vttablet - Requests (by table / by plan / by keyspace )');
  })
  it('vttablet - Requests (by table / by plan / by keyspace ) row has 3 panels', function () {
    cy.get('.dashboard-row').contains('vttablet - Requests (by table / by plan / by keyspace )').find('.dashboard-row__panel_count').contains('(3 panels)');
  })
  it('vttablet - Requests (by table / by plan / by keyspace ) row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains('vttablet - Requests (by table / by plan / by keyspace )');
  })
  it('expands the vttablet - Requests (by table / by plan / by keyspace ) row', function () {
    cy.get('.dashboard-row__title.pointer').contains('vttablet - Requests (by table / by plan / by keyspace )').click();
  })
  it('contains the Requests (by table) panel', function () {
    cy.get('.panel-title').contains('Requests (by table)');
  })
  it('contains the Requests (by plan type) panel', function () {
    cy.get('.panel-title').contains('Requests (by plan type)');
  })
  it('contains the Requests success rate (by keyspace)', function () {
    cy.get('.panel-title').contains('Requests success rate (by keyspace)');
  })
  it('collapses the vttablet - Requests (by table / by plan / by keyspace ) row', function () {
    cy.get('.dashboard-row__title.pointer').contains('vttablet - Requests (by table / by plan / by keyspace )').click();
    cy.get('.dashboard-row--collapsed').contains('vttablet - Requests (by table / by plan / by keyspace )');
  })
  // ERRORS
  it('contains the Errors row', function () {
    cy.get('.dashboard-row').contains('Errors');
  })
  it('Errors row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains('Errors');
  })
  it('Errors row has 3 panels', function(){
    cy.get('.dashboard-row').contains('Errors').find('.dashboard-row__panel_count').contains('(3 panels)');
  })
  it('expands the Errors row', function () {
    cy.get('.dashboard-row__title.pointer').contains('Errors').click();
  })
  it('contains the Errors (by table) panel', function () {
    cy.get('.panel-title').contains('Errors (by table)');
  })
  it('contains the Errors (by plan type/table) panel', function () {
    cy.get('.panel-title').contains('Errors (by plan type/table)');
  })
  it('contains the Errors (by shard/table) panel', function () {
    cy.get('.panel-title').contains('Errors (by shard/table)');
  })
  it('collapses the Errors row', function () {
    cy.get('.dashboard-row__title.pointer').contains('Errors').click();
    cy.get('.dashboard-row--collapsed').contains('Errors');
  })
  // Rows Returned
  it('contains the Rows returned row', function () {
    cy.get('.dashboard-row').contains('Rows returned');
  })
  it('Rows returned row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains('Rows returned');
  })
  it('Rows returned row has 3 panels', function(){
    cy.get('.dashboard-row').contains('Rows returned').find('.dashboard-row__panel_count').contains('(3 panels)');
  })
  it('expands the Rows returned row', function () {
    cy.get('.dashboard-row__title.pointer').contains('Rows returned').click();
  })
  it('contains the Rows returned (by table) panel', function () {
    cy.get('.panel-title').contains('Rows returned (by table)');
  })
  it('contains the Rows returned (by table) panel', function () {
    cy.get('.panel-title').contains('Rows returned (by table)');
  })
  it('contains the Rows returned (by shard) panel', function () {
    cy.get('.panel-title').contains('Rows returned (by shard)');
  })
  it('contains the Rows returned (by table) panel', function () {
    cy.get('.panel-title').contains('Rows returned (by table)');
  })
  it('collapses the Rows returned row', function () {
    cy.get('.dashboard-row__title.pointer').contains('Rows returned').click();
    cy.get('.dashboard-row--collapsed').contains('Rows returned');
  })
  // Query/Transaction timings
  it('contains the Query Timings row', function () {
    cy.get('.dashboard-row').contains("Query/Transaction timings (table filter doesn't apply)");
  })
  it('Query Timings row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains("Query/Transaction timings (table filter doesn't apply)");
  })
  it('Query Timings row has 10 panels', function(){
    cy.get('.dashboard-row').contains("Query/Transaction timings (table filter doesn't apply)").find('.dashboard-row__panel_count').contains('(10 panels)');
  })
  it('expands the Query/Transaction timings (table filter doesn\'t apply row', function () {
    cy.get('.dashboard-row__title.pointer').contains("Query/Transaction timings (table filter doesn't apply)").click();
  })
  it('contains the Query duration (avg) panel', function () {
    cy.get('.panel-title').contains('Query duration (avg');
  })
  it('contains the Query duration (p50) panel', function () {
    cy.get('.panel-title').contains('Query duration (p50)');
  })
  it('contains the Query duration (p95) panel', function () {
    cy.get('.panel-title').contains('Query duration (p95)');
  })
  it('contains the Query duration (p999) panel', function () {
    cy.get('.panel-title').contains('Query duration (p999)');
  })
  it('contains the Transaction duration (avg) panel', function () {
    cy.get('.panel-title').contains('Transaction duration (avg)');
  })
  it('contains the Transaction duration (p50) panel', function () {
    cy.get('.panel-title').contains('Transaction duration (p50)');
  })
  it('contains the Transaction duration (p95) panel', function () {
    cy.get('.panel-title').contains('Transaction duration (p95)');
  })
  it('contains the Transaction duration (p999) panel', function () {
    cy.get('.panel-title').contains('Transaction duration (p999)');
  })
  it('contains the Query duration heatmap panel', function () {
    cy.get('.panel-title').contains('Query duration heatmap');
  })  
  it('contains the Transaction duration heatmap panel', function () {
    cy.get('.panel-title').contains('Transaction duration heatmap');
  })
  it('collapses the Query/Transaction timings (table filter doesn\'t apply) row', function () {
    cy.get('.dashboard-row__title.pointer').contains("Query/Transaction timings (table filter doesn't apply)").click();
    cy.get('.dashboard-row--collapsed').contains("Query/Transaction timings (table filter doesn't apply)");
  })
  // MySQL
  it('contains the MySQL row', function () {
    cy.get('.dashboard-row').contains('MySQL');
  })
  it('MySQL row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains('MySQL');
  })
  it('MySQL row has 5 panels', function(){
    cy.get('.dashboard-row--collapsed').contains('MySQL').find('.dashboard-row__panel_count').contains('(5 panels)');
  })
  it('expands the MySQL row', function () {
    cy.get('.dashboard-row__title.pointer').contains('MySQL').click();
  })
  it('contains the Slow Queries panel', function () {
    cy.get('.panel-title').contains('Slow Queries');
  })
  it('contains the Replication Lag panel', function () {
    cy.get('.panel-title').contains('Replication Lag');
  })
  it('contains the InnoDB row read operations panel', function () {
    cy.get('.panel-title').contains('InnoDB row read operations');
  })
  it('contains the Semi-sync avg wait panel', function () {
    cy.get('.panel-title').contains('Semi-sync avg wait');
  })
  it('contains the MySQL version panel', function () {
    cy.get('.panel-title').contains('MySQL version');
  })
  it('collapses the MySQL row', function () {
    cy.get('.dashboard-row__title.pointer').contains('MySQL').click();
    cy.get('.dashboard-row--collapsed').contains('MySQL');
  })
  // OS
  it('contains the OS row', function () {
    cy.get('.dashboard-row').contains('OS');
  })
  it('OS row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains('OS');
  })
  it('OS row has 4 panels', function(){
    cy.get('.dashboard-row--collapsed').contains('OS').find('.dashboard-row__panel_count').contains('(4 panels)');
  })
  it('expands the OS row', function () {
    cy.get('.dashboard-row__title.pointer').contains('OS').click();
  })
  it('contains the CPU Usage panel', function () {
    cy.get('.panel-title').contains('CPU Usage');
  })
  it('contains the Memory Usage panel', function () {
    cy.get('.panel-title').contains('Memory Usage');
  })
  it('contains the Network panel', function () {
    cy.get('.panel-title').contains('Network Usage');
  })
  it('contains the /mnt disk free panel', function () {
    cy.get('.panel-title').contains('/mnt disk free');
  })
  it('collapses the OS row', function () {
    cy.get('.dashboard-row__title.pointer').contains('OS').click();
    cy.get('.dashboard-row--collapsed').contains('OS');
  })
})
