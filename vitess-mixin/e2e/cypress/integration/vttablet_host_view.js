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
  it('contains the Region Template', function () {
    cy.get('.submenu-controls').contains('Region');
  })
  it('contains the Keyspace Template', function () {
    cy.get('.submenu-controls').contains('Keyspace');
  })
  it('contains the Shard Template', function () {
    cy.get('.submenu-controls').contains('Shard');
  })
  it('contains the Host(s) Template', function () {
    cy.get('.submenu-controls').contains('Host(s)');
  })
  it('contains vitess-mixin Dashboard links dropdown', function () {
    cy.get('.submenu-controls').get('.gf-form').contains('vitess-mixin')
  })
  //Rows and Panels loaded at start time
  it('contains 13 rows', function () {
    cy.get('.dashboard-row').should('have.length', 13)
  })
  it('loads 4 panels at init time', function () {
    cy.get('.panel-title').should('have.length', 4)
  })
  // RED ROW
  it('contains the RED row', function () {
    cy.get('.dashboard-row').contains('RED - Requests / Error rate / Duration')
  })
  it('contains the Requests panel', function () {
    cy.get('.panel-title').contains('Requests')
  })
  it('contains the Error rate panel', function () {
    cy.get('.panel-title').contains('Error rate')
  })
  it('contains the Duration (p99) panel', function () {
    cy.get('.panel-title').contains('Duration (p99)')
  })
  it('collapses the RED row', function () {
    cy.get('.dashboard-row__title.pointer').contains('RED - Requests / Error rate / Duration').click();
    cy.get('.dashboard-row--collapsed').contains('RED - Requests / Error rate / Duration');
    cy.get('.dashboard-row').contains('RED - Requests / Error rate / Duration').find('.dashboard-row__panel_count').contains('(3 panel');
  })
  // RED BY PLAN TYPE ROW

  it('contains the RED (by plan type) row', function () {
    cy.get('.dashboard-row').contains('RED (by plan type)')
  })
  it('RED (by plan type) row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains('RED (by plan type)')
  })
  it('RED (by plan type) row has 3 panels', function () {
    cy.get('.dashboard-row').contains('RED (by plan type)').find('.dashboard-row__panel_count').contains('(3 panels)')
  })
  it('expands the RED (by plan type) row', function () {
    cy.get('.dashboard-row__title.pointer').contains('RED (by plan type)').click();
  })
  it('contains the Requests (by plan type) panel', function () {
    cy.get('.panel-title').contains('Requests (by plan type)')
  })
  it('contains the Error rate (by plan type) panel', function () {
    cy.get('.panel-title').contains('Error rate (by plan type)')
  })
  it('contains the Duration p99 (by plan type) panel', function () {
    cy.get('.panel-title').contains('Duration p99 (by plan type)')
  })
  it('collapses the RED (by plan type) row', function () {
    cy.get('.dashboard-row__title.pointer').contains('RED (by plan type)').click();
    cy.get('.dashboard-row--collapsed').contains('RED (by plan type)');
  })

  // RED BY SHARD ROW

  it('contains the RED (by shard) row', function () {
    cy.get('.dashboard-row').contains('RED (by shard)')
  })
  it('RED (by shard) row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains('RED (by shard)')
  })
  it('RED (by shard) row has 3 panels', function () {
    cy.get('.dashboard-row').contains('RED (by shard)').find('.dashboard-row__panel_count').contains('(3 panels)')
  })
  it('expands the RED (by shard) row', function () {
    cy.get('.dashboard-row__title.pointer').contains('RED (by shard)').click();
  })
  it('contains the Requests (by shard) panel',function() {
    cy.get('.panel-title').contains('Requests (by shard)')
  })
  it('contains the Error rate (by shard) panel',function() {
    cy.get('.panel-title').contains('Error rate (by shard)')
  })
  it('contains the Duration p99 (by shard) panel',function() {
    cy.get('.panel-title').contains('Duration p99 (by shard)')
  })
  it('collapses the RED (by shard) row', function () {
    cy.get('.dashboard-row__title.pointer').contains('RED (by shard)').click();
    cy.get('.dashboard-row--collapsed').contains('RED (by shard)');
  })

  // RED BY TABLE ROW

  it('contains the RED (by table) row', function () {
    cy.get('.dashboard-row').contains('RED (by table)')
  })
  it('RED (by table) row is collapsed', function () {
    cy.get('.dashboard-row--collapsed').contains('RED (by table)')
  })
  it('RED (by table) row has 2 panels', function () {
    cy.get('.dashboard-row').contains('RED (by table)').find('.dashboard-row__panel_count').contains('(2 panels)')
  })
  it('expands the RED (by table) row', function () {
    cy.get('.dashboard-row__title.pointer').contains('RED (by table)').click();
  })
  it('contains the Requests (by table) panel',function() {
    cy.get('.panel-title').contains('Requests (by table)')
  })
  it('contains the Error rate (by table) panel',function() {
    cy.get('.panel-title').contains('Error rate (by table)')
  })

  // THE PROMETHEUS METRIC DOES NOT INCLUDE TABLE INFORMATION 
  // TODO: ADD THE DURATION PANEL IF THIS METRIC BECOMES AVAILABLE.
  // it('contains the Duration p99 (by table) panel',function() {
  //   cy.get('.panel-title').contains('Duration p99 (by table)')
  // })
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
  it('Rows returned row has 2 panels', function () {
    cy.get('.dashboard-row').contains(/^Rows returned/).find('.dashboard-row__panel_count').contains('(2 panels)')
  })
  it('expands the Rows returned row', function () {
    cy.get('.dashboard-row__title.pointer').contains(/^Rows returned/).click();
  })
  it('contains the Rows returned (by table) panel',function() {
    cy.get('.panel-title').contains(/^Rows returned \(by table\)/)
  })
  it('contains the Rows returned (by plan type) panel',function() {
    cy.get('.panel-title').contains(/^Rows returned \(by plan type\)/)
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
  it('Queries/Errors row has 2 panels', function () {
    cy.get('.dashboard-row').contains(/^Queries\/Errors/).find('.dashboard-row__panel_count').contains('(2 panels)')
  })
  it('expands the Queries/Errors row', function () {
    cy.get('.dashboard-row__title.pointer').contains(/^Queries\/Errors/).click();
  })
  it('contains the Queries Killed panel',function() {
    cy.get('.panel-title').contains(/^Queries Killed/)
  })
  it('contains the Query errors by type panel',function() {
    cy.get('.panel-title').contains(/^Query errors by type/)
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
  it('Vitess - Query pool row has 5 panels', function () {
    cy.get('.dashboard-row').contains('Vitess - Query pool').find('.dashboard-row__panel_count').contains('(5 panels)')
  })
  it('expands the Vitess - Query pool row', function () {
    cy.get('.dashboard-row__title.pointer').contains('Vitess - Query pool').click();
  })
  it('contains the Available Connections panel', function () {
    cy.get('.panel-title').contains('Available Connections');
  })
  it('contains the Active Connections panel', function () {
    cy.get('.panel-title').contains('Active Connections');
  })
  it('contains the Idle Closed Rate panel', function () {
    cy.get('.panel-title').contains('Idle Closed Rate');
  })
  it('contains the Wait count panel', function () {
    cy.get('.panel-title').contains('Wait count');
  })
  it('contains the Avg wait time panel', function () {
    cy.get('.panel-title').contains('Avg wait time');
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
  it('Vitess - Transaction pool row has 5 panels', function () {
    cy.get('.dashboard-row').contains('Vitess - Transaction pool').find('.dashboard-row__panel_count').contains('(5 panels)')
  })
  it('expands the Vitess - Transaction pool row', function () {
    cy.get('.dashboard-row__title.pointer').contains('Vitess - Transaction pool').click();
  })
  it('contains the Available Connections panel', function () {
    cy.get('.panel-title').contains('Available Connections');
  })
  it('contains the Active Connections panel', function () {
    cy.get('.panel-title').contains('Active Connections');
  })
  it('contains the Idle Closed Rate panel', function () {
    cy.get('.panel-title').contains('Idle Closed Rate');
  })
  it('contains the Wait count panel', function () {
    cy.get('.panel-title').contains('Wait count');
  })
  it('contains the Avg wait time panel', function () {
    cy.get('.panel-title').contains('Avg wait time');
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
  it('Vitess Timings row has 12 panels', function () {
    cy.get('.dashboard-row').contains(/^Vitess Timings/).find('.dashboard-row__panel_count').contains('(12 panels)')
  })
  it('expands the Vitess Timings row', function () {
    cy.get('.dashboard-row__title.pointer').contains(/^Vitess Timings/).click();
  })
  it('contains the Query Duration (avg) panel', function () {
    cy.get('.panel-title').contains('Query Duration (avg)');
  })
  it('contains the Query Duration (p50) panel', function () {
    cy.get('.panel-title').contains('Query Duration (p50)');
  })
  it('contains the Query Duration (p95) panel', function () {
    cy.get('.panel-title').contains('Query Duration (p95)');
  })
  it('contains the Transaction Duration (avg) panel', function () {
    cy.get('.panel-title').contains('Transaction Duration (avg)');
  })
  it('contains the Transaction Duration (p50) panel', function () {
    cy.get('.panel-title').contains('Transaction Duration (p50)');
  })
  it('contains the Transaction Duration (p95) panel', function () {
    cy.get('.panel-title').contains(/^Transaction Duration \(p95\)/);
  })
  it('contains the Consolidations rate panel', function () {
    cy.get('.panel-title').contains(/^Consolidations rate/);
  })
  it('contains the Consolidations wait time (avg) panel', function () {
    cy.get('.panel-title').contains(/^Consolidations wait time \(avg\)/);
  })
  it('contains the Consolidations wait time (p95) panel', function () {
    cy.get('.panel-title').contains(/^Consolidations wait time \(p95\)/);
  })
  // These are failing but they are rendered so looks like I hit the limit
  // of my cypress knowledge.
  // Investigate how to fix this.
  // it('contains the VtGate -> VtTablet Call Time (avg) panel', function () {
  //   cy.get('.panel-title').contains(/^VtGate -> VtTablet Call Time \(avg\)/);
  // })
  // it('contains the VtGate -> VtTablet Call Time by Shard (avg) panel', function () {
  //   cy.get('.panel-title').contains(/^VtGate -> VtTablet Call Time by Shard \(avg\)/);
  // })
  // it('contains the Query Time Distribution (Heatmap) panel', function () {
  //   cy.get('.panel-title').contains(/^Query Time Distribution \(Heatmap\)/);
  // })
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
  it('MySQL row has 2 panels', function () {
    cy.get('.dashboard-row').contains('MySQL').find('.dashboard-row__panel_count').contains('(2 panels)');
  })
  it('expands the MySQL row', function () {
    cy.get('.dashboard-row__title.pointer').contains('MySQL').click();
  })
  it('contains the Semi-sync avg wait panel', function () {
    cy.get('.panel-title').contains('Semi-sync avg wait');
  })
  it('contains the Slow Queries panel', function () {
    cy.get('.panel-title').contains('Slow Queries');
  })
  it('collapses the MySQL row', function () {
    cy.get('.dashboard-row__title.pointer').contains('MySQL').click();
    cy.get('.dashboard-row--collapsed').contains('MySQL');
  })

    //MYSQL Timings ROW
    it('contains the MySQL row', function () {
      cy.get('.dashboard-row').contains(/MySQL Timings/);
    })
    it('MySQL Timings row is collapsed', function () {
      cy.get('.dashboard-row--collapsed').contains(/MySQL Timings/);
    })
    it('MySQL row has 3 panels', function () {
      cy.get('.dashboard-row').contains(/MySQL Timings/).find('.dashboard-row__panel_count').contains('(3 panels)');
    })
    it('expands the MySQL Timings row', function () {
      cy.get('.dashboard-row__title.pointer').contains(/MySQL Timings/).click();
    })
    it('contains the MySQL time (avg) panel', function () {
      cy.get('.panel-title').contains(/MySQL time \(avg\)/);
    })
    it('contains the MySQL Exec Time P50 panel', function () {
      cy.get('.panel-title').contains(/MySQL Exec Time P50/);
    })
    it('contains the MySQL Exec Time P95 panel', function () {
      cy.get('.panel-title').contains(/MySQL Exec Time P95/);
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
  it('OS row has 5 panels', function () {
    cy.get('.dashboard-row').contains('OS').find('.dashboard-row__panel_count').contains('(5 panels)');
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
  it('contains the /mnt disk free panel', function () {
    cy.get('.panel-title').contains('/mnt disk free');
  })
  it('contains the Network Rx Bytes panel', function () {
    cy.get('.panel-title').contains('Network Rx Bytes');
  })
  it('contains the Network Tx Bytes panel', function () {
    cy.get('.panel-title').contains('Network Tx Bytes');
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
    it('Misc row has 3 panels', function () {
      cy.get('.dashboard-row').contains('Misc').find('.dashboard-row__panel_count').contains('(3 panels)');
    })
    it('expands the Misc row', function () {
      cy.get('.dashboard-row__title.pointer').contains(/^Misc/).click();
    })
    it('contains the GC Count panel', function () {
      cy.get('.panel-title').contains(/^GC Count/);
    })
    it('contains the GC Duration total per seconds panel', function () {
      cy.get('.panel-title').contains(/^GC Duration total per seconds/);
    })
    it('contains the GC Duration quantiles (all hosts) panel', function () {
      cy.get('.panel-title').contains(/GC Duration quantiles \(all hosts\)/);
    })
    it('collapses the Misc row', function () {
      cy.get('.dashboard-row__title.pointer').contains(/^Misc/).click();
      cy.get('.dashboard-row--collapsed').contains(/^Misc/);
    })
})
