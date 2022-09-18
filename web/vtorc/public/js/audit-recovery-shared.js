function auditRecoverySteps(uid, target) {
  var uri = "/api/audit-recovery-steps/" + uid;
  $.get(appUrl(uri), function(steps) {
    steps = steps || [];
    displayAuditSteps(steps, target);
  }, "json");

  function displayAuditSteps(steps, target) {
    hideLoader();
    steps.forEach(function(step) {
      var row = $('<tr/>');
      $('<td/>', {
        text: step.AuditAt
      }).appendTo(row);
      $('<td/>', {
        text: step.Message
      }).appendTo(row);

      row.appendTo(target);
    });
  }
}
