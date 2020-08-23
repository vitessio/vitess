
$(document).ready(function () {
    showLoader();
    var apiUri = "/api/audit/"+currentPage();
    if (auditHostname()) {
    	apiUri = "/api/audit/instance/"+auditHostname()+"/"+auditPort()+"/"+currentPage();
    }
    $.get(appUrl(apiUri), function (auditEntries) {
      auditEntries = auditEntries || [];
      displayAudit(auditEntries);
  	}, "json");
    function displayAudit(auditEntries) {
    	var baseWebUri = appUrl("/web/audit/");
    	if (auditHostname()) {
    		baseWebUri += "instance/"+auditHostname()+"/"+auditPort()+"/";
        }
        hideLoader();
        auditEntries.forEach(function (audit) {
      		var row = jQuery('<tr/>');
      		jQuery('<td/>', { text: audit.AuditTimestamp }).appendTo(row);
      		jQuery('<td/>', { text: audit.AuditType }).appendTo(row);
      		if (audit.AuditInstanceKey.Hostname) {
      			var uri = appUrl("/web/audit/instance/"+audit.AuditInstanceKey.Hostname+"/"+audit.AuditInstanceKey.Port);
      			$('<a/>',  { text: audit.AuditInstanceKey.Hostname+":"+audit.AuditInstanceKey.Port , href: uri}).wrap($("<td/>")).parent().appendTo(row);
      		} else {
      			jQuery('<td/>', { text: audit.AuditInstanceKey.Hostname+":"+audit.AuditInstanceKey.Port }).appendTo(row);
      		}
      		jQuery('<td/>', { text: audit.Message }).appendTo(row);
      		row.appendTo('#audit tbody');
      	});
        if (currentPage() <= 0) {
        	$("#audit .pager .previous").addClass("disabled");
        }
        if (auditEntries.length == 0) {
        	$("#audit .pager .next").addClass("disabled");
        }
        $("#audit .pager .previous").not(".disabled").find("a").click(function() {
            window.location.href = baseWebUri+(currentPage() - 1);
        });
        $("#audit .pager .next").not(".disabled").find("a").click(function() {
            window.location.href = baseWebUri+(currentPage() + 1);
        });
        $("#audit .pager .disabled a").click(function() {
            return false;
        });
    }
});
