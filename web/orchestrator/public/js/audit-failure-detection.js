$(document).ready(function() {
  showLoader();
  var apiUri = "/api/audit-failure-detection/" + currentPage();
  if (clusterAlias() != "") {
    apiUri = "/api/audit-failure-detection/alias/" + clusterAlias() + "/" + currentPage();
  } else if (detectionId() > 0) {
    apiUri = "/api/audit-failure-detection/id/" + detectionId();
  }
  $.get(appUrl(apiUri), function(auditEntries) {
    auditEntries = auditEntries || [];
    $.get(appUrl("/api/replication-analysis-changelog"), function(analysisChangelog) {
      analysisChangelog = analysisChangelog || [];
      displayAudit(auditEntries, analysisChangelog);
    }, "json");
  }, "json");

  function displayAudit(auditEntries, analysisChangelog) {
    var baseWebUri = appUrl("/web/audit-failure-detection/");
    if (clusterAlias()) {
      baseWebUri += "alias/" + clusterAlias() + "/";
    }
    var changelogMap = {}
    analysisChangelog.forEach(function(changelogEntry) {
      changelogMap[getInstanceId(changelogEntry.AnalyzedInstanceKey.Hostname, changelogEntry.AnalyzedInstanceKey.Port)] = changelogEntry.Changelog;
    });

    hideLoader();
    auditEntries.forEach(function(audit) {
      var analyzedInstanceDisplay = audit.AnalysisEntry.AnalyzedInstanceKey.Hostname + ":" + audit.AnalysisEntry.AnalyzedInstanceKey.Port;
      var row = $('<tr/>');
      var analysisElement = $('<a class="more-detection-info"/>').attr("data-detection-id", audit.Id).text(audit.AnalysisEntry.Analysis);

      $('<td/>').prepend(analysisElement).appendTo(row);
      $('<a/>', {
        text: analyzedInstanceDisplay,
        href: appUrl("/web/search/" + analyzedInstanceDisplay)
      }).wrap($("<td/>")).parent().appendTo(row);
      $('<td/>', {
        text: audit.AnalysisEntry.CountReplicas
      }).appendTo(row);
      $('<a/>', {
        text: audit.AnalysisEntry.ClusterDetails.ClusterName,
        href: appUrl("/web/cluster/" + audit.AnalysisEntry.ClusterDetails.ClusterName)
      }).wrap($("<td/>")).parent().appendTo(row);
      $('<a/>', {
        text: audit.AnalysisEntry.ClusterDetails.ClusterAlias,
        href: appUrl("/web/cluster/alias/" + audit.AnalysisEntry.ClusterDetails.ClusterAlias)
      }).wrap($("<td/>")).parent().appendTo(row);
      $('<td/>', {
        text: audit.RecoveryStartTimestamp
      }).appendTo(row);

      var moreInfo = "";
      moreInfo += '<div>Detected: ' + audit.RecoveryStartTimestamp + '</div>';
      if (audit.AnalysisEntry.Replicas.length > 0) {
        moreInfo += '<div>' + audit.AnalysisEntry.CountReplicas + ' replicating hosts :<ul>';
        audit.AnalysisEntry.Replicas.forEach(function(instanceKey) {
          moreInfo += "<li><code>" + getInstanceTitle(instanceKey.Hostname, instanceKey.Port) + "</code></li>";
        });
        moreInfo += "</ul></div>";
      }
      var changelog = changelogMap[getInstanceId(audit.AnalysisEntry.AnalyzedInstanceKey.Hostname, audit.AnalysisEntry.AnalyzedInstanceKey.Port)];
      if (changelog) {
        moreInfo += '<div>Changelog :<ul>';
        changelog.reverse().forEach(function(changelogEntry) {
          var changelogEntryTokens = changelogEntry.split(';');
          var changelogEntryTimestamp = changelogEntryTokens[0];
          var changelogEntryAnalysis = changelogEntryTokens[1];

          if (changelogEntryTimestamp > audit.RecoveryStartTimestamp) {
            // This entry is newer than the detection time; irrelevant
            return;
          }
          moreInfo += "<li><code>" + changelogEntryTimestamp + " <strong>" + changelogEntryAnalysis + "</strong></code></li>";
        });
        moreInfo += "</ul></div>";
      }
      moreInfo += '<div><a href="' + appUrl('/web/audit-recovery/id/' + audit.RelatedRecoveryId) + '">Related recovery</a></div>';

      moreInfo += "<div>Processed by <code>" + audit.ProcessingNodeHostname + "</code></div>";
      row.appendTo('#audit tbody');

      var row = $('<tr/>');
      row.attr("data-detection-id-more-info", audit.Id);
      row.addClass("more-info");
      $('<td colspan="6"/>').append(moreInfo).appendTo(row);
      row.hide().appendTo('#audit tbody');
    });
    if (auditEntries.length == 1) {
      $("[data-detection-id-more-info]").show();
    }
    if (currentPage() <= 0) {
      $("#audit .pager .previous").addClass("disabled");
    }
    if (auditEntries.length == 0) {
      $("#audit .pager .next").addClass("disabled");
    }
    $("#audit .pager .previous").not(".disabled").find("a").click(function() {
      window.location.href = appUrl(baseWebUri + (currentPage() - 1));
    });
    $("#audit .pager .next").not(".disabled").find("a").click(function() {
      window.location.href = appUrl(baseWebUri + (currentPage() + 1));
    });
    $("#audit .pager .disabled a").click(function() {
      return false;
    });
    $("body").on("click", ".more-detection-info", function(event) {
      var detectionId = $(event.target).attr("data-detection-id");
      $('[data-detection-id-more-info=' + detectionId + ']').slideToggle();
    });
  }
});
