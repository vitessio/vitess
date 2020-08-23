$(document).ready(function() {
  showLoader();

  $.get(appUrl("/api/clusters-info"), function(clusters) {
    clusters = clusters || [];
    $.get(appUrl("/api/replication-analysis"), function(replicationAnalysis) {
      $.get(appUrl("/api/blocked-recoveries"), function(blockedRecoveries) {
        blockedRecoveries = blockedRecoveries || [];
        displayClustersAnalysis(clusters, replicationAnalysis, blockedRecoveries);
      }, "json");
    }, "json");
  }, "json");
  $.get(appUrl("/api/blocked-recoveries"), function(blockedRecoveries) {
    blockedRecoveries = blockedRecoveries || [];
    // Result is an array: either empty (no active recovery) or with multiple entries
    blockedRecoveries.forEach(function(blockedRecovery) {
      addAlert('A <strong>' + blockedRecovery.Analysis + '</strong> on ' + getInstanceTitle(blockedRecovery.FailedInstanceKey.Hostname, blockedRecovery.FailedInstanceKey.Port) + ' is blocked due to a <a href="' + appUrl('/web/audit-recovery/id/' + blockedRecovery.BlockingRecoveryId) + '">previous recovery</a>');
    });
  });

  function sortByCountInstances(cluster1, cluster2) {
    if (cluster2.allAnalysisDowntimed && !cluster1.allAnalysisDowntimed) {
      return 1
    }
    if (cluster1.allAnalysisDowntimed && !cluster2.allAnalysisDowntimed) {
      return 1
    }
    var diff = cluster2.CountInstances - cluster1.CountInstances;
    if (diff != 0) {
      return diff;
    }
    return cluster1.ClusterName.localeCompare(cluster2.ClusterName);
  }

  function getBlockedRecoveryKey(hostname, port, analysis) {
    return hostname + ":" + port + ":" + analysis;
  }

  function displayClustersAnalysis(clusters, replicationAnalysis, blockedRecoveries) {
    hideLoader();

    var clustersMap = {};
    clusters.forEach(function(cluster) {
      cluster.analysisEntries = Array();
      cluster.allAnalysisDowntimed = true;
      clustersMap[cluster.ClusterName] = cluster;
    });

    // Apply/associate analysis to clusters
    replicationAnalysis.Details.forEach(function(analysisEntry) {
      if (analysisEntry.Analysis in interestingAnalysis) {
        clustersMap[analysisEntry.ClusterDetails.ClusterName].analysisEntries.push(analysisEntry);
        if (!analysisEntry.IsDowntimed) {
          clustersMap[analysisEntry.ClusterDetails.ClusterName].allAnalysisDowntimed = false;
        }
      }
      analysisEntry.StructureAnalysis = analysisEntry.StructureAnalysis || [];
      analysisEntry.StructureAnalysis.forEach(function(structureAnalysis) {
        analysisEntry.Analysis = structureAnalysis;
        analysisEntry.IsStructureAnalysis = true;
        clustersMap[analysisEntry.ClusterDetails.ClusterName].analysisEntries.push(analysisEntry);
      });
    });
    // Only keep clusters with some analysis (the rest are fine, no need to include them)
    clusters = clusters.filter(function(cluster) {
      return (cluster.analysisEntries.length > 0);
    });


    var blockedrecoveriesMap = {}
    blockedRecoveries.forEach(function(blockedRecovery) {
      var blockedKey = getBlockedRecoveryKey(blockedRecovery.FailedInstanceKey.Hostname, blockedRecovery.FailedInstanceKey.Port, blockedRecovery.Analysis);
      blockedrecoveriesMap[blockedKey] = true;
    });

    function displayInstancesBadge(popoverElement, text, count, badgeClass, title) {
      popoverElement.find(".popover-content>div").append('<div>' + text + ':<div class="pull-right"><span class="badge ' + badgeClass + '" title="' + title + '">' + count + '</span></div></div>');
    }

    function displayAnalysisEntry(analysisEntry, popoverElement) {
      var blockedKey = getBlockedRecoveryKey(analysisEntry.AnalyzedInstanceKey.Hostname, analysisEntry.AnalyzedInstanceKey.Port, analysisEntry.Analysis);
      var displayText = '<hr/><span><strong>' + analysisEntry.Analysis + (analysisEntry.IsDowntimed ? '<br/>[<i>downtime till ' + analysisEntry.DowntimeEndTimestamp + '</i>]' : '') + (blockedrecoveriesMap[blockedKey] ? '<br/><span class="glyphicon glyphicon-exclamation-sign text-danger"></span> Blocked' : '') + "</strong></span>" + "<br/>" + "<span>" + analysisEntry.AnalyzedInstanceKey.Hostname + ":" + analysisEntry.AnalyzedInstanceKey.Port + "</span>";
      if (analysisEntry.IsDowntimed) {
        displayText = '<div class="downtimed">' + displayText + '</div>';
      } else if (blockedrecoveriesMap[blockedKey]) {
        displayText = '<div class="blocked">' + displayText + '</div>';
      }
      popoverElement.find(".popover-content>div").append('<div class="divider"></div><div>' + displayText + '</div> ');
      if (analysisEntry.IsStructureAnalysis) {
        displayInstancesBadge(popoverElement, "Participating replicas", analysisEntry.CountReplicas, "label-warning", "Replicas having structural issue");
      } else {
        displayInstancesBadge(popoverElement, "Affected replicas", analysisEntry.CountReplicas, "label-danger", "Direct replicas of failing instance");
      }
    }

    function displayCluster(cluster) {
      $("#clusters_analysis").append('<div xmlns="http://www.w3.org/1999/xhtml" class="popover instance right" data-cluster-name="' + cluster.ClusterName + '"><div class="arrow"></div><h3 class="popover-title"><div class="pull-left"><a href="' + appUrl('/web/cluster/' + cluster.ClusterName) + '"><span>' + cluster.ClusterName + '</span></a></div><div class="pull-right"></div>&nbsp;<br/>&nbsp;</h3><div class="popover-content"><div></div></div></div>');
      var popoverElement = $("#clusters_analysis [data-cluster-name='" + cluster.ClusterName + "'].popover");

      if (typeof removeTextFromHostnameDisplay != "undefined" && removeTextFromHostnameDisplay()) {
        var title = cluster.ClusterName.replace(removeTextFromHostnameDisplay(), '');
        popoverElement.find("h3 .pull-left a span").html(title);
      }
      if (cluster.ClusterAlias != "") {
        popoverElement.find("h3 .pull-left a span").addClass("small");
        popoverElement.find("h3 .pull-left").prepend('<a href="' + appUrl('/web/cluster/alias/' + encodeURIComponent(cluster.ClusterAlias)) + '"><strong>' + cluster.ClusterAlias + '</strong></a><br/>');
        popoverElement.find("h3 .pull-right").append('<a href="' + appUrl('/web/cluster/alias/' + encodeURIComponent(cluster.ClusterAlias) + '?compact=true') + '"><span class="glyphicon glyphicon-compressed" title="Compact display"></span></a>');
      }
      displayInstancesBadge(popoverElement, "Instances", cluster.CountInstances, "label-primary", "Total instances in cluster");

      cluster.analysisEntries.forEach(function(analysisEntry) {
        displayAnalysisEntry(analysisEntry, popoverElement);
      });
    }

    clusters.sort(sortByCountInstances);
    clusters.forEach(function(cluster) {
      displayCluster(cluster);
    });

    if (clusters.length == 0) {
      // No problems
      var info = "No incidents which require a failover to report. Orchestrator reports the following incidents:<ul>";
      for (var analysis in interestingAnalysis) {
        if (interestingAnalysis[analysis]) {
          info += "<li>" + analysis + "</li>";
        }
      }
      info += "</ul>";
      addInfo(info);
    }

    $("div.popover").popover();
    $("div.popover").show();
  }

  if (isAuthorizedForAction()) {
    // Read-only users don't get auto-refresh. Sorry!
    activateRefreshTimer();
  }
});
