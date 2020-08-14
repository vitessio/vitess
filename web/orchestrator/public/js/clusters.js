$(document).ready(function() {
  showLoader();

  $.get(appUrl("/api/clusters-info"), function(clusters) {
    $.get(appUrl("/api/replication-analysis"), function(replicationAnalysis) {
      $.get(appUrl("/api/problems"), function(problemInstances) {
        if (problemInstances == null) {
          problemInstances = [];
        }
        normalizeInstances(problemInstances, []);
        displayClusters(clusters, replicationAnalysis, problemInstances);
      }, "json");
    }, "json");
  }, "json");

  function sortByCountInstances(cluster1, cluster2) {
    var diff = cluster2.CountInstances - cluster1.CountInstances;
    if (diff != 0) {
      return diff;
    }
    return cluster1.ClusterName.localeCompare(cluster2.ClusterName);
  }

  function sortByClusterName(cluster1, cluster2) {
    return cluster1.ClusterName.localeCompare(cluster2.ClusterName);
  }

  function sortByClusterAlias(cluster1, cluster2) {
    return cluster1.ClusterAlias.localeCompare(cluster2.ClusterAlias);
  }

  function displayClusters(clusters, replicationAnalysis, problemInstances) {
    hideLoader();

    clusters = clusters || [];

    var dashboardSort = $.cookie("dashboard-sort") || "count"

    refreshDashboardSortButton();
    $("#li-dashboard-sort").appendTo("ul.navbar-nav").show();
    $("#dashboard-sort a").click(function() {
      $.cookie("dashboard-sort", $(this).attr("dashboard-sort"), {
        path: '/',
        expires: 3650
      });
      location.reload();
    });

    var clustersProblems = {};
    clusters.forEach(function(cluster) {
      clustersProblems[cluster.ClusterName] = {};
    });

    var clustersAnalysisProblems = {};
    replicationAnalysis.Details.forEach(function(analysisEntry) {
      if (!clustersAnalysisProblems[analysisEntry.ClusterDetails.ClusterName]) {
        clustersAnalysisProblems[analysisEntry.ClusterDetails.ClusterName] = [];
      }
      if (analysisEntry.Analysis in interestingAnalysis) {
        clustersAnalysisProblems[analysisEntry.ClusterDetails.ClusterName].push(analysisEntry);
      }
      analysisEntry.StructureAnalysis = analysisEntry.StructureAnalysis || [];
      analysisEntry.StructureAnalysis.forEach(function(structureAnalysis) {
        analysisEntry.Analysis = structureAnalysis;
        analysisEntry.IsStructureAnalysis = true;
        clustersAnalysisProblems[analysisEntry.ClusterDetails.ClusterName].push(analysisEntry);
      });
    });

    function refreshDashboardSortButton() {
      if (dashboardSort == "name") {
        clusters.sort(sortByClusterName);
      } else if (dashboardSort == "alias") {
        clusters.sort(sortByClusterAlias);
      } else {
        clusters.sort(sortByCountInstances);
      }

      $("#dashboard-sort-button").html("Sort by " + dashboardSort + ' <span class="caret"></span>')
    }

    function addInstancesBadge(clusterName, count, badgeClass, title) {
      $("#clusters [data-cluster-name='" + clusterName + "'].popover").find(".popover-content .pull-right").append('<span class="badge ' + badgeClass + '" title="' + title + '">' + count + '</span> ');
    }

    function incrementClusterProblems(clusterName, problemType) {
      if (!problemType) {
        return
      }
      if (clustersProblems[clusterName][problemType] > 0) {
        clustersProblems[clusterName][problemType] = clustersProblems[clusterName][problemType] + 1;
      } else {
        clustersProblems[clusterName][problemType] = 1;
      }
    }
    problemInstances.forEach(function(instance) {
      incrementClusterProblems(instance.ClusterName, instance.problem)
    });

    clusters.forEach(function(cluster) {
      $("#clusters").append('<div xmlns="http://www.w3.org/1999/xhtml" class="popover instance right" data-cluster-name="' + cluster.ClusterName + '"><div class="arrow"></div><h3 class="popover-title"><div class="pull-left"><a href="' + appUrl('/web/cluster/' + cluster.ClusterName) + '"><span>' + cluster.ClusterName + '</span></a></div><div class="pull-right"></div>&nbsp;<br/>&nbsp;</h3><div class="popover-content"></div></div>');
      var popoverElement = $("#clusters [data-cluster-name='" + cluster.ClusterName + "'].popover");

      if (typeof removeTextFromHostnameDisplay != "undefined" && removeTextFromHostnameDisplay()) {
        var title = cluster.ClusterName.replace(removeTextFromHostnameDisplay(), '');
        popoverElement.find("h3 .pull-left a span").html(title);
      }
      var compactClusterUri = appUrl('/web/cluster/' + cluster.ClusterName + '?compact=true');
      if (cluster.ClusterAlias) {
        popoverElement.find("h3 .pull-left a span").addClass("small");
        popoverElement.find("h3 .pull-left").prepend('<a href="' + appUrl('/web/cluster/alias/' + encodeURIComponent(cluster.ClusterAlias)) + '"><strong>' + cluster.ClusterAlias + '</strong></a><br/>');
        compactClusterUri = appUrl('/web/cluster/alias/' + encodeURIComponent(cluster.ClusterAlias) + '?compact=true');
      }
      if (clustersAnalysisProblems[cluster.ClusterName]) {
        clustersAnalysisProblems[cluster.ClusterName].forEach(function(analysisEntry) {
          var analysisLabel = "text-danger";
          if (analysisEntry.IsStructureAnalysis) {
            analysisLabel = "text-warning";
          }
          var hasDowntime = analysisEntry.IsDowntimed || analysisEntry.IsReplicasDowntimed
          if (hasDowntime) {
            analysisLabel = "text-muted";
          }
          popoverElement.find("h3 .pull-left").prepend('<span class="glyphicon glyphicon-exclamation-sign ' + analysisLabel + '" title="' + analysisEntry.Analysis + ': ' + getInstanceTitle(analysisEntry.AnalyzedInstanceKey.Hostname, analysisEntry.AnalyzedInstanceKey.Port) + '"></span>');
        });

      }
      popoverElement.find("h3 .pull-right").append('<a href="' + compactClusterUri + '"><span class="glyphicon glyphicon-compressed" title="Compact display"></span></a>');
      if (cluster.HasAutomatedIntermediateMasterRecovery === true) {
        popoverElement.find("h3 .pull-right").prepend('<span class="glyphicon glyphicon-heart-empty text-info" title="Automated intermediate master recovery for this cluster ENABLED"></span>');
      }
      if (cluster.HasAutomatedMasterRecovery === true) {
        popoverElement.find("h3 .pull-right").prepend('<span class="glyphicon glyphicon-heart text-info" title="Automated master recovery for this cluster ENABLED"></span>');
      }

      var contentHtml = '' + '<div>Instances: <div class="pull-right"></div></div>';
      popoverElement.find(".popover-content").html(contentHtml);
      addInstancesBadge(cluster.ClusterName, cluster.CountInstances, "label-primary", "Total instances in cluster");
      for (var problemType in clustersProblems[cluster.ClusterName]) {
        addInstancesBadge(cluster.ClusterName, clustersProblems[cluster.ClusterName][problemType], errorMapping[problemType]["badge"], errorMapping[problemType]["description"]);
      }
    });

    $("div.popover").popover();
    $("div.popover").show();

    if (clusters.length == 0) {
      addAlert("No clusters found");
    }
  }

  if (isAuthorizedForAction()) {
    // Read-only users don't get auto-refresh. Sorry!
    activateRefreshTimer();
  }
});
