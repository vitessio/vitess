$(document).ready(function() {
  var isExpanded = true;

  showLoader();

  $.get(appUrl("/api/cluster-pool-instances/" + currentClusterName()), function(clusterPoolInstances) {
    $.get(appUrl("/api/problems"), function(problemInstances) {
      problemInstances = problemInstances || [];
      var problemInstancesMap = normalizeInstances(problemInstances, []);
      displayClusterPoolInstances(clusterPoolInstances, problemInstances, problemInstancesMap);
    }, "json");
  }, "json");

  function sortByCountInstances(pool1, pool2) {
    var diff = pool2.instances.length - pool1.instances.length;
    if (diff != 0) {
      return diff;
    }
    return pool1.name.localeCompare(pool2.name);
  }

  function displayClusterPoolInstances(clusterPoolInstances, problemInstances, problemInstancesMap) {
    hideLoader();

    var poolsProblems = {};
    var pools = new Array();
    for (var pool in clusterPoolInstances.Details) {
      if (clusterPoolInstances.Details.hasOwnProperty(pool)) {
        poolsProblems[pool] = {};
        pools.push({
          name: pool,
          instances: clusterPoolInstances.Details[pool]
        });
      }
    }
    pools.sort(sortByCountInstances);

    function addInstancesBadge(poolName, count, badgeClass, title) {
      $("#pools [data-pool-name='" + poolName + "'].popover").find(".popover-content .pull-right").append('<span class="badge ' + badgeClass + '" title="' + title + '">' + count + '</span> ');
    }

    function incrementPoolProblems(poolName, problemType) {
      if (poolsProblems[poolName][problemType] > 0) {
        poolsProblems[poolName][problemType] = poolsProblems[poolName][problemType] + 1;
      } else {
        poolsProblems[poolName][problemType] = 1;
      }
    }

    function incrementPoolsProblems(instance, problemType) {
      if (!problemType) {
        return
      }
      if (typeof instance.problemHint === 'undefined') {
        instance.problemHint = problemType
      }
      pools.forEach(function(pool) {
        pool.instances.forEach(function(poolInstance) {
          if ((poolInstance.Hostname == instance.Key.Hostname) && (poolInstance.Port = instance.Key.Port)) {
            incrementPoolProblems(pool.name, problemType)
          }
        });
      });
    }
    problemInstances.forEach(function(instance) {
      incrementPoolsProblems(instance, instance.problem)
    });

    pools.forEach(function(pool) {
      $("#pools").append('<div xmlns="http://www.w3.org/1999/xhtml" class="popover instance right" data-pool-name="' + pool.name + '"><div class="arrow"></div><h3 class="popover-title"><div class="pull-left"><span>' + pool.name + '</span></div><div class="pull-right"></div>&nbsp;<br/>&nbsp;</h3><div class="popover-content"></div></div>');
      var popoverElement = $("#pools [data-pool-name='" + pool.name + "'].popover");

      var contentHtml = '' + '<div>Instances: <div class="pull-right"></div><div class="pool-instances-listing"></div></div>';
      popoverElement.find(".popover-content").html(contentHtml);
      addInstancesBadge(pool.name, pool.instances.length, "label-primary", "Total instances in pool");
      for (var problemType in poolsProblems[pool.name]) {
        addInstancesBadge(pool.name, poolsProblems[pool.name][problemType], errorMapping[problemType]["badge"], errorMapping[problemType]["description"]);
      }
      pool.instances.forEach(function(instance) {
        var instanceId = getInstanceId(instance.Hostname, instance.Port);
        var problemInstance = problemInstancesMap[instanceId];
        var instanceDisplay = instance.Hostname + ":" + instance.Port;
        if (typeof removeTextFromHostnameDisplay != "undefined" && removeTextFromHostnameDisplay()) {
          instanceDisplay = instanceDisplay.replace(removeTextFromHostnameDisplay(), '');
        }
        var instanceContent = "<div>";
        if (problemInstance && problemInstance.problemHint) {
          instanceContent += '<span class="badge ' + errorMapping[problemInstance.problemHint]["badge"] + '" title="' + errorMapping[problemInstance.problemHint]["description"] + '">&nbsp;</span> '
        }
        instanceContent += instanceDisplay;
        instanceContent += "</div>";
        popoverElement.find("div.pool-instances-listing").append(instanceContent);
      });
    });

    $("div.popover").popover();
    $("div.popover").show();

    if (pools.length == 0) {
      addAlert("No pools found");
    }
  }

  if (isAuthorizedForAction()) {
    // Read-only users don't get auto-refresh. Sorry!
    activateRefreshTimer();
  }
  $("#dropdown-context").append('<li><a data-command="expand-instances">Expand</a></li>');
  $("#dropdown-context").append('<li><a href="' + appUrl('/web/cluster/' + currentClusterName()) + '">Topology</a></li>');
  $("body").on("click", "a[data-command=expand-instances]", function(event) {
    isExpanded = !isExpanded;
    updateExpandedStatus();
  });

  function updateExpandedStatus() {
    if (isExpanded) {
      $("div.pool-instances-listing").removeClass('hidden');
      $("#dropdown-context a[data-command=expand-instances]").prepend('<span class="glyphicon glyphicon-ok"></span> ');
    } else {
      $("div.pool-instances-listing").addClass('hidden');
      $("#dropdown-context a[data-command=expand-instances] span").remove();
    }
  }

  updateExpandedStatus();
});
