$(document).ready(function() {
  showLoader();

  var problemsURI = "/api/problems";
  if (typeof currentClusterName != "undefined") {
    problemsURI += "/" + currentClusterName();
  }
  $.get(appUrl(problemsURI), function(instances) {
    instances = instances || [];
    $.get(appUrl("/api/maintenance"), function(maintenanceList) {
      maintenanceList = maintenanceList || [];
      normalizeInstances(instances, maintenanceList);
      displayProblemInstances(instances);
    }, "json");
  }, "json");

  function displayProblemInstances(instances) {
    hideLoader();

    if (isAnonymized()) {
      $("#instance_problems").remove();
      return;
    }

    function SortByProblemOrder(instance0, instance1) {
      var orderDiff = instance0.problemOrder - instance1.problemOrder;
      if (orderDiff != 0) return orderDiff;
      var orderDiff = instance1.ReplicationLagSeconds.Int64 - instance0.ReplicationLagSeconds.Int64;
      if (orderDiff != 0) return orderDiff;
      orderDiff = instance0.title.localeCompare(instance1.title);
      if (orderDiff != 0) return orderDiff;
      return 0;
    }
    instances.sort(SortByProblemOrder);

    var countProblemInstances = 0;
    instances.forEach(function(instance) {
      var considerInstance = instance.hasProblem
      if (countProblemInstances >= 1000) {
        considerInstance = false;
      }
      if (considerInstance) {
        var li = $("<li/>");
        var instanceEl = Instance.createElement(instance).addClass("instance-problem").appendTo(li);
        $("#instance_problems ul").append(li);

        renderInstanceElement(instanceEl, instance, "problems"); //popoverElement
        instanceEl.click(function() {
          openNodeModal(instance);
          return false;
        });
        if (countProblemInstances == 0) {
          // First problem instance
          $("#instance_problems_button").addClass("btn-" + instance.renderHint)
        }
        countProblemInstances += 1;
      }
    });
    if (countProblemInstances > 0 && (autoshowProblems() == "true") && ($.cookie("anonymize") != "true")) {
      $("#instance_problems .dropdown-toggle").dropdown('toggle');
    }
    if (countProblemInstances == 0) {
      $("#instance_problems").hide();
    }

    $("div.popover").popover();
    $("div.popover").show();
  }
});
