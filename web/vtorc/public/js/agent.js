$(document).ready(function() {
  showLoader();

  var hasActiveSeeds = false;

  $.get(appUrl("/api/agent/" + currentAgentHost()), function(agent) {
    showLoader();
    agent.AvailableLocalSnapshots || (agent.AvailableLocalSnapshots = [])
    agent.AvailableSnapshots || (agent.AvailableSnapshots = [])
    displayAgent(agent);
  }, "json");

  $.get(appUrl("/api/agent-active-seeds/" + currentAgentHost()), function(activeSeeds) {
    showLoader();
    activeSeeds.forEach(function(activeSeed) {
      appendSeedDetails(activeSeed, "[data-agent=active_seeds]");
    });
    if (activeSeeds.length == 0) {
      $("div.active_seeds").parent().hide();
      $("div.seed_states").parent().hide();
    }
    if (activeSeeds.length > 0) {
      hasActiveSeeds = true;
      activateRefreshTimer();

      $.get(appUrl("/api/agent-seed-states/" + activeSeeds[0].SeedId), function(seedStates) {
        showLoader();
        seedStates.forEach(function(seedState) {
          appendSeedState(seedState);
        });
      }, "json");
    }
  }, "json");
  $.get(appUrl("/api/agent-recent-seeds/" + currentAgentHost()), function(recentSeeds) {
    showLoader();
    recentSeeds.forEach(function(recentSeed) {
      appendSeedDetails(recentSeed, "[data-agent=recent_seeds]");
    });
    if (recentSeeds.length == 0) {
      $("div.recent_seeds").parent().hide();
    }
  }, "json");

  function displayAgent(agent) {
    if (!agent.Hostname) {
      $("[data-agent=hostname]").html('<span class="alert-danger">Not found</span>');
      return;
    }
    $("[data-agent=hostname]").html(agent.Hostname)
    $("[data-agent=hostname_search]").html(
      '<a href="' + appUrl('/web/search?s=' + agent.Hostname + ':' + agent.MySQLPort) + '">' + agent.Hostname + '</a>' + '<div class="pull-right"><button class="btn btn-xs btn-success" data-command="discover" data-hostname="' + agent.Hostname + '" data-mysql-port="' + agent.MySQLPort + '">Discover</button></div>'
    );
    $("[data-agent=port]").html(agent.Port)
    $("[data-agent=last_submitted]").html(agent.LastSubmitted)

    var mySQLStatus = "" + agent.MySQLRunning + '<div class="pull-right">' +
      (agent.MySQLRunning ? '<button class="btn btn-xs btn-danger" data-command="mysql-stop">Stop</button>' :
        '<button class="btn btn-xs btn-success" data-command="mysql-start">Start</button>') +
      '</div>';
    $("[data-agent=mysql_running]").html(mySQLStatus)
    $("[data-agent=mysql_port]").html(agent.MySQLPort)
    $("[data-agent=mysql_disk_usage]").html(toHumanFormat(agent.MySQLDiskUsage))

    if (agent.MySQLErrorLogTail != null && agent.MySQLErrorLogTail.length > 0) {
      rows = agent.MySQLErrorLogTail;
      rows = rows.map(function(row) {
        if (row.trim() == "") {
          row = "[empty line]"
        }
        return row;
      });
      $("[data-agent=mysql_error_log_tail]").html(rows[rows.length - 1])
      $("body").on("click", "a[data-agent=mysql_error_log_tail]", function(event) {
        rows = rows.map(function(row) {
          return '<strong>' + row + '</strong>';
        });
        rows = rows.map(function(row) {
          if (row.indexOf("[ERROR]") >= 0) {
            return '<code class="text-danger">' + row + '</code>';
          } else if (row.indexOf("[Warning]") >= 0) {
            return '<code class="text-warning">' + row + '</code>';
          } else if (row.indexOf("[Note]") >= 0) {
            return '<code class="text-info">' + row + '</code>';
          } else {
            return '<code class="text-primary">' + row + '</code>';
          }
        });
        bootbox.alert('<div style="overflow: auto">' + rows.join("<br/>") + '</div>');
        return false;
      });
    }

    function beautifyAvailableSnapshots(hostnames) {
      var result = hostnames.filter(function(hostname) {
        return hostname.trim() != "";
      });
      result = result.map(function(hostname) {
        if (hostname == agent.Hostname) {
          return '<td><span class="">' + hostname + '</span></td><td></td>';
        }
        var isLocal = $.inArray(hostname, agent.AvailableLocalSnapshots) >= 0;
        var btnType = (isLocal ? "btn-success" : "btn-warning");
        return '<td><a href="' + appUrl('/web/agent/' + hostname) + '">' + hostname + '</a><div class="pull-right"><button class="btn btn-xs ' + btnType + '" data-command="seed" data-seed-source-host="' + hostname + '" data-seed-local="' + isLocal + '" data-mysql-running="' + agent.MySQLRunning + '">Seed</button></div></td>';
      });
      result = result.map(function(entry) {
        return '<tr>' + entry + '</tr>';
      });
      return result;
    }
    beautifyAvailableSnapshots(agent.AvailableLocalSnapshots).forEach(function(entry) {
      $("[data-agent=available_local_snapshots]").append(entry)
    });
    availableRemoteSnapshots = agent.AvailableSnapshots.filter(function(snapshot) {
      return agent.AvailableLocalSnapshots.indexOf(snapshot) < 0;
    });
    beautifyAvailableSnapshots(availableRemoteSnapshots).forEach(function(entry) {
      $("[data-agent=available_remote_snapshots]").append(entry)
    });

    var mountedVolume = ""
    if (agent.MountPoint) {
      if (agent.MountPoint.IsMounted) {
        mountedVolume = agent.MountPoint.LVPath;
        var mountMessage = '<td>';
        mountMessage += '<code>' + mountedVolume + '</code> mounted on ' +
          '<code>' + agent.MountPoint.Path + '</code>, size ' + toHumanFormat(agent.MountPoint.DiskUsage);
        mountMessage += '<br/>MySQL data path: <code>' + agent.MountPoint.MySQLDataPath + '</code>';
        mountMessage += '</td><td><div class="pull-right"><button class="btn btn-xs btn-danger" data-command="unmount">Unmount</button></div></td>';
        $("[data-agent=mount_point]").append(mountMessage);
      }
    }

    if (agent.LogicalVolumes) {
      var lvSnapshots = agent.LogicalVolumes.filter(function(logicalVolume) {
        return logicalVolume.IsSnapshot;
      }).map(function(logicalVolume) {
        return logicalVolume.Path;
      });
      var result = lvSnapshots.map(function(volume) {
        var volumeText = '';
        var volumeTextType = 'text-info';
        if (volume == mountedVolume) {
          volumeText = '<button class="btn btn-xs btn-danger" data-command="unmount">Unmount</button>';
          volumeTextType = 'text-success';
        } else if (!(agent.MountPoint && agent.MountPoint.IsMounted)) {
          volumeText += '<button class="btn btn-xs btn-success" data-command="mountlv" data-lv="' + volume + '">Mount</button>'
          volumeText += ' <button class="btn btn-xs btn-danger" data-command="removelv" data-lv="' + volume + '">Remove</button>'
        } else {
          // Do nothing
        }
        volumeText = '<td><code class="' + volumeTextType + '"><strong>' + volume + '</strong></code><div class="pull-right">' + volumeText + '</div></td>';
        return volumeText;
      });
      result = result.map(function(entry) {
        return '<tr>' + entry + '</tr>';
      });

      result.forEach(function(entry) {
        $("[data-agent=lv_snapshots]").append(entry)
      });
    }

    hideLoader();
  }


  $("body").on("click", "button[data-command=unmount]", function(event) {
    if (hasActiveSeeds) {
      addAlert("This agent participates in an active seed; please await or abort active seed before unmounting");
      return;
    }
    showLoader();
    $.get(appUrl("/api/agent-umount/" + currentAgentHost()), function(operationResult) {
      hideLoader();
      if (operationResult.Code == "ERROR") {
        addAlert(operationResult.Message)
      } else {
        location.reload();
      }
    }, "json");
  });
  $("body").on("click", "button[data-command=mountlv]", function(event) {
    var lv = $(event.target).attr("data-lv")
    showLoader();
    $.get(appUrl("/api/agent-mount/" + currentAgentHost() + "?lv=" + encodeURIComponent(lv)), function(operationResult) {
      hideLoader();
      if (operationResult.Code == "ERROR") {
        addAlert(operationResult.Message)
      } else {
        location.reload();
      }
    }, "json");
  });
  $("body").on("click", "button[data-command=removelv]", function(event) {
    var lv = $(event.target).attr("data-lv")
    var message = "Are you sure you wish to remove logical volume <code><strong>" + lv + "</strong></code>?";
    bootbox.confirm(message, function(confirm) {
      if (confirm) {
        showLoader();
        $.get(appUrl("/api/agent-removelv/" + currentAgentHost() + "?lv=" + encodeURIComponent(lv)), function(operationResult) {
          hideLoader();
          if (operationResult.Code == "ERROR") {
            addAlert(operationResult.Message)
          } else {
            location.reload();
          }
        }, "json");
      }
    });
  });
  $("body").on("click", "button[data-command=create-snapshot]", function(event) {
    var message = "Are you sure you wish to create a new snapshot on <code><strong>" +
      currentAgentHost() + "</strong></code>?";
    bootbox.confirm(message, function(confirm) {
      if (confirm) {
        showLoader();
        $.get(appUrl("/api/agent-create-snapshot/" + currentAgentHost()), function(operationResult) {
          hideLoader();
          if (operationResult.Code == "ERROR") {
            addAlert(operationResult.Message)
          } else {
            location.reload();
          }
        }, "json");
      }
    });
  });
  $("body").on("click", "button[data-command=mysql-stop]", function(event) {
    var message = "Are you sure you wish to shut down MySQL service on <code><strong>" +
      currentAgentHost() + "</strong></code>?";
    bootbox.confirm(message, function(confirm) {
      if (confirm) {
        showLoader();
        $.get(appUrl("/api/agent-mysql-stop/" + currentAgentHost()), function(operationResult) {
          hideLoader();
          if (operationResult.Code == "ERROR") {
            addAlert(operationResult.Message)
          } else {
            location.reload();
          }
        }, "json");
      }
    });
  });
  $("body").on("click", "button[data-command=mysql-start]", function(event) {
    showLoader();
    $.get(appUrl("/api/agent-mysql-start/" + currentAgentHost()), function(operationResult) {
      hideLoader();
      if (operationResult.Code == "ERROR") {
        addAlert(operationResult.Message)
      } else {
        location.reload();
      }
    }, "json");
  });
  $("body").on("click", "button[data-command=seed]", function(event) {
    if (hasActiveSeeds) {
      addAlert("This agent already participates in an active seed; please await or abort active seed");
      return;
    }
    if ($(event.target).attr("data-mysql-running") == "true") {
      addAlert("MySQL is running on this host. Please first stop the MySQL service");
      return;
    }
    var sourceHost = $(event.target).attr("data-seed-source-host");
    var isLocalSeed = ($(event.target).attr("data-seed-local") == "true");

    var message = "Are you sure you wish to destroy data on <code><strong>" +
      currentAgentHost() + "</strong></code> and seed from <code><strong>" +
      sourceHost + "</strong></code>?";
    if (isLocalSeed) {
      message += '<p/><span class="text-success">This seed is dc-local</span>';
    } else {
      message += '<p/><span class="text-danger">This seed is non-local and will require cross-DC data transfer!</span>';
    }

    bootbox.confirm(message, function(confirm) {
      if (confirm) {
        showLoader();
        $.get(appUrl("/api/agent-seed/" + currentAgentHost() + "/" + sourceHost), function(operationResult) {
          hideLoader();
          if (operationResult.Code == "ERROR") {
            addAlert(operationResult.Message)
          } else {
            location.reload();
          }
        }, "json");
      }
    });
  });
  $("body").on("click", "button[data-command=discover]", function(event) {
    var hostname = $(event.target).attr("data-hostname")
    var mySQLPort = $(event.target).attr("data-mysql-port")
    discover(hostname, mySQLPort)
  });

});
