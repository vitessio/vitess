var refreshIntervalSeconds = 60; // seconds
var secondsTillRefresh = refreshIntervalSeconds;
var isReloadingPage = false;
var nodeModalVisible = false;

reloadPageHint = {
  hint: "",
  hostname: "",
  port: ""
}

function updateCountdownDisplay() {
  if ($.cookie("auto-refresh") == "true") {
    $("#refreshCountdown").html('<span class="glyphicon glyphicon-repeat" title="Click to pause"></span> ' + secondsTillRefresh + 's');
  } else {
    secondsTillRefresh = refreshIntervalSeconds;
    $("#refreshCountdown").html('<span class="glyphicon glyphicon-pause" title="Click to countdown"></span> ' + secondsTillRefresh + 's');
  }
}

function startRefreshTimer() {
  var refreshFunction = function() {
    if (nodeModalVisible) {
      return;
    }
    if (isReloadingPage) {
      return;
    }
    secondsTillRefresh = Math.max(secondsTillRefresh - 1, 0);
    if (secondsTillRefresh <= 0) {
      isReloadingPage = true;
      $(".navbar-nav li[data-nav-page=refreshCountdown]").addClass("active");
      showLoader();
      location.reload(true);
    }
    updateCountdownDisplay();
  }
  setInterval(refreshFunction, 1 * 1000);
}

function resetRefreshTimer() {
  secondsTillRefresh = refreshIntervalSeconds;
}

function activateRefreshTimer() {
  startRefreshTimer();
  $(document).click(function() {
    resetRefreshTimer();
  });
  $(document).mousemove(function() {
    resetRefreshTimer();
  });
}

function showLoader() {
  $(".ajaxLoader").css('visibility', 'visible');
}

function hideLoader() {
  $(".ajaxLoader").css('visibility', 'hidden');
}

function isAnonymized() {
  return ($.cookie("anonymize") == "true");
}

function isAliased() {
  return ($.cookie("alias") == "true");
}

function isSilentUI() {
  return ($.cookie("silent-ui") == "true");
}

function isCompactDisplay() {
  return ($.cookie("compact-display") == "true");
}

// origin: https://vanillajstoolkit.com/
/**
 * Sanitize and encode all HTML in a user-submitted string
 * https://portswigger.net/web-security/cross-site-scripting/preventing
 * @param  {String} str  The user-submitted string
 * @return {String} str  The sanitized string
 */
function sanitizeHTML (str) {
	return str.replace(/[^\w-_. ]/gi, function (c) {
		return '&#' + c.charCodeAt(0) + ';';
	});
}

function anonymizeInstanceId(instanceId) {
  var tokens = instanceId.split("__");
  return "instance-" + md5(tokens[1]).substring(0, 4) + ":" + tokens[2];
}

function appUrl(url) {
  // Create an absolute URL that respects URL-rewriting proxies,
  // such as the Kubernetes apiserver proxy.
  var here = window.location.pathname;
  var pos = here.lastIndexOf('/web/');
  if (pos < 0) {
    return url;
  }
  // This assumes the Orchestrator UI is accessed as ".../web/...".
  // The part before the "/web/" should be prefixed onto any URLs we write.
  return here.substr(0, pos) + url;
}

function visualizeBrand() {
  var img = $("<img>");

  img.attr("src", appUrl("/images/orchestrator-logo-32.png")).attr("alt", "GitHub");

  if (document.domain && document.domain.indexOf("outbrain.com") >= 0) {
    img.attr("src", appUrl("/images/outbrain-logo-32.png")).attr("alt", "Outbrain");
  }
  if (document.domain && document.domain.indexOf("booking.com") >= 0) {
    img.attr("src", appUrl("/images/booking-logo-32.png")).attr("alt", "Booking.com");
  }
  $(".orchestrator-brand").prepend(img)
}

function showContextMenu() {
  $("[data-nav-page=context]").css('visibility', 'visible');
}

function booleanString(b) {
  return (b ? "true" : "false");
}

function toHumanFormat(bytes) {
  var s = ['bytes', 'kB', 'MB', 'GB', 'TB', 'PB'];
  var e = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, e)).toFixed(2) + " " + s[e];
}

function getInstanceId(host, port) {
  return "instance__" + host.replace(/[.]/g, "_") + "__" + port
}


function canonizeInstanceTitle(title) {
  if (typeof removeTextFromHostnameDisplay != "undefined" && removeTextFromHostnameDisplay()) {
    return title.replace(removeTextFromHostnameDisplay(), '');
  }
  return title;
}

function getInstanceTitle(host, port) {
  if (host == "") {
    return "";
  }
  return canonizeInstanceTitle(host + ":" + port);
}



function addAlert(alertText, alertClass) {
  if (isAnonymized()) {
    return false;
  }
  if (typeof(alertClass) === 'undefined') {
    alertClass = "danger";
  }
  $("#alerts_container").append(
    '<div class="alert alert-' + alertClass + ' alert-dismissable">' + '<button type="button" class="close" data-dismiss="alert" aria-hidden="true">&times;</button>' + alertText + '</div>');
  $(".alert").alert();
  return false;
}


function addInfo(alertText) {
  return addAlert(alertText, "info");
}

function apiCommand(uri, hint) {
  showLoader();
  $.get(appUrl(uri), function(operationResult) {
    hideLoader();
    reloadWithOperationResult(operationResult, hint);
  }, "json").fail(function(operationResult) {
    hideLoader();
    if (operationResult.responseJSON.Code == "ERROR") {
      addAlert(operationResult.responseJSON.Message)
    }
  });
  return false;
}

function reloadWithMessage(msg, details, hint) {
  msg = msg || '';
  var hostname = "";
  var port = "";
  if (details) {
    hostname = details.Hostname || hostname
    port = details.Port || port
  }
  hint = hint || "";
  var newUri = window.location.href.split("#")[0].split("?")[0] + "?orchestrator-msg=" + encodeURIComponent(msg) + "&hostname=" + hostname + "&port=" + port + "&hint=" + hint;
  if (isCompactDisplay && isCompactDisplay()) {
    newUri += "&compact=true";
  }
  window.location.href = newUri;
}

function reloadWithOperationResult(operationResult, hint) {
  var msg = operationResult.Message;
  reloadWithMessage(msg, operationResult.Details, hint);
}


// Modal

function addNodeModalDataAttribute(name, value) {
  var codeClass = "text-primary";
  if (value == "true" || value == true) {
    codeClass = "text-success";
  }
  if (value == "false" || value === false) {
    codeClass = "text-danger";
  }
  if (name == "Maintenance") {
    codeClass = "text-danger";
  }
  $('#modalDataAttributesTable').append(
    '<tr><td>' + name + '</td><td><code class="' + codeClass + '"><strong>' + value + '</strong></code><div class="pull-right attributes-buttons"></div></td></tr>');
  return $('#modalDataAttributesTable tr:last td:last');
}

function addModalAlert(alertText) {
  $("#node_modal .modal-body").append(
    '<div class="alert alert-danger alert-dismissable">' + '<button type="button" class="close" data-dismiss="alert" aria-hidden="true">&times;</button>' + alertText + '</div>');
  $(".alert").alert();
  return false;
}

function openNodeModal(node) {
  if (!node) {
    return false;
  }
  if (node.isAggregate) {
    return false;
  }
  nodeModalVisible = true;
  var hiddenZone = $('#node_modal .hidden-zone');
  $('#node_modal #modalDataAttributesTable button[data-btn][data-grouped!=true]').appendTo("#node_modal .modal-footer");
  $('#node_modal #modalDataAttributesTable [data-btn-group]').appendTo("#node_modal .modal-footer");

  $('#node_modal .modal-title').html('<code class="text-primary">' + node.title + "</code>");

  $('#modalDataAttributesTable').html("");

  $('#node_modal button[data-btn=end-maintenance]').hide();
  if (node.inMaintenance) {
    td = addNodeModalDataAttribute("Maintenance", node.maintenanceReason);
    $('#node_modal button[data-btn=end-maintenance]').appendTo(td.find("div")).show();
  }

  if (node.InstanceAlias) {
    addNodeModalDataAttribute("Instance Alias", node.InstanceAlias);
  }
  addNodeModalDataAttribute("Last seen", node.LastSeenTimestamp + " (" + node.SecondsSinceLastSeen.Int64 + "s ago)");
  if (node.UnresolvedHostname) {
    addNodeModalDataAttribute("Unresolved hostname", node.UnresolvedHostname);
  }
  $('#node_modal [data-btn-group=move-equivalent]').appendTo(hiddenZone);
  if (node.MasterKey.Hostname) {
    var td = addNodeModalDataAttribute("Master", node.masterTitle);
    if (node.IsDetachedMaster) {
      $('#node_modal button[data-btn=reattach-replica-master-host]').appendTo(td.find("div"));
    } else {
      $('#node_modal button[data-btn=reattach-replica-master-host]').appendTo(hiddenZone);
    }
    $('#node_modal button[data-btn=reset-replica]').appendTo(td.find("div"))

    td = addNodeModalDataAttribute("Replication running", booleanString(node.replicationRunning));
    $('#node_modal button[data-btn=start-replica]').appendTo(td.find("div"))
    $('#node_modal button[data-btn=restart-replica]').appendTo(td.find("div"))
    $('#node_modal button[data-btn=stop-replica]').appendTo(td.find("div"))

    if (!node.replicationRunning) {
      if (node.LastSQLError) {
        td = addNodeModalDataAttribute("Last SQL error", node.LastSQLError);
        $('#node_modal button[data-btn=skip-query]').appendTo(td.find("div"))
      }
      if (node.LastIOError) {
        addNodeModalDataAttribute("Last IO error", node.LastIOError);
      }
    }
    addNodeModalDataAttribute("Seconds behind master", node.SecondsBehindMaster.Valid ? node.SecondsBehindMaster.Int64 : "null");
    addNodeModalDataAttribute("Replication lag", node.ReplicationLagSeconds.Valid ? node.ReplicationLagSeconds.Int64 : "null");
    addNodeModalDataAttribute("SQL delay", node.SQLDelay);

    var masterCoordinatesEl = addNodeModalDataAttribute("Master coordinates", node.ExecBinlogCoordinates.LogFile + ":" + node.ExecBinlogCoordinates.LogPos);
    $('#node_modal [data-btn-group=move-equivalent] ul').empty();
    $.get(appUrl("/api/master-equivalent/") + node.MasterKey.Hostname + "/" + node.MasterKey.Port + "/" + node.ExecBinlogCoordinates.LogFile + "/" + node.ExecBinlogCoordinates.LogPos, function(equivalenceResult) {
      if (!equivalenceResult.Details) {
        return false;
      }
      equivalenceResult.Details.forEach(function(equivalence) {
        if (equivalence.Key.Hostname == node.Key.Hostname && equivalence.Key.Port == node.Key.Port) {
          // This very instance; will not move below itself
          return;
        }
        var title = canonizeInstanceTitle(equivalence.Key.Hostname + ':' + equivalence.Key.Port);
        $('#node_modal [data-btn-group=move-equivalent] ul').append('<li><a href="#" data-btn="move-equivalent" data-hostname="' + equivalence.Key.Hostname + '" data-port="' + equivalence.Key.Port + '">' + title + '</a></li>');
      });

      if ($('#node_modal [data-btn-group=move-equivalent] ul li').length) {
        $('#node_modal [data-btn-group=move-equivalent]').appendTo(masterCoordinatesEl.find("div"));
      }
    }, "json");
    if (node.IsDetached) {
      $('#node_modal button[data-btn=detach-replica]').appendTo(hiddenZone)
      $('#node_modal button[data-btn=reattach-replica]').appendTo(masterCoordinatesEl.find("div"))
    } else {
      $('#node_modal button[data-btn=detach-replica]').appendTo(masterCoordinatesEl.find("div"))
      $('#node_modal button[data-btn=reattach-replica]').appendTo(hiddenZone)
    }
  } else {
    $('#node_modal button[data-btn=reset-replica]').appendTo(hiddenZone);
    $('#node_modal button[data-btn=reattach-replica-master-host]').appendTo(hiddenZone);
    $('#node_modal button[data-btn=skip-query]').appendTo(hiddenZone);
    $('#node_modal button[data-btn=detach-replica]').appendTo(hiddenZone)
    $('#node_modal button[data-btn=reattach-replica]').appendTo(hiddenZone)
  }
  if (node.LogBinEnabled) {
    addNodeModalDataAttribute("Self coordinates", node.SelfBinlogCoordinates.LogFile + ":" + node.SelfBinlogCoordinates.LogPos);
  }
  var td = addNodeModalDataAttribute("Num replicas", node.Replicas.length);
  $('#node_modal button[data-btn=regroup-replicas]').appendTo(td.find("div"))
  addNodeModalDataAttribute("Server ID", node.ServerID);
  if (node.ServerUUID) {
    addNodeModalDataAttribute("Server UUID", node.ServerUUID);
  }
  addNodeModalDataAttribute("Version", node.Version);
  var td = addNodeModalDataAttribute("Read only", booleanString(node.ReadOnly));
  $('#node_modal button[data-btn=set-read-only]').appendTo(td.find("div"))
  $('#node_modal button[data-btn=set-writeable]').appendTo(td.find("div"))

  addNodeModalDataAttribute("Has binary logs", booleanString(node.LogBinEnabled));
  if (node.LogBinEnabled) {
    var format = node.Binlog_format;
    if (format == 'ROW' && node.BinlogRowImage != '') {
      format = format + "/" + node.BinlogRowImage;
    }
    addNodeModalDataAttribute("Binlog format", format);
    var td = addNodeModalDataAttribute("Logs replication updates", booleanString(node.LogReplicationUpdatesEnabled));
    $('#node_modal button[data-btn=take-siblings]').appendTo(td.find("div"))
  }

  $('#node_modal [data-btn-group=gtid-errant-fix]').hide();
  addNodeModalDataAttribute("GTID supported", booleanString(node.supportsGTID));
  if (node.supportsGTID) {
    var td = addNodeModalDataAttribute("GTID based replication", booleanString(node.usingGTID));
    $('#node_modal button[data-btn=enable-gtid]').appendTo(td.find("div"))
    $('#node_modal button[data-btn=disable-gtid]').appendTo(td.find("div"))
    if (node.GTIDMode) {
      addNodeModalDataAttribute("GTID mode", node.GTIDMode);
    }
    if (node.ExecutedGtidSet) {
      addNodeModalDataAttribute("Executed GTID set", node.ExecutedGtidSet);
    }
    if (node.GtidPurged) {
      addNodeModalDataAttribute("GTID purged", node.GtidPurged);
    }
    if (node.GtidErrant) {
      td = addNodeModalDataAttribute("GTID errant", node.GtidErrant);
      $('#node_modal [data-btn-group=gtid-errant-fix]').appendTo(td.find("div"))
      $('#node_modal [data-btn-group=gtid-errant-fix]').show();
    }
  }
  addNodeModalDataAttribute("Semi-sync enforced", booleanString(node.SemiSyncEnforced));

  addNodeModalDataAttribute("Uptime", node.Uptime);
  addNodeModalDataAttribute("Allow TLS", node.AllowTLS);
  addNodeModalDataAttribute("Region", node.Region);
  addNodeModalDataAttribute("Data center", node.DataCenter);
  addNodeModalDataAttribute("Physical environment", node.PhysicalEnvironment);
  addNodeModalDataAttribute("Cluster",
    '<a href="' + appUrl('/web/cluster/' + node.ClusterName) + '">' + node.ClusterName + '</a>');
  addNodeModalDataAttribute("Audit",
    '<a href="' + appUrl('/web/audit/instance/' + node.Key.Hostname + '/' + node.Key.Port) + '">' + node.title + '</a>');
  addNodeModalDataAttribute("Agent",
    '<a href="' + appUrl('/web/agent/' + node.Key.Hostname) + '">' + node.Key.Hostname + '</a>');

  $('#node_modal [data-btn]').unbind("click");

  $("#beginDowntimeOwner").val(getUserId());
  $('#node_modal button[data-btn=begin-downtime]').click(function() {
    if (!$("#beginDowntimeOwner").val()) {
      return addModalAlert("You must fill the owner field");
    }
    if (!$("#beginDowntimeReason").val()) {
      return addModalAlert("You must fill the reason field");
    }
    var uri = "/api/begin-downtime/" + node.Key.Hostname + "/" + node.Key.Port + "/" + $("#beginDowntimeOwner").val() + "/" + $("#beginDowntimeReason").val() + "/" + $("#beginDowntimeDuration").val();
    apiCommand(uri);
  });
  $('#node_modal button[data-btn=refresh-instance]').click(function() {
    apiCommand("/api/refresh/" + node.Key.Hostname + "/" + node.Key.Port, "refresh");
  });
  $('#node_modal button[data-btn=skip-query]').click(function() {
    apiCommand("/api/skip-query/" + node.Key.Hostname + "/" + node.Key.Port);
  });
  $('#node_modal button[data-btn=start-replica]').click(function() {
    apiCommand("/api/start-replica/" + node.Key.Hostname + "/" + node.Key.Port);
  });
  $('#node_modal button[data-btn=restart-replica]').click(function() {
    apiCommand("/api/restart-replica/" + node.Key.Hostname + "/" + node.Key.Port);
  });
  $('#node_modal button[data-btn=stop-replica]').click(function() {
    apiCommand("/api/stop-replica/" + node.Key.Hostname + "/" + node.Key.Port);
  });
  $('#node_modal button[data-btn=detach-replica]').click(function() {
    apiCommand("/api/detach-replica/" + node.Key.Hostname + "/" + node.Key.Port);
  });
  $('#node_modal button[data-btn=reattach-replica]').click(function() {
    apiCommand("/api/reattach-replica/" + node.Key.Hostname + "/" + node.Key.Port);
  });
  $('#node_modal button[data-btn=reattach-replica-master-host]').click(function() {
    apiCommand("/api/reattach-replica-master-host/" + node.Key.Hostname + "/" + node.Key.Port);
  });
  $('#node_modal button[data-btn=reset-replica]').click(function() {
    var message = "<p>Are you sure you wish to reset <code><strong>" + node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code>?" +
      "<p>This will stop and break the replication." +
      "<p>FYI, this is a destructive operation that cannot be easily reverted";
    bootbox.confirm(message, function(confirm) {
      if (confirm) {
        apiCommand("/api/reset-replica/" + node.Key.Hostname + "/" + node.Key.Port);
      }
    });
    return false;
  });
  $('#node_modal [data-btn=gtid-errant-reset-master]').click(function() {
    var message = "<p>Are you sure you wish to reset master on <code><strong>" + node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code>?" +
      "<p>This will purge binary logs on server.";
    bootbox.confirm(message, function(confirm) {
      if (confirm) {
        apiCommand("/api/gtid-errant-reset-master/" + node.Key.Hostname + "/" + node.Key.Port);
      }
    });
    return false;
  });
  $('#node_modal [data-btn=gtid-errant-inject-empty]').click(function() {
    var message = "<p>Are you sure you wish to inject empty transactions on the master of this cluster?";
    bootbox.confirm(message, function(confirm) {
      if (confirm) {
        apiCommand("/api/gtid-errant-inject-empty/" + node.Key.Hostname + "/" + node.Key.Port);
      }
    });
    return false;
  });
  $('#node_modal button[data-btn=set-read-only]').click(function() {
    apiCommand("/api/set-read-only/" + node.Key.Hostname + "/" + node.Key.Port);
  });
  $('#node_modal button[data-btn=set-writeable]').click(function() {
    apiCommand("/api/set-writeable/" + node.Key.Hostname + "/" + node.Key.Port);
  });
  $('#node_modal button[data-btn=enable-gtid]').click(function() {
    var message = "<p>Are you sure you wish to enable GTID on <code><strong>" + node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code>?" +
      "<p>Replication <i>might</i> break as consequence";
    bootbox.confirm(message, function(confirm) {
      if (confirm) {
        apiCommand("/api/enable-gtid/" + node.Key.Hostname + "/" + node.Key.Port);
      }
    });
  });
  $('#node_modal button[data-btn=disable-gtid]').click(function() {
    var message = "<p>Are you sure you wish to disable GTID on <code><strong>" + node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code>?" +
      "<p>Replication <i>might</i> break as consequence";
    bootbox.confirm(message, function(confirm) {
      if (confirm) {
        apiCommand("/api/disable-gtid/" + node.Key.Hostname + "/" + node.Key.Port);
      }
    });
  });
  $('#node_modal button[data-btn=forget-instance]').click(function() {
    var message = "<p>Are you sure you wish to forget <code><strong>" + node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code>?" +
      "<p>It may be re-discovered if accessible from an existing instance through replication topology.";
    bootbox.confirm(message, function(confirm) {
      if (confirm) {
        apiCommand("/api/forget/" + node.Key.Hostname + "/" + node.Key.Port);
      }
    });
    return false;
  });

  $("body").on("click", "#node_modal a[data-btn=move-equivalent]", function(event) {
    var targetHostname = $(event.target).attr("data-hostname");
    var targetPort = $(event.target).attr("data-port");
    apiCommand("/api/move-equivalent/" + node.Key.Hostname + "/" + node.Key.Port + "/" + targetHostname + "/" + targetPort);
  });

  if (node.IsDowntimed) {
    $('#node_modal .end-downtime .panel-heading').html("Downtimed by <strong>" + node.DowntimeOwner + "</strong> until " + node.DowntimeEndTimestamp);
    $('#node_modal .end-downtime .panel-body').html(
      node.DowntimeReason
    );
    $('#node_modal .begin-downtime').hide();
    $('#node_modal button[data-btn=begin-downtime]').hide();

    $('#node_modal .end-downtime').show();
    $('#node_modal button[data-btn=end-downtime]').show();
  } else {
    $('#node_modal .begin-downtime').show();
    $('#node_modal button[data-btn=begin-downtime]').show();

    $('#node_modal .end-downtime').hide();
    $('#node_modal button[data-btn=end-downtime]').hide();
  }
  $('#node_modal button[data-btn=skip-query]').hide();
  $('#node_modal button[data-btn=start-replica]').hide();
  $('#node_modal button[data-btn=restart-replica]').hide();
  $('#node_modal button[data-btn=stop-replica]').hide();

  if (node.MasterKey.Hostname) {
    if (node.replicationRunning || node.replicationAttemptingToRun) {
      $('#node_modal button[data-btn=stop-replica]').show();
      $('#node_modal button[data-btn=restart-replica]').show();
    } else if (!node.replicationRunning) {
      $('#node_modal button[data-btn=start-replica]').show();
    }
    if (!node.ReplicationSQLThreadRuning && node.LastSQLError) {
      $('#node_modal button[data-btn=skip-query]').show();
    }
  }

  $('#node_modal button[data-btn=set-read-only]').hide();
  $('#node_modal button[data-btn=set-writeable]').hide();
  if (node.ReadOnly) {
    $('#node_modal button[data-btn=set-writeable]').show();
  } else {
    $('#node_modal button[data-btn=set-read-only]').show();
  }

  $('#node_modal button[data-btn=enable-gtid]').hide();
  $('#node_modal button[data-btn=disable-gtid]').hide();
  if (node.supportsGTID && node.usingGTID) {
    $('#node_modal button[data-btn=disable-gtid]').show();
  } else if (node.supportsGTID) {
    $('#node_modal button[data-btn=enable-gtid]').show();
  }

  $('#node_modal button[data-btn=regroup-replicas]').hide();
  if (node.Replicas.length > 1) {
    $('#node_modal button[data-btn=regroup-replicas]').show();
  }
  $('#node_modal button[data-btn=regroup-replicas]').click(function() {
    var message = "<p>Are you sure you wish to regroup replicas of <code><strong>" + node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code>?" +
      "<p>This will attempt to promote one replica over its siblings";
    bootbox.confirm(message, function(confirm) {
      if (confirm) {
        apiCommand("/api/regroup-replicas/" + node.Key.Hostname + "/" + node.Key.Port);
      }
    });
  });

  $('#node_modal button[data-btn=take-siblings]').hide();
  if (node.LogBinEnabled && node.LogReplicationUpdatesEnabled) {
    $('#node_modal button[data-btn=take-siblings]').show();
  }
  $('#node_modal button[data-btn=take-siblings]').click(function() {
    var apiUrl = "/api/take-siblings/" + node.Key.Hostname + "/" + node.Key.Port;
    if (isSilentUI()) {
      apiCommand(apiUrl);
    } else {
      var message = "<p>Are you sure you want <code><strong>" + node.Key.Hostname + ":" + node.Key.Port +
        "</strong></code> to take its siblings?";
      bootbox.confirm(message, function(confirm) {
        if (confirm) {
          apiCommand(apiUrl);
        }
      });
    }
  });
  $('#node_modal button[data-btn=end-downtime]').click(function() {
    apiCommand("/api/end-downtime/" + node.Key.Hostname + "/" + node.Key.Port);
  });
  $('#node_modal button[data-btn=recover]').hide();
  if (node.lastCheckInvalidProblem() && node.children && node.children.length > 0) {
    $('#node_modal button[data-btn=recover]').show();
  }
  $('#node_modal button[data-btn=recover]').click(function() {
    apiCommand("/api/recover/" + node.Key.Hostname + "/" + node.Key.Port);
  });
  $('#node_modal button[data-btn=end-maintenance]').click(function() {
    apiCommand("/api/end-maintenance/" + node.Key.Hostname + "/" + node.Key.Port);
  });

  if (!isAuthorizedForAction()) {
    $('#node_modal button[data-btn]').hide();
    $('#node_modal [data-btn-group]').hide();
  }

  $('#node_modal').modal({})
  $('#node_modal').unbind('hidden.bs.modal');
  $('#node_modal').on('hidden.bs.modal', function() {
    nodeModalVisible = false;
  })
}


function normalizeInstance(instance) {
  instance.id = getInstanceId(instance.Key.Hostname, instance.Key.Port);
  instance.title = instance.Key.Hostname + ':' + instance.Key.Port;
  instance.canonicalTitle = instance.title;
  instance.masterTitle = instance.MasterKey.Hostname + ":" + instance.MasterKey.Port;
  // If this host is a replication group member, we set its masterId to the group primary, unless the instance is itself
  // the primary. In that case, we set it to its async/semi-sync master (if configured). Notice that for group members
  // whose role is not defined (e.g. because they are in ERROR state) we still set their master ID to the group primary.
  // Setting the masterId to the group primary is what allows us to visualize group secondary members as replicating
  // from the group primary.
  if (instance.ReplicationGroupName != "" && (instance.ReplicationGroupMemberRole == "SECONDARY" || instance.ReplicationGroupMemberRole == ""))
    masterKey = instance.ReplicationGroupPrimaryInstanceKey;
  else
    masterKey = instance.MasterKey;
  instance.masterId = getInstanceId(masterKey.Hostname, masterKey.Port);

  instance.replicationRunning = instance.ReplicationSQLThreadRuning && instance.ReplicationIOThreadRuning;
  instance.replicationAttemptingToRun = instance.ReplicationSQLThreadRuning || instance.ReplicationIOThreadRuning;
  instance.replicationLagReasonable = Math.abs(instance.ReplicationLagSeconds.Int64 - instance.SQLDelay) <= 10;
  instance.isSeenRecently = instance.SecondsSinceLastSeen.Valid && instance.SecondsSinceLastSeen.Int64 <= 3600;
  instance.supportsGTID = instance.SupportsOracleGTID || instance.UsingMariaDBGTID;
  instance.usingGTID = instance.UsingOracleGTID || instance.UsingMariaDBGTID;
  instance.isMaxScale = (instance.Version.indexOf("maxscale") >= 0);

  // used by cluster-tree
  instance.children = [];
  instance.parent = null;
  instance.hasMaster = true;
  instance.masterNode = null;
  instance.inMaintenance = false;
  instance.maintenanceReason = "";
  instance.maintenanceEntry = null;
  instance.isFirstChildInDisplay = false

  instance.isMaster = (instance.title == instance.ClusterName);
  instance.isCoMaster = false;
  instance.isCandidateMaster = false;
  instance.isMostAdvancedOfSiblings = false;
  instance.isVirtual = false;
  instance.isAnchor = false;
  instance.isAggregate = false;

  instance.renderHint = "";

  if (instance.LastSQLError == '""') {
    instance.LastSQLError = '';
  }
  if (instance.LastIOError == '""') {
    instance.LastIOError = '';
  }
}

function normalizeInstanceProblem(instance) {

  function instanceProblemIfExists(problemName) {
    if (instance.Problems.includes(problemName)) {
      return problemName
    }
    return null;
  }
  instance.inMaintenanceProblem = function() {
    return instanceProblemIfExists('in_maintenance');
  }
  instance.lastCheckInvalidProblem = function() {
    return instanceProblemIfExists('last_check_invalid');
  }
  instance.notRecentlyCheckedProblem = function() {
    return instanceProblemIfExists('not_recently_checked');
  }
  instance.notReplicatingProblem = function() {
    return instanceProblemIfExists('not_replicating');
  }
  instance.replicationLagProblem = function() {
    return instanceProblemIfExists('replication_lag');
  }
  instance.errantGTIDProblem = function() {
    return instanceProblemIfExists('errant_gtid');
  }
  instance.replicationGroupMemberStateProblem = function() {
    return instanceProblemIfExists("group_replication_member_not_online")
  }

  instance.problem = null;
  instance.Problems = instance.Problems || [];
  if (instance.Problems.length > 0) {
    instance.problem = instance.Problems[0]; // highest priority one
  }
  instance.problemOrder = 0;
  if (instance.inMaintenanceProblem()) {
    instance.problemDescription = "This instance is now under maintenance due to some pending operation.\nSee audit page";
    instance.problemOrder = 1;
  } else if (instance.lastCheckInvalidProblem()) {
    instance.problemDescription = "Instance cannot be reached by orchestrator.\nIt might be dead or there may be a network problem";
    instance.problemOrder = 2;
  } else if (instance.notRecentlyCheckedProblem()) {
    instance.problemDescription = "Orchestrator has not made an attempt to reach this instance for a while now.\nThis should generally not happen; consider refreshing or re-discovering this instance";
    instance.problemOrder = 3;
  } else if (instance.notReplicatingProblem()) {
    // check replicas only; where not replicating
    instance.problemDescription = "Replication is not running.\nEither stopped manually or is failing on I/O or SQL error.";
    instance.problemOrder = 4;
  } else if (instance.replicationLagProblem()) {
    instance.problemDescription = "Replica is lagging.\nThis diagnostic is based on either Seconds_behind_master or configured ReplicationLagQuery";
    instance.problemOrder = 5;
  } else if (instance.errantGTIDProblem()) {
    instance.problemDescription = "Replica has GTID entries not found on its master";
    instance.problemOrder = 6;
  } else if (instance.replicationGroupMemberStateProblem()) {
    instance.problemDescription = "Replication group member in state " + instance.ReplicationGroupMemberState;
    instance.problemOrder = 7;
  }
  instance.hasProblem = (instance.problem != null);
  instance.hasConnectivityProblem = (!instance.IsLastCheckValid || !instance.IsRecentlyChecked);
}

var virtualInstanceCounter = 0;

function createVirtualInstance() {
  var virtualInstance = {
    id: "orchestrator-virtual-instance-" + (virtualInstanceCounter++),
    children: [],
    parent: null,
    hasMaster: false,
    inMaintenance: false,
    maintenanceEntry: null,
    isMaster: false,
    isCoMaster: false,
    isVirtual: true,
    ReplicationLagSeconds: 0,
    SecondsSinceLastSeen: 0
  }
  normalizeInstanceProblem(virtualInstance);
  return virtualInstance;
}

function normalizeInstances(instances, maintenanceList) {
  if (!instances) {
    instances = [];
  }
  instances.forEach(function(instance) {
    normalizeInstance(instance);
  });
  // Take canonical host name: strip down longest common suffix of all hosts
  // (experimental; you may not like it)
  var hostNames = instances.map(function(instance) {
    return instance.title
  });
  instances.forEach(function(instance) {
    instance.canonicalTitle = canonizeInstanceTitle(instance.title)
  });
  var instancesMap = {};
  instances.forEach(function(instance) {
    instancesMap[instance.id] = instance;
  });
  // mark maintenance instances
  maintenanceList.forEach(function(maintenanceEntry) {
    var instanceId = getInstanceId(maintenanceEntry.Key.Hostname, maintenanceEntry.Key.Port)
    if (instanceId in instancesMap) {
      instancesMap[instanceId].inMaintenance = true;
      instancesMap[instanceId].maintenanceReason = maintenanceEntry.Reason;
      instancesMap[instanceId].maintenanceEntry = maintenanceEntry;
    }
  });
  instances.forEach(function(instance) {
    // Now that we also know about maintenance
    normalizeInstanceProblem(instance);
  });
  // create the tree array
  instances.forEach(function(instance) {
    // add to parent
    var parent = instancesMap[instance.masterId];
    if (parent) {
      instance.parent = parent;
      instance.masterNode = parent;
      // create child array if it doesn't exist
      parent.children.push(instance);
      // (parent.contents || (parent.contents = [])).push(instance);
    } else {
      // parent is null or missing
      instance.hasMaster = false;
      instance.parent = null;
      instance.masterNode = null;
    }
  });

  instances.forEach(function(instance) {
    if (instance.masterNode != null) {
      instance.isSQLThreadCaughtUpWithIOThread = (instance.ExecBinlogCoordinates.LogFile == instance.ReadBinlogCoordinates.LogFile &&
        instance.ExecBinlogCoordinates.LogPos == instance.ReadBinlogCoordinates.LogPos);
    } else {
      instance.isSQLThreadCaughtUpWithIOThread = false;
    }
  });

  instances.forEach(function(instance) {
    if (instance.isMaster && instance.parent != null && instance.parent.parent != null && instance.parent.parent.id == instance.id) {
      // In case there's a master-master setup, introduce a virtual node
      // that is parent of both.
      // This is for visualization purposes...
      var virtualCoMastersRoot = createVirtualInstance();
      coMaster = instance.parent;

      function setAsCoMaster(instance, coMaster) {
        instance.isCoMaster = true;
        instance.hasMaster = true;
        instance.masterId = coMaster.id;
        instance.masterNode = coMaster;

        var index = coMaster.children.indexOf(instance);
        if (index >= 0)
          coMaster.children.splice(index, 1);

        instance.parent = virtualCoMastersRoot;
        virtualCoMastersRoot.children.push(instance);
      }
      setAsCoMaster(instance, coMaster);
      setAsCoMaster(coMaster, instance);

      instancesMap[virtualCoMastersRoot.id] = virtualCoMastersRoot;
    }
  });
  return instancesMap;
}

function formattedInterval(n) {
    var days = Math.floor(n / 24 / 3600);

    n = n % (24 * 3600) ;
    var hours = Math.floor(n / 3600);

    n = n % 3600;
    var minutes = Math.floor(n / 60);

    n = n % 60;
    var seconds = n;

    var formatted = seconds.toString(10) + "s";
    if (days != 0 || hours != 0 || minutes != 0) {
        formatted = minutes.toString(10) + "m " + formatted;
    }
    if (days != 0 || hours != 0) {
        formatted = hours.toString(10) + "h " + formatted;
    }
    if (days != 0) {
        formatted = days.toString(10) + "d " + formatted;
    }

    return formatted;
}

function renderInstanceElement(popoverElement, instance, renderType) {
  // $(this).find("h3 .pull-left").html(anonymizeInstanceId(instanceId));
  // $(this).find("h3").attr("title", anonymizeInstanceId(instanceId));
  var anonymizedInstanceId = anonymizeInstanceId(instance.id);
  popoverElement.attr("data-nodeid", instance.id);
  var tooltip = "";
  if (isAnonymized()) {
    tooltip = anonymizedInstanceId;
  } else {
    tooltip = (isAliased() ? instance.InstanceAlias : instance.title);
    if (instance.Region) { tooltip += "\nRegion: " + instance.Region; }
    if (instance.DataCenter) { tooltip += "\nData center: " + instance.DataCenter; }
    if (instance.PhysicalEnvironment) { tooltip += "\nPhysical environment: " + instance.PhysicalEnvironment; }
  }
  popoverElement.find("h3").attr('title', tooltip);
  popoverElement.find("h3").html('&nbsp;<div class="pull-left">' +
    (isAnonymized() ? anonymizedInstanceId : isAliased() ? instance.InstanceAlias : instance.canonicalTitle) +
    '</div><div class="pull-right instance-glyphs"><span class="glyphicon glyphicon-cog" title="Open config dialog"></span></div>');
  var indicateLastSeenInStatus = false;

  if (instance.isAggregate) {
    popoverElement.find("h3 div.pull-right span").remove();
    popoverElement.find(".instance-content").append('<div>Instances: <div class="pull-right"></div></div>');

    function addInstancesBadge(count, badgeClass, title) {
      popoverElement.find(".instance-content .pull-right").append('<span class="badge ' + badgeClass + '" data-toggle="tooltip" data-placement="bottom" data-html="true" title="' + title + '">' + count + '</span> ');
      popoverElement.find('[data-toggle="tooltip"]').tooltip();
    }
    var instancesHint = instance.aggregatedProblems[""].join("<br>");
    addInstancesBadge(instance.aggregatedInstances.length, "label-primary", "Aggregated instances<br>" + instancesHint);

    for (var problemType in instance.aggregatedProblems) {
      if (errorMapping[problemType]) {
        var description = errorMapping[problemType]["description"];
        var instancesHint = instance.aggregatedProblems[problemType].join("<br>");
        addInstancesBadge(instance.aggregatedProblems[problemType].length, errorMapping[problemType]["badge"], description + "<br>" + instancesHint);
      }
    }
  }
  if (!instance.isAggregate) {
    if (instance.isFirstChildInDisplay) {
      popoverElement.addClass("first-child-in-display");
      popoverElement.attr("data-first-child-in-display", "true");
    }
    if (instance.supportsGTID) {
      if (instance.hasMaster && !instance.usingGTID) {
        popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon text-muted glyphicon-globe" title="Support GTID but not using it in replication"></span> ');
      } else if (instance.GtidErrant) {
        popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon text-danger glyphicon-globe" title="Errant GTID found"></span> ');
      } else {
        popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-globe" title="Using GTID"></span> ');
      }
    }
    if (instance.UsingPseudoGTID) {
      popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-globe" title="Using Pseudo GTID"></span> ');
    }
    if (!instance.ReadOnly) {
      popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-pencil" title="Writeable"></span> ');
    }
    if (instance.isMostAdvancedOfSiblings) {
      popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-star" title="Most advanced replica"></span> ');
    }
    if (instance.CountMySQLSnapshots > 0) {
      popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-camera" title="' + instance.CountMySQLSnapshots + ' snapshots"></span> ');
    }
    if (instance.HasReplicationFilters) {
      popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-filter" title="Using replication filters"></span> ');
    }
    if (instance.SemiSyncMasterStatus) {
      popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-check" title="Semi sync enabled (master side)"></span> ');
    }
    if (instance.SemiSyncReplicaStatus) {
      popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-saved" title="Semi sync enabled (replica side)"></span> ');
    }
    if (instance.LogBinEnabled && instance.LogReplicationUpdatesEnabled) {
      popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-forward" title="Logs replication updates"></span> ');
    }
    // Icons for GR
    var text_style;
    if (instance.ReplicationGroupName != "") {
      if (instance.ReplicationGroupMemberRole == "PRIMARY")
        text_style = "";
      else
        text_style = "text-muted";
      popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon '+ text_style +' glyphicon-tower" title="Group replication '+ instance.ReplicationGroupMemberRole + ' ' + instance.ReplicationGroupMemberState +'"></span> ');
    }

    if (instance.IsCandidate) {
      popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-heart" title="Candidate"></span> ');
    }
    if (instance.PromotionRule == "prefer_not") {
      popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-thumbs-down" title="Prefer not promote"></span> ');
    }
    if (instance.PromotionRule == "must_not") {
      popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-ban-circle" title="Must not promote"></span> ');
    }
    if (instance.inMaintenanceProblem()) {
      popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-wrench" title="In maintenance"></span> ');
    }
    if (instance.IsDetached) {
      popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-remove-sign" title="Replication forcibly detached"></span> ');
    }
    if (instance.IsDowntimed) {
      var downtimeMessage = 'Downtimed by ' + instance.DowntimeOwner + ': ' + instance.DowntimeReason + '.\nEnds: ' + instance.DowntimeEndTimestamp;
      popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-volume-off" title="' + downtimeMessage + '"></span> ');
    }

    if (instance.lastCheckInvalidProblem()) {
      instance.renderHint = "fatal";
      indicateLastSeenInStatus = true;
    } else if (instance.notRecentlyCheckedProblem()) {
      instance.renderHint = "stale";
      indicateLastSeenInStatus = true;
    } else if (instance.notReplicatingProblem()) {
      // check replicas only; check master only if it's co-master where not
      // replicating
      instance.renderHint = "danger";
    } else if (instance.replicationLagProblem()) {
      instance.renderHint = "warning";
    } else if (instance.replicationGroupMemberStateProblem()) {
      switch (instance.ReplicationGroupMemberState) {
        case "RECOVERING":
          instance.renderHint = "warning";
          break;
        default:
          instance.renderHint = "danger";
          break;
      }
    }
    if (instance.renderHint != "") {
      popoverElement.find("h3").addClass("label-" + instance.renderHint);
    }
    var statusMessage = formattedInterval(instance.ReplicationLagSeconds.Int64) + ' lag';
    if (indicateLastSeenInStatus) {
      statusMessage = 'seen ' + formattedInterval(instance.SecondsSinceLastSeen.Int64) + ' ago';
    }
    var identityHtml = '';
    if (isAnonymized()) {
      identityHtml += instance.Version.match(/[^.]+[.][^.]+/);
    } else {
      identityHtml += instance.Version;
    }
    if (instance.LogBinEnabled) {
      var format = instance.Binlog_format;
      if (format == 'ROW' && instance.BinlogRowImage != '') {
        format = format + "/" + instance.BinlogRowImage.substring(0,1);
      }
      identityHtml += " " + format;
    }
    if (!isAnonymized()) {
      identityHtml += ', ' + instance.FlavorName;
    }

    var contentHtml = '' + '<div class="pull-right">' + statusMessage + ' </div>' + '<p class="instance-basic-info">' + identityHtml + '</p>';
    if (instance.isCoMaster) {
      contentHtml += '<p><strong>Co master</strong></p>';
    } else if (instance.isMaster) {
      contentHtml += '<p><strong>Master</strong></p>';
    }
    if (renderType == "search") {
      if (instance.SuggestedClusterAlias) {
        contentHtml += '<p>' + 'Cluster: <a href="' + appUrl('/web/cluster/alias/' + instance.SuggestedClusterAlias) + '">' + instance.SuggestedClusterAlias + '</a>' + '</p>';
      } else {
        contentHtml += '<p>' + 'Cluster: <a href="' + appUrl('/web/cluster/' + instance.ClusterName) + '">' + instance.ClusterName + '</a>' + '</p>';
      }
    }
    if (renderType == "problems") {
      contentHtml += '<p>' + 'Problem: <strong title="' + instance.problemDescription + '">' + instance.problem.replace(/_/g, ' ') + '</strong>' + '</p>';
    }
    popoverElement.find(".instance-content").html(contentHtml);
  }

  popoverElement.find("h3 .instance-glyphs").click(function() {
    openNodeModal(instance);
    return false;
  });
}

var onClustersListeners = [];

function onClusters(func) {
  onClustersListeners.push(func);
}


function getParameterByName(name) {
  name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
  var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
    results = regex.exec(location.search);
  return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}


function renderGlobalRecoveriesButton(isGlobalRecoveriesEnabled) {
  var iconContainer = $("#global-recoveries-icon > span");
  if (isGlobalRecoveriesEnabled) {
    iconContainer
      .prop("title", "Global Recoveries Enabled")
      .addClass("glyphicon-ok-sign")
      .removeClass("hidden")
      .click(function(event) {
        bootbox.confirm("<h3>Global Recoveries</h3>Are you sure you want to <strong>disable</strong> global recoveries?", function(confirm) {
          if (confirm) {
            apiCommand("/api/disable-global-recoveries");
          }
        })
      });
  } else {
    iconContainer
      .prop("title", "Global Recoveries Disabled")
      .addClass("glyphicon-remove-sign")
      .removeClass("hidden")
      .click(function(event) {
        bootbox.confirm("<h3>Global Recoveries</h3>Are you sure you want to enable global recoveries?", function(confirm) {
          if (confirm) {
            apiCommand("/api/enable-global-recoveries");
          }
        })
      });
  }
}

$(document).ready(function() {
  visualizeBrand();
  if (webMessage()) {
    addAlert(webMessage(), "warning")
  }
  $.get(appUrl("/api/clusters-info"), function(clusters) {
    clusters = clusters || [];

    function sortAlphabetically(cluster1, cluster2) {
      var cmp = cluster1.ClusterAlias.localeCompare(cluster2.ClusterAlias);
      if (cmp == 0) {
        cmp = cluster1.ClusterName.localeCompare(cluster2.ClusterName);
      }
      return cmp;
    }
    clusters.sort(sortAlphabetically);

    clusters.forEach(function(cluster) {
      var url = appUrl('/web/cluster/' + cluster.ClusterName)
      var title = cluster.ClusterName;
      if ((cluster.ClusterAlias != "") && (cluster.ClusterAlias != cluster.ClusterName)) {
        url = appUrl('/web/cluster/alias/' + encodeURIComponent(cluster.ClusterAlias));
        title = '<strong>' + cluster.ClusterAlias + '</strong>, <span class="small">' + title + '</span>';;
      }
      $("#dropdown-clusters").append('<li><a href="' + url + '">' + title + '</a></li>');
    });
    onClustersListeners.forEach(function(func) {
      func(clusters);
    });
  }, "json");

  $.get(appUrl("/api/check-global-recoveries"), function(response) {
    var isEnabled = (response.Details == "enabled")
    renderGlobalRecoveriesButton(isEnabled);
  }, "json");

  $(".ajaxLoader").click(function() {
    return false;
  });
  $("#refreshCountdown").click(function() {
    if ($.cookie("auto-refresh") == "true") {
      $.cookie("auto-refresh", "false", {
        path: '/',
        expires: 1
      });
    } else {
      $.cookie("auto-refresh", "true", {
        path: '/',
        expires: 1
      });
    }
    updateCountdownDisplay();
  });
  if (agentsHttpActive() == "true") {
    $("#nav_agents").show();
  }
  if (contextMenuVisible() == "true") {
    showContextMenu();
  }
  if (!isAuthorizedForAction()) {
    $("[data-nav-page=read-only]").css('display', 'inline-block');
  }
  if (getUserId() != "" && !isAnonymized()) {
    $("[data-nav-page=user-id]").css('display', 'inline-block');
    $("[data-nav-page=user-id] a").html(" " + getUserId());
  }
  var orchestratorMsg = sanitizeHTML(getParameterByName("orchestrator-msg"))
  if (orchestratorMsg) {
    addInfo(orchestratorMsg)

    reloadPageHint = {
      hint: getParameterByName("hint"),
      hostname: getParameterByName("hostname"),
      port: getParameterByName("port")
    }
    history.pushState(null, document.title, location.href.split("?orchestrator-msg=")[0])
  }
  if (typeof($.cookie("auto-refresh")) === 'undefined') {
    $.cookie("auto-refresh", "true", {
      path: '/',
      expires: 1
    });
  }
  $("#searchInput").focusin(function() {
    $("[data-nav-page=search] button").show();
    $("#searchInput").css("width", "");
  });
  $("#searchInput").focusout(function() {
    $("[data-nav-page=search] button").hide();
    $("#searchInput").css("width", $("#searchInput").width()/2);
  });
  $("#searchInput").focusout();
});
