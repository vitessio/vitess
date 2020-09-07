function Cluster() {
  if (this == window)
    return new Cluster();
  var _this = this;
  Function.addTo(_this, [repositionIntanceDiv]);


  var moveInstanceMethod = $.cookie("move-instance-method") || "smart";
  var droppableIsActive = false;

  var renderColors = ["#ff8c00", "#4682b4", "#9acd32", "#dc143c", "#9932cc", "#ffd700", "#191970", "#7fffd4", "#808080", "#dda0dd"];
  var dcColorsMap = {};

  var _instances, _replicationAnalysis, _maintenanceList, _instancesMap, _isDraggingTrailer = false;
  var _countDragOver = 0;

  var _instanceCommands = {
    "recover-auto": function(e) {
      apiCommand("/api/recover/" + _instancesMap[e.draggedNodeId].Key.Hostname + "/" + _instancesMap[e.draggedNodeId].Key.Port);
      return true;
    },
    "recover-auto-lite": function(e) {
      apiCommand("/api/recover-lite/" + _instancesMap[e.draggedNodeId].Key.Hostname + "/" + _instancesMap[e.draggedNodeId].Key.Port);
      return true;
    },
    "force-master-failover": function(e) {
      apiCommand("/api/force-master-failover/" + _instancesMap[e.draggedNodeId].Key.Hostname + "/" + _instancesMap[e.draggedNodeId].Key.Port);
      return true;
    },
    "recover-suggested-successor": function(e) {
      var suggestedSuccessorHost = $(e.target).attr("data-successor-host");
      var suggestedSuccessorPort = $(e.target).attr("data-successor-port");
      apiCommand("/api/recover/" + _instancesMap[e.draggedNodeId].Key.Hostname + "/" + _instancesMap[e.draggedNodeId].Key.Port + "/" + suggestedSuccessorHost + "/" + suggestedSuccessorPort);
      return true;
    },
    "relocate-replicas": function(e) {
      var belowHost = $(e.target).attr("data-successor-host");
      var belowPort = $(e.target).attr("data-successor-port");
      apiCommand("/api/relocate-replicas/" + _instancesMap[e.draggedNodeId].Key.Hostname + "/" + _instancesMap[e.draggedNodeId].Key.Port + "/" + belowHost + "/" + belowPort);
      return true;
    },
    "make-master": function(e) {
      makeMaster(_instancesMap[e.draggedNodeId]);
      return false;
    },
    "make-local-master": function(e) {
      makeLocalMaster(_instancesMap[e.draggedNodeId]);
      return false;
    },
  };

  Object.defineProperties(_this, {
    moveInstanceMethod: {
      get: function() {
        return moveInstanceMethod;
      }
    },
    droppableIsActive: {
      get: function() {
        return droppableIsActive;
      }
    },
    renderColors: {
      get: function() {
        return renderColors;
      }
    },
    dcColorsMap: {
      get: function() {
        return dcColorsMap;
      }
    },
    _instances: {
      get: function() {
        return _instances;
      }
    },
    _replicationAnalysis: {
      get: function() {
        return _replicationAnalysis;
      }
    },
    _maintenanceList: {
      get: function() {
        return _maintenanceList;
      }
    },
    _instancesMap: {
      get: function() {
        return _instancesMap;
      }
    },
  });

  main();

  function isColorizeDC() {
    if ($.cookie("colorize-dc") == "false") {
      return false
    }
    return true
  }

  function getInstanceDiv(instanceId) {
    var popoverDiv = $("#cluster_container > .instance[data-nodeid='" + instanceId + "']");
    return popoverDiv
  }

  function repositionIntanceDiv(id) {
    if (!id)
      return false;

    var popoverDiv = getInstanceDiv(id);
    var wrapper = $(popoverDiv).data("svg-instance-wrapper");
    if (!wrapper) {
      // Can happen for virtual node
      return false;
    }
    var circle = wrapper.previousElementSibling;
    var pos = getSvgPos(circle);
    pos.left += 25;
    pos.top -= popoverDiv.height() / 2 - 2;
    popoverDiv.css({
      left: pos.left + "px",
      top: pos.top + "px"
    });

    popoverDiv.show();
  }

  function clearDroppable() {
    $(".original-dragged").removeClass("original-dragged");
    resetRefreshTimer();
    $("#cluster_container .accept_drop_check").removeClass("accept_drop_check");
    $("#cluster_container .accept_drop").removeClass("accept_drop");
    $("#cluster_container .accept_drop_warning").removeClass("accept_drop_warning");
    $(".being-dragged").removeClass("being-dragged");
    $(".instance-trailer").show();
    droppableIsActive = false;
  }


  // All instance dragging/dropping code goes here
  function activateInstanceDraggable(instanceEl) {
    if (!isAuthorizedForAction())
      return;

    var nodesMap = _instancesMap;
    var draggedNodeId = instanceEl.attr("data-nodeid");
    var trailerEl = instanceEl_getTrailerEl(instanceEl);
    var opts = {
      addClasses: true,
      opacity: 1,
      cancel: "button,a,span, .instance-trailer",
      snap: "#cluster_container .instance",
      snapMode: "inner",
      snapTolerance: 10,
      helper: "clone",
      zIndex: 100,
      containment: "#cluster_container",
      start: instance_dragStart,
      drag: instance_drag,
      stop: instance_dragStop,
    };

    if (nodesMap[draggedNodeId].lastCheckInvalidProblem() || nodesMap[draggedNodeId].notRecentlyCheckedProblem()) {
      instanceEl.find("h3").click(function() {
        openNodeModal(nodesMap[draggedNodeId]);
        return false;
      });
    } else {
      $(instanceEl).draggable(opts);
    }

    var opts2 = Q.copy(opts);
    opts.cancel = "button,a";
    $(trailerEl).draggable(opts);
  }

  function instance_dragStart(e, ui) {
    var instanceEl = $(e.target).closest(".instance");
    var trailerEl = instanceEl_getTrailerEl(instanceEl);

    clearDroppable();
    droppableIsActive = true;

    // dragging begins
    _isDraggingTrailer = $(e.originalEvent.target).closest(".instance-trailer").length == 1;
    if (_isDraggingTrailer) {
      if (!isAuthorizedForAction())
        return false;

      var draggedNode = instanceEl_getNode(instanceEl);
      draggedNode.children.forEach(function f(instance) {
        var instanceEl2 = getInstanceDiv(instance.id);
        instanceEl2.addClass("original-dragged");
      });

      trailerEl.addClass("original-dragged");
    } else {
      instanceEl.addClass("original-dragged");
    }
  }

  function instance_drag(e, ui) {
    resetRefreshTimer();
  }

  function instance_dragStop(e, ui) {
    clearDroppable();

  }

  function instanceEl_getNode(instanceEl) {
    return _instancesMap[instanceEl.attr("data-nodeid")];
  }

  function generateInstanceDiv(svgInstanceWrapper, nodesMap) {
    var isVirtual = $(svgInstanceWrapper).attr("data-fo-is-virtual") == "true";
    var isAnchor = $(svgInstanceWrapper).attr("data-fo-is-anchor") == "true";
    if (isVirtual || isAnchor)
      return;
    var id = $(svgInstanceWrapper).attr("data-fo-id");
    var node = nodesMap[id];

    var instanceEl = Instance.createElement(node).addClass("instance-diagram arrow_box").appendTo("#cluster_container");
    $(instanceEl).hide();
    $(svgInstanceWrapper).data("instance-popover", instanceEl);
    $(instanceEl).data("svg-instance-wrapper", svgInstanceWrapper);

    renderInstanceElement(instanceEl, node, "cluster");

    var masterSectionEl = $('<div class="instance-master-section" data-nodeid="' + node.id + '"></div>').appendTo(instanceEl);
    var normalSectionEl = $('<div class="instance-normal-section" data-nodeid="' + node.id + '"></div>').appendTo(instanceEl);
    if (node.children) {
      var trailerEl = $('<div class="instance-trailer" data-nodeid="' + node.id + '"><div><span class="glyphicon glyphicon-chevron-left" title="Drag and drop replicas of this instance"></span></div></div>').appendTo(instanceEl);
      instanceEl.data("instance-trailer", trailerEl);
      var numReplicas = 0;
      node.children.forEach(function(replica) {
        if (replica.isAggregate) {
          numReplicas += replica.aggregatedInstances.length;
        } else {
          numReplicas += 1;
        }
      });
      var numReplicasMessage = ((numReplicas == 1) ? "1 replica" : "" + numReplicas + " replicas");
      trailerEl.getAppend(".instance-trailer-title").text(numReplicasMessage);
      trailerEl.getAppend(".instance-trailer-content").text("Drag to move replicas");
    }
    if (isColorizeDC()) {
      var dcColor = dcColorsMap[node.DataCenter];
      $(instanceEl).css("border-color", dcColor);
      $(instanceEl).css("border-width", 2);

      var trailerEl = $(instanceEl).data("instance-trailer");
      if (trailerEl) {
        $(trailerEl).css("border-color", dcColor);
        $(trailerEl).css("border-width", 2);
        $(trailerEl).addClass("colorized");
      }
    }

    activateInstanceDraggable(instanceEl);
    prepareInstanceDroppable(normalSectionEl);
    prepareInstanceMasterSectionDroppable(masterSectionEl);
  }

  function instanceEl_getTrailerEl(instanceEl) {
    return instanceEl.find(".instance-trailer").not(".ui-draggable-dragging");
  }



  function wireInstanceCommands() {
    $("body").on("click", ".instance h3 .instance-glyphs", function(e) {
      var target = $(e.target);
      e.draggedNodeId = target.attr("data-nodeid");
      if (e.draggedNodeId == $(".instance").attr("data-nodeid"))
        return;
      openNodeModal(_instancesMap[draggedNodeId]);
      return false;
    });

    $("body").on("click", ".instance a[data-command], .instance button[data-command]", function(e) {
      var target = $(e.target).closest("a");
      var instanceEl = target.closest(".instance");
      e.draggedNodeId = instanceEl.attr("data-nodeid");

      var cmd = target.attr("data-command");

      var action = _instanceCommands[cmd];
      if (action == null)
        return;
      var res = action(e);
      return res;
    });
  }



  function prepareInstanceDroppable(instanceEl) {
    var nodesMap = _instancesMap;
    instanceEl.droppable({
      accept: function(draggable) {
        // Find the objects that accept a draggable (i.e. valid droppables)
        if (!droppableIsActive) {
          return false
        }
        if (instanceEl[0] == draggable[0])
          return false;
        var draggedNodeId = draggable.attr("data-nodeid");
        var draggedNode = nodesMap[draggedNodeId];
        var targetNode = nodesMap[instanceEl.attr("data-nodeid")];
        var action = _isDraggingTrailer ? moveChildren : moveInstance;
        var acceptDrop = action(draggedNode, targetNode, false);
        var instanceDiv = $(this).closest(".instance");
        instanceDiv.addClass("accept_drop_check");
        if (acceptDrop.accept == "ok") {
          instanceDiv.addClass("accept_drop");
        }
        if (acceptDrop.accept == "warning") {
          instanceDiv.addClass("accept_drop_warning");
        }
        $(this).attr("data-drop-comment", acceptDrop.accept ? acceptDrop.type : "");
        var accepted = acceptDrop.accept != null;
        return accepted;
      },
      hoverClass: "draggable-hovers",
      over: function(event, ui) {
        _countDragOver++;
        var duplicate = ui.helper;
        // Called once when dragged object is over another object
        if ($(this).attr("data-drop-comment")) {
          $(duplicate).addClass("draggable-msg");
          $(duplicate).find(".instance-content,.instance-trailer-content").html($(this).attr("data-drop-comment"))
        } else {
          $(duplicate).find(".instance-content,.instance-trailer-content").html('<span class="glyphicon glyphicon-minus-sign text-danger"></span> Cannot drop here')
        }
      },
      out: function(event, ui) {
        _countDragOver--;
        if (_countDragOver > 0) {
          return;
        }
        var duplicate = ui.helper;
        // Called once when dragged object leaves other object
        $(duplicate).removeClass("draggable-msg");
        $(duplicate).find(".instance-content,.instance-trailer-content").html("")
      },
      drop: function(e, ui) {
        var draggedNodeId = ui.draggable.attr("data-nodeid");
        var action = _isDraggingTrailer ? moveChildren : moveInstance;
        var duplicate = ui.helper;
        action(nodesMap[draggedNodeId], nodesMap[$(this).attr("data-nodeid")], true);
        clearDroppable();
      }
    });

  }

  function prepareInstanceMasterSectionDroppable(instanceMasterSectionEl) {
    var nodesMap = _instancesMap;
    instanceMasterSectionEl.droppable({
      accept: function(draggable) {
        // Find the objects that accept a draggable (i.e. valid droppables)
        if (!droppableIsActive) {
          return false
        }
        if (instanceMasterSectionEl[0] == draggable[0])
          return false;
        var draggedNodeId = draggable.attr("data-nodeid");
        var draggedNode = nodesMap[draggedNodeId];
        var targetNode = nodesMap[instanceMasterSectionEl.attr("data-nodeid")];
        var action = _isDraggingTrailer ? moveChildren : moveInstanceOnMaster;
        var acceptDrop = action(draggedNode, targetNode, false);
        var instanceDiv = $(this).closest(".instance");
        instanceDiv.addClass("accept_drop_check");
        if (acceptDrop.accept == "ok") {
          instanceDiv.addClass("accept_drop");
        }
        if (acceptDrop.accept == "warning") {
          instanceDiv.addClass("accept_drop_warning");
        }
        $(this).attr("data-drop-comment", acceptDrop.accept ? acceptDrop.type : "");
        var accepted = acceptDrop.accept != null;
        return accepted;
      },
      hoverClass: "draggable-hovers",
      over: function(event, ui) {
        _countDragOver += 1;
        var duplicate = ui.helper;
        // Called once when dragged object is over another object
        if ($(this).attr("data-drop-comment")) {
          $(duplicate).addClass("draggable-msg");
          $(duplicate).find(".instance-content,.instance-trailer-content").html($(this).attr("data-drop-comment"))
        } else {
          $(duplicate).find(".instance-content,.instance-trailer-content").html('<span class="glyphicon glyphicon-minus-sign text-danger"></span> Cannot drop here')
        }
      },
      out: function(event, ui) {
        _countDragOver--;
        if (_countDragOver > 0) {
          return;
        }
        var duplicate = ui.helper;
        // Called once when dragged object leaves other object
        $(duplicate).removeClass("draggable-msg");
        $(duplicate).find(".instance-content,.instance-trailer-content").html("")
      },
      drop: function(e, ui) {
        var draggedNodeId = ui.draggable.attr("data-nodeid");
        var duplicate = ui.helper;
        var action = _isDraggingTrailer ? moveChildren : moveInstanceOnMaster;
        action(nodesMap[draggedNodeId], nodesMap[$(this).attr("data-nodeid")], true);
        clearDroppable();
      }
    });

  }

  // moveInstance checks whether an instance (node) can be dropped on another (droppableNode).
  // The function consults with the current moveInstanceMethod; the type of action taken is based on that.
  // For example, actions can be repoint, match-below, relocate, move-up, take-master etc.
  // When shouldApply is false nothing gets executed, and the function merely serves as a predictive
  // to the possibility of the drop.
  function moveInstance(node, droppableNode, shouldApply) {
    if (!isAuthorizedForAction()) {
      // Obviously this is also checked on server side, no need to try stupid hacks
      return {
        accept: false
      };
    }
    var droppableTitle = getInstanceDiv(droppableNode.id).find("h3 .pull-left").html();
    if (moveInstanceMethod == "smart") {
      // Moving via GTID or Pseudo GTID
      if (node.hasConnectivityProblem || droppableNode.hasConnectivityProblem || droppableNode.isAggregate) {
        // Obviously can't handle.
        return {
          accept: false
        };
      }
      if (droppableNode.MasterKey.Hostname && droppableNode.MasterKey.Hostname != "_") {
        // droppableNode has master
        if (!droppableNode.LogReplicationUpdatesEnabled) {
          // Obviously can't handle.
          return {
            accept: false
          };
        }
        // It's OK for the master itself to not have log_slave_updates
      }

      if (node.id == droppableNode.id) {
        return {
          accept: false
        };
      }
      if (instanceIsChild(droppableNode, node) && node.isMaster && !node.isCoMaster) {
        if (node.hasProblem) {
          return {
            accept: false
          };
        }
        if (shouldApply) {
          makeCoMaster(node, droppableNode);
        }
        return {
          accept: "ok",
          type: '<span class="glyphicon glyphicon-exclamation-sign text-warning"></span> <strong>MAKE CO MASTER</strong> with ' + droppableTitle,
        };
      }
      if (instanceIsDescendant(droppableNode, node)) {
        // Wrong direction!
        return {
          accept: false
        };
      }
      if (node.isAggregate) {
        if (shouldApply) {
          relocateReplicas(node.masterNode, droppableNode, node.aggregatedInstancesPattern);
        }
        return {
          accept: "warning",
          type: "relocate [" + node.aggregatedInstances.length + "] < " + droppableTitle
        };
      }
      // the general case
      if (shouldApply) {
        relocate(node, droppableNode);
      }
      return {
        accept: "warning",
        type: "relocate < " + droppableTitle
      };
    }
    var gtidBelowFunc = null;
    var gtidOperationName = "";
    if (moveInstanceMethod == "pseudo-gtid") {
      gtidBelowFunc = matchBelow;
      gtidOperationName = "match";
    }
    if (moveInstanceMethod == "gtid") {
      gtidBelowFunc = moveBelowGTID;
      gtidOperationName = "move:gtid";
    }
    if (gtidBelowFunc != null) {
      // Moving via GTID or Pseudo GTID
      if (node.hasConnectivityProblem || droppableNode.hasConnectivityProblem || droppableNode.isAggregate) {
        // Obviously can't handle.
        return {
          accept: false
        };
      }
      if (node.isAggregate) {
        return {
          accept: false
        };
      }
      if (droppableNode.MasterKey.Hostname && droppableNode.MasterKey.Hostname != "_") {
        // droppableNode has master
        if (!droppableNode.LogReplicationUpdatesEnabled) {
          // Obviously can't handle.
          return {
            accept: false
          };
        }
        // It's OK for the master itself to not have log_slave_updates
      }

      if (node.id == droppableNode.id) {
        return {
          accept: false
        };
      }
      if (instanceIsChild(droppableNode, node) && node.isMaster && !node.isCoMaster) {
        if (node.hasProblem) {
          return {
            accept: false
          };
        }
        if (shouldApply) {
          makeCoMaster(node, droppableNode);
        }
        return {
          accept: "ok",
          type: '<span class="glyphicon glyphicon-exclamation-sign text-warning"></span> <strong>MAKE CO MASTER</strong> with ' + droppableTitle,
        };
      }
      if (instanceIsDescendant(droppableNode, node)) {
        // Wrong direction!
        return {
          accept: false
        };
      }
      if (instanceIsDescendant(node, droppableNode)) {
        // clearly node cannot be more up to date than droppableNode
        if (shouldApply) {
          gtidBelowFunc(node, droppableNode);
        }
        return {
          accept: "ok",
          type: gtidOperationName + " " + droppableTitle
        };
      }
      if (isReplicationBehindSibling(node, droppableNode)) {
        // verified that node isn't more up to date than droppableNode
        if (shouldApply) {
          gtidBelowFunc(node, droppableNode);
        }
        return {
          accept: "ok",
          type: gtidOperationName + " " + droppableTitle
        };
      }
      // the general case, where there's no clear family connection, meaning we cannot infer
      // which instance is more up to date. It's under the user's responsibility!
      if (shouldApply) {
        gtidBelowFunc(node, droppableNode);
      }
      return {
        accept: "warning",
        type: gtidOperationName + " " + droppableTitle
      };
    }
    if (moveInstanceMethod == "classic") {
      // Not pseudo-GTID mode, non GTID mode
      if (node.id == droppableNode.id) {
        return {
          accept: false
        };
      }
      if (node.isAggregate) {
        return {
          accept: false
        };
      }
      if (instanceIsChild(droppableNode, node) && node.isCoMaster) {
        // We may allow a co-master to change its other co-master under some conditions,
        // see MakeCoMaster() in instance_topology.go
        if (!droppableNode.ReadOnly) {
          return {
            accept: false
          };
        }
        var coMaster = node.masterNode;
        if (coMaster.id == droppableNode.id) {
          return {
            accept: false
          };
        }
        if (coMaster.lastCheckInvalidProblem() || coMaster.notRecentlyCheckedProblem() || coMaster.ReadOnly) {
          if (shouldApply) {
            makeCoMaster(node, droppableNode);
          }
          return {
            accept: "ok",
            type: '<span class="glyphicon glyphicon-exclamation-sign text-warning"></span> <strong>MAKE CO MASTER</strong> with ' + droppableTitle,
          };
        }
      }
      if (node.isCoMaster) {
        return {
          accept: false
        };
      }
      if (instancesAreSiblings(node, droppableNode)) {
        if (node.hasProblem || droppableNode.hasProblem || droppableNode.isAggregate || !droppableNode.LogReplicationUpdatesEnabled) {
          return {
            accept: false
          };
        }
        if (shouldApply) {
          moveBelow(node, droppableNode);
        }
        return {
          accept: "ok",
          type: "moveBelow " + droppableTitle
        };
      }
      if (instanceIsGrandchild(node, droppableNode)) {
        if (node.hasProblem) {
          // Typically, when a node has a problem we do not allow moving it up.
          // But there's a special situation when allowing is desired: when the parent has personal issues,
          // (say disk issue or otherwise something heavyweight running which slows down replication)
          // and you want to move up the replica which is only delayed by its master.
          // So to help out, if the instance is identically at its master's trail, it is allowed to move up.
          if (!node.isSQLThreadCaughtUpWithIOThread) {
            return {
              accept: false
            };
          }
        }
        if (shouldApply) {
          moveUp(node, droppableNode);
        }
        return {
          accept: "ok",
          type: "moveUp under " + droppableTitle
        };
      }
      if (instanceIsChild(node, droppableNode) && !droppableNode.isMaster) {
        if (node.hasProblem) {
          // Typically, when a node has a problem we do not allow moving it up.
          // But there's a special situation when allowing is desired: when
          // this replica is completely caught up;
          if (!node.isSQLThreadCaughtUpWithIOThread) {
            return {
              accept: false
            };
          }
        }
        if (shouldApply) {
          takeMaster(node, droppableNode);
        }
        return {
          accept: "ok",
          type: "takeMaster " + droppableTitle
        };
      }
      if (instanceIsChild(droppableNode, node) && node.isMaster && !node.isCoMaster) {
        if (node.hasProblem) {
          return {
            accept: false
          };
        }
        if (shouldApply) {
          makeCoMaster(node, droppableNode);
        }
        return {
          accept: "ok",
          type: '<span class="glyphicon glyphicon-exclamation-sign text-warning"></span> <strong>MAKE CO MASTER</strong> with ' + droppableTitle,
        };
      }
      return {
        accept: false
      };
    }
    if (shouldApply) {
      addAlert(
        "Cannot move <code><strong>" +
        node.Key.Hostname + ":" + node.Key.Port +
        "</strong></code> under <code><strong>" +
        droppableNode.Key.Hostname + ":" + droppableNode.Key.Port +
        "</strong></code>. " +
        "You may only move a node down below its sibling or up below its grandparent."
      );
    }
    return {
      accept: false
    };
  }


  function moveInstanceOnMaster(node, droppableNode, shouldApply) {
    var unaccepted = {
      accept: false
    };
    if (!isAuthorizedForAction()) {
      // Obviously this is also checked on server side, no need to try stupid hacks
      return unaccepted;
    }
    var droppableTitle = getInstanceDiv(droppableNode.id).find("h3 .pull-left").html();

    if (node.hasConnectivityProblem || droppableNode.hasConnectivityProblem || droppableNode.isAggregate) {
      // Obviously can't handle.
      return unaccepted;
    }
    if (instanceIsChild(node, droppableNode) && !droppableNode.isMaster && !droppableNode.isCoMaster) {
      if (node.hasProblem) {
        // Typically, when a node has a problem we do not allow moving it up.
        // But there's a special situation when allowing is desired: when
        // this replica is completely caught up;
        if (!node.isSQLThreadCaughtUpWithIOThread) {
          return {
            accept: false
          };
        }
      }
      if (shouldApply) {
        takeMaster(node, droppableNode);
      }
      return {
        accept: "ok",
        type: '<span class="glyphicon glyphicon-exclamation-sign text-warning"></span> <strong>take master</strong> ' + droppableTitle
      };
    }
    if (instanceIsChild(node, droppableNode) &&
      droppableNode.isMaster &&
      !node.isCoMaster
    ) {
      if (node.hasProblem) {
        return {
          accept: false
        };
      }
      if (shouldApply) {
        gracefulMasterTakeover(node, droppableNode);
      }
      return {
        accept: "ok",
        type: '<span class="glyphicon glyphicon-exclamation-sign text-warning"></span> <strong>PROMOTE AS MASTER</strong> '
      };
    }
    return moveInstance(node, droppableNode, shouldApply);
  }

  // moveChildren checks whether an children of an instance (node) can be dropped on another (droppableNode).
  // The function consults with the current moveInstanceMethod; the type of action taken is based on that.
  // For example, actions can be repoint-replicas, match-replicas, relocate-replicas, move-up-replicas etc.
  // When shouldApply is false nothing gets executed, and the function merely serves as a predictive
  // to the possibility of the drop.
  function moveChildren(node, droppableNode, shouldApply) {
    if (!isAuthorizedForAction()) {
      // Obviously this is also checked on server side, no need to try stupid hacks
      return {
        accept: false
      };
    }
    var droppableTitle = getInstanceDiv(droppableNode.id).find("h3 .pull-left").html();
    if (moveInstanceMethod == "smart") {
      // Moving via GTID or Pseudo GTID
      if (droppableNode.hasConnectivityProblem || droppableNode.isAggregate) {
        // Obviously can't handle.
        return {
          accept: false
        };
      }
      if (droppableNode.MasterKey.Hostname && droppableNode.MasterKey.Hostname != "_") {
        // droppableNode has master
        if (!droppableNode.LogReplicationUpdatesEnabled) {
          // Obviously can't handle.
          return {
            accept: false
          };
        }
        // It's OK for the master itself to not have log_slave_updates
      }

      if (node.id == droppableNode.id) {
        if (shouldApply) {
          relocateReplicas(node, droppableNode);
        }
        return {
          accept: "ok",
          type: "relocate < " + droppableTitle
        };
      }
      if (instanceIsDescendant(droppableNode, node) && node.children.length <= 1) {
        // Can generally move replicas onto one of them, but there needs to be at least two replicas...
        // Otherwise we;re trying to mvoe a replica under itself which is clearly an error.
        return {
          accept: false
        };
      }
      // the general case
      if (shouldApply) {
        relocateReplicas(node, droppableNode);
      }
      return {
        accept: "warning",
        type: "relocate < " + droppableTitle
      };
    }

    var gtidBelowFunc = null;
    var gtidOperationName = "";
    if (moveInstanceMethod == "pseudo-gtid") {
      gtidBelowFunc = matchReplicas;
      gtidOperationName = "match";
    }
    if (moveInstanceMethod == "gtid") {
      gtidBelowFunc = moveReplicasGTID;
      gtidOperationName = "move:gtid";
    }
    if (gtidBelowFunc != null) {
      // Moving via GTID or Pseudo GTID
      if (droppableNode.hasConnectivityProblem || droppableNode.isAggregate) {
        // Obviously can't handle.
        return {
          accept: false
        };
      }
      if (droppableNode.MasterKey.Hostname && droppableNode.MasterKey.Hostname != "_") {
        // droppableNode has master
        if (!droppableNode.LogReplicationUpdatesEnabled) {
          // Obviously can't handle.
          return {
            accept: false
          };
        }
        // It's OK for the master itself to not have log_slave_updates
      }
      if (node.id == droppableNode.id) {
        if (shouldApply) {
          gtidBelowFunc(node, droppableNode);
        }
        return {
          accept: "ok",
          type: gtidOperationName + " < " + droppableTitle
        };
      }
      if (instanceIsDescendant(droppableNode, node) && node.children.length <= 1) {
        // Can generally move replicas onto one of them, but there needs to be at least two replicas...
        // Otherwise we;re trying to mvoe a replica under itself which is clearly an error.
        // Wrong direction!
        return {
          accept: false
        };
      }
      if (instanceIsDescendant(node, droppableNode)) {
        // clearly node cannot be more up to date than droppableNode
        if (shouldApply) {
          gtidBelowFunc(node, droppableNode);
        }
        return {
          accept: "ok",
          type: gtidOperationName + " < " + droppableTitle
        };
      }
      // the general case, where there's no clear family connection, meaning we cannot infer
      // which instance is more up to date. It's under the user's responsibility!
      if (shouldApply) {
        gtidBelowFunc(node, droppableNode);
      }
      return {
        accept: "warning",
        type: gtidOperationName + " < " + droppableTitle
      };
    }
    if (moveInstanceMethod == "classic") {
      // Not pseudo-GTID mode, non GTID mode
      if (node.id == droppableNode.id) {
        if (shouldApply) {
          repointReplicas(node);
        }
        return {
          accept: "ok",
          type: "repointReplicas < " + droppableTitle
        };
      }
      if (instanceIsChild(node, droppableNode)) {
        if (shouldApply) {
          moveUpReplicas(node, droppableNode);
        }
        return {
          accept: "ok",
          type: "moveUpReplicas < " + droppableTitle
        };
      }
      return {
        accept: false
      };
    }
    if (shouldApply) {
      addAlert(
        "Cannot move replicas of <code><strong>" +
        node.Key.Hostname + ":" + node.Key.Port +
        "</strong></code> under <code><strong>" +
        droppableNode.Key.Hostname + ":" + droppableNode.Key.Port +
        "</strong></code>. " +
        "You may only repoint or move up the replicas of an instance. Otherwise try Smart Mode."
      );
    }
    return {
      accept: false
    };
  }


  function executeMoveOperation(message, apiUrl) {
    if (isSilentUI()) {
      apiCommand(apiUrl);
    } else {
      bootbox.confirm(anonymizeIfNeedBe(message), function(confirm) {
        if (confirm) {
          apiCommand(apiUrl);
        }
      });
    }
    $("#cluster_container .accept_drop_check").removeClass("accept_drop_check");
    $("#cluster_container .accept_drop").removeClass("accept_drop");
    $("#cluster_container .accept_drop").removeClass("accept_drop_warning");
    return false;
  }

  function relocate(node, siblingNode) {
    var message = "<h4>relocate</h4>Are you sure you wish to turn <code><strong>" +
      node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code> into a replica of <code><strong>" +
      siblingNode.Key.Hostname + ":" + siblingNode.Key.Port +
      "</strong></code>?" +
      "<h4>Note</h4><p>Orchestrator will try and figure out the best relocation path. This may involve multiple steps. " +
      "<p>In case multiple steps are involved, failure of one would leave your instance hanging in a different location than you expected, " +
      "but it would still be in a <i>valid</i> state.";
    var apiUrl = "/api/relocate/" + node.Key.Hostname + "/" + node.Key.Port + "/" + siblingNode.Key.Hostname + "/" + siblingNode.Key.Port;
    return executeMoveOperation(message, apiUrl);
  }

  function relocateReplicas(node, siblingNode, pattern) {
    pattern = pattern || "";
    var message = "<h4>relocate-replicas</h4>Are you sure you wish to relocate replicas of <code><strong>" +
      node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code> below <code><strong>" +
      siblingNode.Key.Hostname + ":" + siblingNode.Key.Port +
      "</strong></code>?" +
      "<h4>Note</h4><p>Orchestrator will try and figure out the best relocation path. This may involve multiple steps. " +
      "<p>In case multiple steps are involved, failure of one may leave some instances hanging in a different location than you expected, " +
      "but they would still be in a <i>valid</i> state.";
    var apiUrl = "/api/relocate-replicas/" + node.Key.Hostname + "/" + node.Key.Port + "/" + siblingNode.Key.Hostname + "/" + siblingNode.Key.Port + "?pattern=" + encodeURIComponent(pattern);
    return executeMoveOperation(message, apiUrl);
  }

  function repointReplicas(node, siblingNode) {
    var message = "<h4>repoint-replicas</h4>Are you sure you wish to repoint replicas of <code><strong>" +
      node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code>?";
    var apiUrl = "/api/repoint-replicas/" + node.Key.Hostname + "/" + node.Key.Port;
    return executeMoveOperation(message, apiUrl);
  }

  function moveUpReplicas(node, masterNode) {
    var message = "<h4>move-up-replicas</h4>Are you sure you wish to move up replicas of <code><strong>" +
      node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code> below <code><strong>" +
      masterNode.Key.Hostname + ":" + masterNode.Key.Port +
      "</strong></code>?";
    var apiUrl = "/api/move-up-replicas/" + node.Key.Hostname + "/" + node.Key.Port;
    return executeMoveOperation(message, apiUrl);
  }

  function matchReplicas(node, otherNode) {
    var message = "<h4>match-replicas</h4>Are you sure you wish to match replicas of <code><strong>" +
      node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code> below <code><strong>" +
      otherNode.Key.Hostname + ":" + otherNode.Key.Port +
      "</strong></code>?";
    var apiUrl = "/api/match-replicas/" + node.Key.Hostname + "/" + node.Key.Port + "/" + otherNode.Key.Hostname + "/" + otherNode.Key.Port;
    return executeMoveOperation(message, apiUrl);
  }

  function moveBelow(node, siblingNode) {
    var message = "<h4>move-below</h4>Are you sure you wish to turn <code><strong>" +
      node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code> into a replica of <code><strong>" +
      siblingNode.Key.Hostname + ":" + siblingNode.Key.Port +
      "</strong></code>?";
    var apiUrl = "/api/move-below/" + node.Key.Hostname + "/" + node.Key.Port + "/" + siblingNode.Key.Hostname + "/" + siblingNode.Key.Port;
    return executeMoveOperation(message, apiUrl);
  }

  function moveUp(node, grandparentNode) {
    var message = "<h4>move-up</h4>Are you sure you wish to turn <code><strong>" +
      node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code> into a replica of <code><strong>" +
      grandparentNode.Key.Hostname + ":" + grandparentNode.Key.Port +
      "</strong></code>?";
    var apiUrl = "/api/move-up/" + node.Key.Hostname + "/" + node.Key.Port;
    return executeMoveOperation(message, apiUrl);
  }

  function takeMaster(node, masterNode) {
    var message = "<h4>take-master</h4>Are you sure you wish to make <code><strong>" +
      node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code> master of <code><strong>" +
      masterNode.Key.Hostname + ":" + masterNode.Key.Port +
      "</strong></code>?";
    var apiUrl = "/api/take-master/" + node.Key.Hostname + "/" + node.Key.Port;
    return executeMoveOperation(message, apiUrl);
  }

  function matchBelow(node, otherNode) {
    var message = "<h4>PSEUDO-GTID MODE, match-below</h4>Are you sure you wish to turn <code><strong>" +
      node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code> into a replica of <code><strong>" +
      otherNode.Key.Hostname + ":" + otherNode.Key.Port +
      "</strong></code>?";
    var apiUrl = "/api/match-below/" + node.Key.Hostname + "/" + node.Key.Port + "/" + otherNode.Key.Hostname + "/" + otherNode.Key.Port;
    return executeMoveOperation(message, apiUrl);
  }

  function moveBelowGTID(node, otherNode) {
    var message = "<h4>GTID MODE, move-below</h4>Are you sure you wish to turn <code><strong>" +
      node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code> into a replica of <code><strong>" +
      otherNode.Key.Hostname + ":" + otherNode.Key.Port +
      "</strong></code>?";
    var apiUrl = "/api/move-below-gtid/" + node.Key.Hostname + "/" + node.Key.Port + "/" + otherNode.Key.Hostname + "/" + otherNode.Key.Port;
    return executeMoveOperation(message, apiUrl);
  }

  function moveReplicasGTID(node, otherNode) {
    var message = "<h4>GTID MODE, move-replicas</h4>Are you sure you wish to move replicas of <code><strong>" +
      node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code> below <code><strong>" +
      otherNode.Key.Hostname + ":" + otherNode.Key.Port +
      "</strong></code>?";
    var apiUrl = "/api/move-replicas-gtid/" + node.Key.Hostname + "/" + node.Key.Port + "/" + otherNode.Key.Hostname + "/" + otherNode.Key.Port;
    return executeMoveOperation(message, apiUrl);
  }

  function makeCoMaster(node, childNode) {
    var message = "<h4>make-co-master</h4>Are you sure you wish to make <code><strong>" +
      node.Key.Hostname + ":" + node.Key.Port +
      "</strong></code> and <code><strong>" +
      childNode.Key.Hostname + ":" + childNode.Key.Port +
      "</strong></code> co-masters?";
    bootbox.confirm(anonymizeIfNeedBe(message), function(confirm) {
      if (confirm) {
        apiCommand("/api/make-co-master/" + childNode.Key.Hostname + "/" + childNode.Key.Port);
        return true;
      }
    });
    return false;
  }


  function gracefulMasterTakeover(newMasterNode, existingMasterNode) {
    var message = '<h1><span class="glyphicon glyphicon-exclamation-sign text-warning"></span> DANGER ZONE</h1><h4>Graceful-master-takeover</h4>Are you sure you wish to promote <code><strong>' +
      newMasterNode.Key.Hostname + ':' + newMasterNode.Key.Port +
      '</strong></code> as master?';
    bootbox.confirm(anonymizeIfNeedBe(message), function(confirm) {
      if (confirm) {
        apiCommand("/api/graceful-master-takeover/" + existingMasterNode.Key.Hostname + "/" + existingMasterNode.Key.Port + "/" + newMasterNode.Key.Hostname + "/" + newMasterNode.Key.Port);
        return true;
      }
    });
    return false;
  }

  function instancesAreSiblings(node1, node2) {
    if (node1.id == node2.id) return false;
    if (node1.masterNode == null) return false;
    if (node2.masterNode == null) return false;
    if (node1.masterNode.id != node2.masterNode.id) return false;
    return true;
  }


  function instanceIsChild(node, parentNode) {
    if (!node.hasMaster) {
      return false;
    }
    if (node.masterNode.id != parentNode.id) {
      return false;
    }
    if (node.id == parentNode.id) {
      return false;
    }
    return true;
  }


  function instanceIsGrandchild(node, grandparentNode) {
    if (!node.hasMaster) {
      return false;
    }
    var masterNode = node.masterNode;
    if (!masterNode.hasMaster) {
      return false;
    }
    if (masterNode.masterNode.id != grandparentNode.id) {
      return false;
    }
    if (node.id == grandparentNode.id) {
      return false;
    }
    return true;
  }

  function instanceIsDescendant(node, nodeAtQuestion, depth) {
    depth = depth || 0;
    if (depth > node.ReplicationDepth + 1) {
      // Safety check for master-master topologies: avoid infinite loop
      return false;
    }
    if (nodeAtQuestion == null) {
      return false;
    }
    if (node.id == nodeAtQuestion.id) {
      return false;
    }
    if (!node.hasMaster) {
      return false;
    }
    if (node.masterNode.id == nodeAtQuestion.id) {
      return true;
    }
    return instanceIsDescendant(node.masterNode, nodeAtQuestion, depth + 1)
  }

  // Returns true when the two instances are siblings, and 'node' is behind or at same position
  // (in reltation to shared master) as its 'sibling'.
  // i.e. 'sibling' is same as, or more up to date by master than 'node'.
  function isReplicationBehindSibling(node, sibling) {
    if (!instancesAreSiblings(node, sibling)) {
      return false;
    }
    return compareInstancesExecBinlogCoordinates(node, sibling) <= 0;
  }

  function isReplicationStrictlyBehindSibling(node, sibling) {
    if (!instancesAreSiblings(node, sibling)) {
      return false;
    }
    return compareInstancesExecBinlogCoordinates(node, sibling) < 0;
  }

  function compareInstancesExecBinlogCoordinates(i0, i1) {
    if (i0.ExecBinlogCoordinates.LogFile == i1.ExecBinlogCoordinates.LogFile) {
      // executing from same master log file
      return i0.ExecBinlogCoordinates.LogPos - i1.ExecBinlogCoordinates.LogPos;
    }
    return (getLogFileNumber(i0.ExecBinlogCoordinates.LogFile) - getLogFileNumber(i1.ExecBinlogCoordinates.LogFile));

  }

  function getLogFileNumber(logFileName) {
    logFileTokens = logFileName.split(".")
    return parseInt(logFileTokens[logFileTokens.length - 1])
  }

  // compactInstances aggregates sibling instances of same DC such that they are visualized as a single box.
  function compactInstances(instances, instancesMap) {
    function aggregateInstances(parentInstance, dataCenter, instances) {
      if (!instances) {
        return false;
      }
      if (instances.length < 2) {
        return false;
      }

      var aggregatedProblems = {}

      function incrementProblems(problemType, title) {
        if (aggregatedProblems[problemType]) {
          aggregatedProblems[problemType].push(title);
        } else {
          aggregatedProblems[problemType] = [title];
        }
      }
      var instanceFullNames = [];
      instances.forEach(function(instance) {
        var instanceDescription = instance.title + " " + instance.Version;
        if (isAnonymized()) {
          instanceDescription = anonymizeInstanceId(instance.id);
        }
        instanceDescription += ", " + instance.ReplicationLagSeconds.Int64 + "s lag";
        incrementProblems("", instanceDescription)
        instanceFullNames.push(getInstanceTitle(instance.Key.Hostname, instance.Key.Port));
        instance.Problems.forEach(function(problem) {
          incrementProblems(problem, instanceDescription)
        });
      });
      var aggergateInstance = instances[0];
      aggergateInstance.isAggregate = true;
      aggergateInstance.title = "[aggregation]";
      if (dataCenter) {
        aggergateInstance.title = "[aggregation in " + dataCenter + "]";
        aggergateInstance.InstanceAlias = aggergateInstance.title;
      }
      aggergateInstance.canonicalTitle = aggergateInstance.title;
      aggergateInstance.aggregatedInstances = instances; // includes itself
      aggergateInstance.aggregatedProblems = aggregatedProblems;
      aggergateInstance.aggregatedInstancesPattern = "(" + instanceFullNames.join("|") + ")";

      instances.forEach(function(instance) {
        if (!instance.isAggregate) {
          parentInstance.children.remove(instance);
          delete instancesMap[instance.id];
        }
      });
      return true;
    }
    instances.forEach(function(instance) {
      if (!instance.children) {
        return false;
      }
      // Aggregating children who are childless
      childlessChildren = instance.children.filter(function(child) {
        return (!child.children || child.children.length == 0)
      });

      var dcInstances = {};
      childlessChildren.forEach(function(instance) {
        if (!dcInstances[instance.DataCenter]) {
          dcInstances[instance.DataCenter] = [];
        }
        dcInstances[instance.DataCenter].push(instance);
      });
      for (var dc in dcInstances) {
        if (dcInstances.hasOwnProperty(dc)) {
          aggregateInstances(instance, dc, dcInstances[dc])
        }
      }
      return true;
    });
    return instancesMap;
  }

  function analyzeClusterInstances() {
    var nodesMap = _instancesMap;
    instances = []
    for (var nodeId in nodesMap) {
      instances.push(nodesMap[nodeId]);
    }

    instances.forEach(function(instance) {
      if (!instance.hasConnectivityProblem)
        return;
      // The instance has a connectivity problem! Do a client-side recommendation of most advanced replica:
      // a direct child of the master, with largest exec binlog coordinates.
      var sortedChildren = instance.children.slice();
      sortedChildren.sort(compareInstancesExecBinlogCoordinates)

      instance.children.forEach(function(child) {
        if (!child.hasConnectivityProblem) {
          if (compareInstancesExecBinlogCoordinates(child, sortedChildren[sortedChildren.length - 1]) == 0) {
            child.isMostAdvancedOfSiblings = true;
            if (instance.isMaster && !instance.isCoMaster) {
              // Moreover, the instance is the (only) master!
              // Therefore its most advanced replicas are candidate masters
              child.isCandidateMaster = true;
            }
          }
        }
      });
    });
    instances.forEach(function(instance) {
      if (instance.children && instance.children.length > 0) {
        instance.children[0].isFirstChildInDisplay = true
      }
    });
  }

  function preVisualizeInstances() {
    var nodesMap = _instancesMap;
    // DC colors
    var knownDCs = [];
    instances.forEach(function(instance) {
      knownDCs.push(instance.DataCenter)
    });

    function uniq(a) {
      return a.sort().filter(function(item, pos) {
        return !pos || item != a[pos - 1];
      })
    }
    knownDCs = uniq(knownDCs);
    if (isColorizeDC() && !isAnonymized()) {
      $('<span>Data centers:</span>').appendTo('#cluster_legend');
    }
    for (i = 0; i < knownDCs.length; ++i) {
      var color = renderColors[i % renderColors.length]
      dcColorsMap[knownDCs[i]] = color;
      if (isColorizeDC() && !isAnonymized() && knownDCs[i]) {
        $('<span>' + knownDCs[i] + '</span>').addClass("dc").css('border-color', color).appendTo('#cluster_legend');
      }
    }
  }


  function refreshClusterOperationModeButton() {
    if (moveInstanceMethod == "smart") {
      $("#move-instance-method-button").removeClass("btn-success").removeClass("btn-primary").removeClass("btn-warning").addClass("btn-info");
    } else if (moveInstanceMethod == "classic") {
      $("#move-instance-method-button").removeClass("btn-info").removeClass("btn-primary").removeClass("btn-warning").addClass("btn-success");
    } else if (moveInstanceMethod == "gtid") {
      $("#move-instance-method-button").removeClass("btn-success").removeClass("btn-info").removeClass("btn-warning").addClass("btn-primary");
    } else if (moveInstanceMethod == "pseudo-gtid") {
      $("#move-instance-method-button").removeClass("btn-success").removeClass("btn-primary").removeClass("btn-info").addClass("btn-warning");
    }
    $("#move-instance-method-button").html(moveInstanceMethod + ' mode <span class="caret"></span>')
  }

  // This is legacy and will be removed
  function makeMaster(instance) {
    var message = "Are you sure you wish to make <code><strong>" + instance.Key.Hostname + ":" + instance.Key.Port + "</strong></code> the new master?" + "<p>Siblings of <code><strong>" + instance.Key.Hostname + ":" + instance.Key.Port + "</strong></code> will turn to be its children, " + "via Pseudo-GTID." + "<p>The instance will be set to be writeable (<code><strong>read_only = 0</strong></code>)." + "<p>Replication on this instance will be stopped, but not reset. You should run <code><strong>RESET SLAVE</strong></code> yourself " + "if this instance will indeed become the master." + "<p>Pointing your application servers to the new master is on you.";
    var apiUrl = "/api/make-master/" + instance.Key.Hostname + "/" + instance.Key.Port;
    return executeMoveOperation(message, apiUrl);
  }

  //This is legacy and will be removed
  function makeLocalMaster(instance) {
    var message = "Are you sure you wish to make <code><strong>" + instance.Key.Hostname + ":" + instance.Key.Port + "</strong></code> a local master?" + "<p>Siblings of <code><strong>" + instance.Key.Hostname + ":" + instance.Key.Port + "</strong></code> will turn to be its children, " + "via Pseudo-GTID." + "<p>The instance will replicate from its grandparent.";
    var apiUrl = "/api/make-local-master/" + instance.Key.Hostname + "/" + instance.Key.Port;
    return executeMoveOperation(message, apiUrl);
  }


  function promptForAlias(oldAlias) {
    bootbox.prompt({
      title: "Enter alias for this cluster",
      value: oldAlias,
      callback: function(result) {
        if (result !== null) {
          showLoader();
          getData("/api/set-cluster-alias/" + currentClusterName() + "?alias=" + encodeURIComponent(result), function(operationResult) {
            hideLoader();
            if (operationResult.Code == "ERROR") {
              addAlert(operationResult.Message)
            } else {
              location.reload();
            }
          });
        }
      }
    });
  }

  function showOSCReplicas() {
    getData("/api/cluster-osc-replicas/" + currentClusterName(), function(instances) {
      var instancesMap = normalizeInstances(instances, Array());
      var instancesTitles = Array();
      instances.forEach(function(instance) {
        instancesTitles.push(instance.title);
      });
      var instancesTitlesConcatenates = instancesTitles.join(" ");
      bootbox.alert("Heuristic list of OSC controller replicas: <pre>" + instancesTitlesConcatenates + "</pre>");
    });
  }

  function anonymizeIfNeedBe(message) {
    if (isAnonymized()) {
      message = message.replace(/<strong>.*?<\/strong>/g, "############");
    }
    return message;
  }

  function addSidebarInfoPopoverContent(content, tag, hr) {
    if (hr === true) {
      content = '<hr/>' + content
    }
    wrappedContent = '<div data-tag="'+tag+'">' + content + '<div style="clear: both;"></div></div>';
    $("#cluster_info").append(wrappedContent)
  }

  function populateSidebar(clusterInfo) {
    var content = '';

    {
      var content = '<button type="button" class="close" aria-hidden="true">&times;</button>';
      addSidebarInfoPopoverContent(content, "close", false);
      $("#cluster_info button.close").click(function() {
        $("#cluster_info").hide();
      });
    }
    {
      var content = currentClusterName();
      addSidebarInfoPopoverContent(content, "cluster-name", true);
    }
    {
      var content = 'Alias: ' + clusterInfo.ClusterAlias + '';
      addSidebarInfoPopoverContent(content, "cluster-alias", true);
    } {
      var content = 'Domain: ' + clusterInfo.ClusterDomain + '';
      addSidebarInfoPopoverContent(content, "cluster-domain", true);
    }

    var maxItems = 5
    getData("/api/audit-recovery/alias/" + clusterInfo.ClusterAlias, function(recoveries) {
      recoveries = recoveries || []
      recoveries = recoveries.slice(0, maxItems)
      if (recoveries.length > 0) {
        var content = '<a href="' + appUrl('/web/audit-recovery/alias/' + clusterInfo.ClusterAlias) + '">Recovery history</a>';
        addSidebarInfoPopoverContent(content, "audit-recovery-title", true);
      }
      recoveries.forEach(function(recovery) {
        var glyph = '<span class="glyphicon text-success glyphicon-ok-sign"></span>';
        if (recovery.IsSuccessful === false) {
          glyph = '<span class="glyphicon text-danger glyphicon-remove-sign"></span>';
        }
        var content = '<a href="/web/audit-recovery/uid/'+recovery.UID+'">' + recovery.RecoveryStartTimestamp + '</a>: ' + glyph + ' ' + recovery.AnalysisEntry.Analysis
        addSidebarInfoPopoverContent(content, "audit-recovery", true);
      });
    });
    getData("/api/audit-failure-detection/alias/" + clusterInfo.ClusterAlias, function(failureDetections) {
      failureDetections = failureDetections || []
      failureDetections = failureDetections.slice(0, maxItems)
      if (failureDetections.length > 0) {
        var content = '<a href="' + appUrl('/web/audit-failure-detection/alias/' + clusterInfo.ClusterAlias) + '">Failure detection</a>';
        addSidebarInfoPopoverContent(content, "audit-detection-title", true);
      }
      failureDetections.forEach(function(failureDetection) {
        var content = failureDetection.RecoveryStartTimestamp + ': ' + failureDetection.AnalysisEntry.Analysis
        addSidebarInfoPopoverContent(content, "audit-detection", true);
      });
    });
    // Colorize-dc
    {
      var glyph = $("#cluster_sidebar [data-bullet=colorize-dc] .glyphicon");
      if (isColorizeDC()) {
        glyph.addClass("text-info");
        glyph.attr("title", "Disable colors");
      } else {
        glyph.addClass("text-muted");
        glyph.attr("title", "Color by data center");
      }
    }
    // Compact display
    {
      var anchor = $("#cluster_sidebar [data-bullet=compact-display] a");
      var glyph = $(anchor).find(".glyphicon")
      if (isCompactDisplay()) {
        glyph.addClass("text-info");
        glyph.attr("title", "Disable compact display");
      } else {
        glyph.addClass("text-muted");
        glyph.attr("title", "Enable compact display");
      }
    }
    // Pool indicator
    {
      var glyph = $("#cluster_sidebar [data-bullet=pool-indicator] .glyphicon");
      if ($.cookie("pool-indicator") == "true") {
        glyph.addClass("text-info");
        glyph.attr("title", "Disable pool indication");
      } else {
        glyph.addClass("text-muted");
        glyph.attr("title", "Enable pool indication");
      }
    }
    // Anonymize
    {
      var glyph = $("#cluster_sidebar [data-bullet=anonymize] .glyphicon");
      if (isAnonymized()) {
        glyph.addClass("text-info");
        glyph.attr("title", "Cancel anonymize");
      } else {
        glyph.addClass("text-muted");
        glyph.attr("title", "Anonymize display");
      }
    }
    // Alias
    {
      var glyph = $("#cluster_sidebar [data-bullet=alias] .glyphicon");
      var is = isAliased();
      glyph.addClass(is ? "text-info" : "text-muted");
      glyph.attr("title", is ? "Cancel alias" : "Instance alias display");
    }
    // Silent UI
    {
      var glyph = $("#cluster_sidebar [data-bullet=silent-ui] .glyphicon");
      if (isSilentUI()) {
        glyph.addClass("text-info");
        glyph.attr("title", "Cancel UI silence");
      } else {
        glyph.addClass("text-muted");
        glyph.attr("title", "Silence UI questions");
      }
    }
  }

  function onAnalysisEntry(analysisEntry, instance) {
    var glyph = '';
    var hasDowntime = analysisEntry.IsDowntimed || analysisEntry.IsReplicasDowntimed
    if (analysisEntry.IsStructureAnalysis) {
      glyph = '<span class="pull-left glyphicon glyphicon-exclamation-sign '+(hasDowntime ? "text-muted" : "text-warning")+'"></span>';
    } else {
      glyph = '<span class="pull-left glyphicon glyphicon-exclamation-sign '+(hasDowntime ? "text-muted" : "text-danger")+'"></span>';
    }
    var analysisContent = '<div><strong>' + analysisEntry.Analysis + "</strong></div>";
    var extraText = '';
    if  (analysisEntry.IsDowntimed) {
      extraText = '<i>downtime till ' + analysisEntry.DowntimeEndTimestamp + '</i>';
    } else if (analysisEntry.IsReplicasDowntimed) {
      extraText = '<i>replicas downtimed</i>';
    }
    if (extraText != '') {
      analysisContent += '<div>' + extraText + '</div>';
    }
    analysisContent += "<div>" + analysisEntry.AnalyzedInstanceKey.Hostname + ":" + analysisEntry.AnalyzedInstanceKey.Port + "</div>";
    var content = '<div><div class="pull-left">'+glyph+'</div><div class="pull-right">'+analysisContent+'</div></div>';
    addSidebarInfoPopoverContent(content, "analysis", false);
    if (analysisEntry.IsStructureAnalysis) {
      return;
    }
    var popoverElement = getInstanceDiv(instance.id);

    popoverElement.append('<h4 class="popover-footer"><div class="dropdown"></div></h4>');
    popoverElement.find(".popover-footer .dropdown").append('<button type="button" class="btn btn-xs btn-default dropdown-toggle" id="recover_dropdown_' + instance.id + '" data-toggle="dropdown" aria-haspopup="true" aria-expanded="true"><span class="glyphicon glyphicon-heart text-danger"></span> Recover <span class="caret"></span></button><ul class="dropdown-menu" aria-labelledby="recover_dropdown_' + instance.id + '"></ul>');
    popoverElement.find(".popover-footer .dropdown").append('<ul class="dropdown-menu" aria-labelledby="recover_dropdown_' + instance.id + '"></ul>');
    var recoveryListing = popoverElement.find(".dropdown ul");

    if (instance.isMaster) {
      recoveryListing.append('<li><a href="#" data-btn="force-master-failover" data-command="force-master-failover"><div class="glyphicon glyphicon-exclamation-sign text-danger"></div> <span class="text-danger">Force fail over <strong>now</strong> (even if normal handling would not fail over)</span></a></li>');
      recoveryListing.append('<li role="separator" class="divider"></li>');

      // Suggest successor
      instance.children.forEach(function(replica) {
        if (!replica.LogBinEnabled) {
          return
        }
        if (replica.SQLDelay > 0) {
          return
        }
        if (!replica.LogReplicationUpdatesEnabled) {
          return
        }
        if (replica.lastCheckInvalidProblem()) {
          return
        }
        if (replica.notRecentlyCheckedProblem()) {
          return
        }
        recoveryListing.append(
          '<li><a href="#" data-btn="recover-suggested-successor" data-command="recover-suggested-successor" data-successor-host="' + replica.Key.Hostname + '" data-successor-port="' + replica.Key.Port + '">Recover, try to promote <code>' + replica.title + '</code></a></li>');
      });
    }
    if (!instance.isMaster) {
      recoveryListing.append('<li><a href="#" data-btn="auto" data-command="recover-auto">Auto (implies running external hooks/processes)</a></li>');
      recoveryListing.append('<li role="separator" class="divider"></li>');
      recoveryListing.append('<li><a href="#" data-btn="relocate-replicas" data-command="relocate-replicas" data-successor-host="' + instance.MasterKey.Hostname + '" data-successor-port="' + instance.MasterKey.Port + '">Relocate replicas to <code>' + instance.masterTitle + '</code></a></li>');
    }
    if (instance.masterNode) {
      // Intermediate master; suggest successor
      instance.masterNode.children.forEach(function(sibling) {
        if (sibling.id == instance.id) {
          return
        }
        if (!sibling.LogBinEnabled) {
          return
        }
        if (!sibling.LogReplicationUpdatesEnabled) {
          return
        }
        if (sibling.lastCheckInvalidProblem()) {
          return
        }
        if (sibling.notRecentlyCheckedProblem()) {
          return
        }
        recoveryListing.append(
          '<li><a href="#" data-btn="relocate-replicas" data-command="relocate-replicas" data-successor-host="' + sibling.Key.Hostname + '" data-successor-port="' + sibling.Key.Port + '">Relocate replicas to <code>' + sibling.title + '</code></a></li>');
      });
    }
  }

  function reviewReplicationAnalysis(replicationAnalysis) {
    var instancesMap = _instancesMap;
    var clusterHasReplicationAnalysisIssue = false;
    var allIssuesAreDowntimed = true;
    var clusterHasStructureAnalysisIssue = false;
    replicationAnalysis.Details.forEach(function(analysisEntry) {
      if (analysisEntry.ClusterDetails.ClusterName != currentClusterName()) {
        return;
      }
      var hasDowntime = analysisEntry.IsDowntimed || analysisEntry.IsReplicasDowntimed
      if (!hasDowntime) {
        allIssuesAreDowntimed = false;
      }
      var instanceId = getInstanceId(analysisEntry.AnalyzedInstanceKey.Hostname, analysisEntry.AnalyzedInstanceKey.Port);
      var instance = instancesMap[instanceId]
      if (analysisEntry.Analysis in interestingAnalysis) {
        clusterHasReplicationAnalysisIssue = true;
        onAnalysisEntry(analysisEntry, instance);
      }
      analysisEntry.StructureAnalysis = analysisEntry.StructureAnalysis || [];
      analysisEntry.StructureAnalysis.forEach(function(structureAnalysis) {
        clusterHasStructureAnalysisIssue = true;
        analysisEntry.Analysis = structureAnalysis;
        analysisEntry.IsStructureAnalysis = true;
        onAnalysisEntry(analysisEntry, instance);
      });
    });
    if (clusterHasReplicationAnalysisIssue) {
      var iconClass = (allIssuesAreDowntimed ? "text-muted" : "text-danger");
      $("#cluster_sidebar [data-bullet=info] div span").addClass(iconClass).addClass("glyphicon-exclamation-sign");;
    } else if (clusterHasStructureAnalysisIssue) {
      var iconClass = (allIssuesAreDowntimed ? "text-muted" : "text-warning");
      $("#cluster_sidebar [data-bullet=info] div span").addClass(iconClass).addClass("glyphicon-exclamation-sign");;
    } else {
      $("#cluster_sidebar [data-bullet=info] div span").addClass("text-info").addClass("glyphicon-info-sign");
    }
  }


  function indicateClusterPoolInstances(clusterPoolInstances) {
    var instancesMap = _instancesMap;
    for (var pool in clusterPoolInstances.Details) {
      if (clusterPoolInstances.Details.hasOwnProperty(pool)) {
        clusterPoolInstances.Details[pool].forEach(function(instanceKey) {
          var instanceId = getInstanceId(instanceKey.Hostname, instanceKey.Port)
          var instance = instancesMap[instanceId];
          if (!instance.IsInPool) {
            instance.IsInPool = true;
            getInstanceDiv(instance.id).find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-tint" title="pools:"></span> ');
          }
          var indicatorElement = getInstanceDiv(instance.id).find("h3 div.pull-right span.glyphicon-tint");
          indicatorElement.attr("title", indicatorElement.attr("title") + " " + pool);
        });
      }
    }
  }

  function main() {
    $(domReady);
  }

  function renderCluster() {
    var instances = _instances;
    var replicationAnalysis = _replicationAnalysis;
    var maintenanceList = _maintenanceList;
    _instancesMap = normalizeInstances(instances, maintenanceList);
    if (isCompactDisplay()) {
      _instancesMap = compactInstances(instances, _instancesMap);
    }
    analyzeClusterInstances();
    preVisualizeInstances();
    visualizeInstances(_instancesMap, generateInstanceDiv, _this);
    wireInstanceCommands();

    //prepareDraggable();

    reviewReplicationAnalysis(replicationAnalysis);

    instances.forEach(function(instance) {
      if (instance.isMaster) {
        getData("/api/recently-active-instance-recovery/" + instance.Key.Hostname + "/" + instance.Key.Port, function(recoveries) {
          if (!recoveries) {
            return
          }
          // Result is an array: either empty (no active recovery) or with multiple entries
          var recoveryEntry = recoveries[0];
          addInfo('<strong>' + instance.title + '</strong> has just recently (' + recoveryEntry.RecoveryEndTimestamp + ') been promoted as result of <strong>' + recoveryEntry.AnalysisEntry.Analysis + '</strong>. It may still take some time to rebuild topology graph.');
        });
      }
    });
    if ($.cookie("pool-indicator") == "true") {
      getData("/api/cluster-pool-instances/" + currentClusterName(), function(clusterPoolInstances) {
        indicateClusterPoolInstances(clusterPoolInstances);
      });
    }
    if (reloadPageHint.hint == "refresh") {
      var instanceId = getInstanceId(reloadPageHint.hostname, reloadPageHint.port);
      var instance = _instancesMap[instanceId]
      if (instance) {
        openNodeModal(instance);
      }
    }
  }

  function domReady() {
    getData("/api/cluster/" + currentClusterName(), function(instances) {
      _instances = instances;
      getData("/api/replication-analysis/" + currentClusterName(), function(replicationAnalysis) {
        _replicationAnalysis = replicationAnalysis;
        getData("/api/maintenance", function(maintenanceList) {
          _maintenanceList = maintenanceList;
          $(document).trigger('orchestrator:preRenderCluster');
          renderCluster();
          $(document).trigger('orchestrator:postRenderCluster');
        });
      });
    });
    getData("/api/cluster-info/" + currentClusterName(), function(clusterInfo) {
      var alias = clusterInfo.ClusterAlias
      var visualAlias = (alias ? alias : currentClusterName())
      document.title = document.title.split(" - ")[0] + " - " + visualAlias;

      if (!isAnonymized()) {
        $("#cluster_name").text(visualAlias);
        var clusterSubtitle = '';
        if (clusterInfo.HasAutomatedMasterRecovery === true) {
          clusterSubtitle += '<span class="glyphicon glyphicon-heart text-info" title="Automated master recovery for this cluster ENABLED"></span>';
        } else {
          clusterSubtitle += '<span class="glyphicon glyphicon-heart text-muted pull-right" title="Automated master recovery for this cluster DISABLED"></span>';
        }
        if (clusterInfo.HasAutomatedIntermediateMasterRecovery === true) {
          clusterSubtitle += '<span class="glyphicon glyphicon-heart-empty text-info" title="Automated intermediate master recovery for this cluster ENABLED"></span>';
        } else {
          clusterSubtitle += '<span class="glyphicon glyphicon-heart-empty text-muted pull-right" title="Automated intermediate master recovery for this cluster DISABLED"></span>';
        }
        $("#cluster_subtitle").append(clusterSubtitle)


        $("#dropdown-context").append('<li><a data-command="change-cluster-alias" data-alias="' + clusterInfo.ClusterAlias + '">Alias: ' + alias + '</a></li>');
      }
      $("#dropdown-context").append('<li><a href="' + appUrl('/web/cluster-pools/' + currentClusterName()) + '">Pools</a></li>');
      if (isCompactDisplay()) {
        $("#dropdown-context").append('<li><a data-command="expand-display" href="' + location.href.split("?")[0].split("#")[0] + '?compact=false"><span class="glyphicon glyphicon-ok small"></span> Compact display</a></li>');
      } else {
        $("#dropdown-context").append('<li><a data-command="compact-display" href="' + location.href.split("?")[0].split("#")[0] + '?compact=true">Compact display</a></li>');
      }
      $("#dropdown-context").append('<li><a data-command="pool-indicator">Pool indicator</a></li>');
      $("#dropdown-context").append('<li><a data-command="colorize-dc">Colorize DC</a></li>');
      $("#dropdown-context").append('<li><a data-command="anonymize">Anonymize</a></li>');
      $("#dropdown-context").append('<li><a data-command="alias">Alias</a></li>');
      if ($.cookie("pool-indicator") == "true") {
        $("#dropdown-context a[data-command=pool-indicator]").prepend('<span class="glyphicon glyphicon-ok small"></span> ');
      }
      if (isAnonymized()) {
        $("#dropdown-context a[data-command=anonymize]").prepend('<span class="glyphicon glyphicon-ok small"></span> ');
      }
      if (isAliased()) {
        $("#dropdown-context a[data-command=alias]").prepend('<span class="glyphicon glyphicon-ok small"></span> ');
      }
      if (isColorizeDC()) {
        $("#dropdown-context a[data-command=colorize-dc]").prepend('<span class="glyphicon glyphicon-ok small"></span> ');
      }
      populateSidebar(clusterInfo);
    });

    getData("/api/active-cluster-recovery/" + currentClusterName(), function(recoveries) {
      // Result is an array: either empty (no active recovery) or with multiple entries
      recoveries.forEach(function(recoveryEntry) {
        addInfo('<strong><a href="' + appUrl('/web/audit-recovery/cluster/' + currentClusterName()) + '">' + recoveryEntry.AnalysisEntry.Analysis + ' active recovery in progress</strong></a>. Topology is subject to change in the next moments.');
      });
    });
    getData("/api/recently-active-cluster-recovery/" + currentClusterName(), function(recoveries) {
      if (!recoveries) {
        return
      }
      // Result is an array: either empty (no active recovery) or with multiple entries
      var recoveryEntry = recoveries[0]
      addInfo('This cluster just recently (' + recoveryEntry.RecoveryEndTimestamp + ') recovered from <strong><a href="' + appUrl('/web/audit-recovery/cluster/' + currentClusterName()) + '">' + recoveryEntry.AnalysisEntry.Analysis + '</strong></a>. It may still take some time to rebuild topology graph.');
    });
    getData("/api/blocked-recoveries/cluster/" + currentClusterName(), function(blockedRecoveries) {
      // Result is an array: either empty (no active recovery) or with multiple entries
      blockedRecoveries.forEach(function(blockedRecovery) {
        addAlert('A <strong>' + blockedRecovery.Analysis + '</strong> on ' + getInstanceTitle(blockedRecovery.FailedInstanceKey.Hostname, blockedRecovery.FailedInstanceKey.Port) + ' is blocked due to a <a href="' + appUrl('/web/audit-recovery/id/' + blockedRecovery.BlockingRecoveryId) + '">previous recovery</a>');
      });
    });

    $("#li-move-instance-method").appendTo("ul.navbar-nav").show();
    $("#move-instance-method a").click(function() {
      moveInstanceMethod = $(this).attr("data-method");
      refreshClusterOperationModeButton();
      $.cookie("move-instance-method", moveInstanceMethod, {
        path: '/',
        expires: 1
      });
    });
    $("#instance_problems_button").attr("title", "Cluster Problems");

    $("body").on("click", "a[data-command=change-cluster-alias]", function(event) {
      promptForAlias($(event.target).attr("data-alias"));
    });
    $("body").on("click", "a[data-command=cluster-osc-replicas]", function(event) {
      showOSCReplicas();
    });
    $("body").on("click", "a[data-command=pool-indicator]", function(event) {
      if ($.cookie("pool-indicator") == "true") {
        $.cookie("pool-indicator", "false", {
          path: '/',
          expires: 1
        });
        location.reload();
        return
      }
      $.cookie("pool-indicator", "true", {
        path: '/',
        expires: 1
      });
      location.reload();
    });
    $("body").on("click", "a[data-command=anonymize]", function(event) {
      if (isAnonymized()) {
        $.cookie("anonymize", "false", {
          path: '/',
          expires: 1
        });
        location.reload();
        return
      }
      $.cookie("anonymize", "true", {
        path: '/',
        expires: 1
      });
      location.reload();
    });
    $("body").on("click", "a[data-command=alias]", function(event) {
      $.cookie("alias", isAliased() ? "false" : "true", {
        path: '/',
        expires: 1
      });
      location.reload();
    });
    $("body").on("click", "a[data-command=info]", function(event) {
      $("#cluster_info").toggle();
      return false
    });
    $("body").on("click", "a[data-command=colorize-dc]", function(event) {
      if (isColorizeDC()) {
        $.cookie("colorize-dc", "false", {
          path: '/',
          expires: 1
        });
      } else {
        $.cookie("colorize-dc", "true", {
          path: '/',
          expires: 1
        });
      }
      location.reload();
      return
    });
    $("body").on("click", "a[data-command=compact-display]", function(event) {
      if ($.cookie("compact-display") == "true") {
        $.cookie("compact-display", "false", {
          path: '/',
          expires: 1
        });
      } else {
        $.cookie("compact-display", "true", {
          path: '/',
          expires: 1
        });
      }
      location.reload();
      return
    });
    $("body").on("click", "a[data-command=silent-ui]", function(event) {
      if ($.cookie("silent-ui") == "true") {
        $.cookie("silent-ui", "false", {
          path: '/',
          expires: 1
        });
      } else {
        $.cookie("silent-ui", "true", {
          path: '/',
          expires: 1
        });
      }
      location.reload();
      return
    });

    $("[data-toggle=popover]").popover();
    $("[data-toggle=popover]").show();

    if (isAuthorizedForAction()) {
      // Read-only users don't get auto-refresh. Sorry!
      activateRefreshTimer();
    }
    refreshClusterOperationModeButton();
  }

  $(document).keyup(function(e) {
    if (e.keyCode == 27) {
      $("#cluster_info").hide();
    }
  });

  function getData(url, cb) {
    $.get(appUrl(url), cb, "json");
  }


}

function getHtmlPos(el) {
  return {
    left: el.offsetLeft,
    top: el.offsetTop
  };
}

function getSvgPos(el) {
  var svg = $(el).closest("svg")[0];
  if (!svg) {
    return false;
  }
  var pt = svg.createSVGPoint();
  var matrix = el.getCTM();
  var box = el.getBBox();
  pt.x = box.x;
  pt.y = box.y;
  var pt2 = pt.matrixTransform(matrix);
  return {
    left: pt2.x,
    top: pt2.y
  };
}

var _page = new Cluster();
