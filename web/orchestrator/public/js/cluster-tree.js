function visualizeInstances(nodesMap, onSvgInstanceWrapper, clusterControl) {
  nodesList = []
  for (var nodeId in nodesMap) {
    nodesList.push(nodesMap[nodeId]);
  }
  var anchorVisualizationExperimentalMode = false;
  if (anchorVisualizationExperimentalMode) {
    nodesList.forEach(function(node) {
      if (node.children.length == 0) {
        return;
      }
      var realChildren = node.children;
      var pseudoChild = jQuery.extend({}, node)
      pseudoChild.children = realChildren;
      pseudoChild.children.forEach(function(child) {
        child.parent = pseudoChild;
      });
      pseudoChild.hasMaster = true;
      pseudoChild.parent = node;
      pseudoChild.isAnchor = true;
      pseudoChild.id += "__virtualnode";

      nodesMap[pseudoChild.id] = pseudoChild;
      nodesList.push(pseudoChild);
      node.children = [pseudoChild];
    });
  }
  // Calculate tree dimensions
  var maxNodeDepth = 20;
  // virtualDepth is the depth in tree excluding virtual nodes.
  // Virtual nodes are introduced as a means to present co-masters on same depth while retaining tree structure. This is
  // merely for visualization purposes. In such case, a virtual node is introduced which is the parent (though not master) of
  // said co-masters. But the virtual node is not displayed, and does not affect the visualized depth positioning of the nodes
  // underneath it.
  function getNodeDepth(node, recursiveLevel) {
    if (recursiveLevel > maxNodeDepth)
      return 0;
    if (node.virtualDepth == null) {
      if (node.parent == null) {
        node.virtualDepth = 0;
      } else {
        var parentDepth = getNodeDepth(node.parent, recursiveLevel + 1);
        node.virtualDepth = (node.parent.isVirtual || node.parent.isAnchor ? parentDepth : parentDepth + 1);
      }
    }
    return node.virtualDepth;
  }
  nodesList.forEach(function(node) {
    getNodeDepth(node, 0);
  });
  var numNodesPerDepth = {}
  nodesList.forEach(function(node) {
    if (node.virtualDepth in numNodesPerDepth) {
      numNodesPerDepth[node.virtualDepth] = numNodesPerDepth[node.virtualDepth] + 1;
    } else {
      numNodesPerDepth[node.virtualDepth] = 1;
    }
  });
  var maxDepth = 0;
  var maxNodesAtDepth = 0;
  $.each(numNodesPerDepth, function(key, value) {
    maxDepth = Math.max(maxDepth, key);
    maxNodesAtDepth = Math.max(maxNodesAtDepth, value);
  });

  var margin = {
    top: 0,
    right: 60,
    bottom: 0,
    left: 15,
  };
  var horizontalSpacing = 320;
  var verticalSpacing = 100;
  var svgWidth = $("#cluster_container").width() - margin.right - margin.left;
  svgWidth = Math.min(svgWidth, (maxDepth + 1) * horizontalSpacing);
  var svgHeight = $("#cluster_container").height() - margin.top - margin.bottom;
  svgHeight = Math.max(svgHeight, maxNodesAtDepth * verticalSpacing);

  var i = 0;
  var duration = 0;

  var tree = d3.layout.tree();
  tree = tree.size([svgHeight, svgWidth]);

  var diagonal = d3.svg.diagonal().projection(function(d) {
    return [d.y, d.x];
  });

  var svg = d3.select("#cluster_container").append("svg")
    .attr("width", svgWidth + margin.right + margin.left)
    .attr("height", svgHeight + margin.top + margin.bottom)
    .attr("xmlns", "http://www.w3.org/2000/svg").attr("version", "1.1").append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  var root = null;
  nodesList.forEach(function(node) {
    if (!node.hasMaster) {
      root = node;
    }
  });
  if (root == null) {
    nodesList.forEach(function(node) {
      if (node.isCoMaster) {
        root = node;
      }
    });
  }
  root.x0 = svgHeight / 2;
  root.y0 = 0;
  update(root);

  function update(source) {
    nodesList.where(function(t) {
      return t.x != 0;
    }).forEach(function(t) {
      t.prevX = t.x;
      t.prevY = t.y;
    });
    // Compute the new tree layout.
    var nodes = tree.nodes(root).reverse();
    var links = tree.links(nodes);
    nodesList.where(function(t) {
      return t.prevX != null;
    }).forEach(function(t) {
      t.x = t.prevX;
      t.y = t.prevY;
    });

    // Normalize for fixed-depth.
    nodes.forEach(function(d) {
      // Position on screen according to virtual-depth, not mathematical tree depth
      // (ignores virtual nodes, which are hidden)
      if (d.isAnchor) {
        d.y = (d.virtualDepth * horizontalSpacing) - horizontalSpacing / 2;
      } else {
        d.y = d.virtualDepth * horizontalSpacing;
      }
    });

    // Update the nodes…
    var node = svg.selectAll("g.node").data(nodes,
      function(d) {
        return d.id || (d.id = ++i);
      });

    // Enter any new nodes at the parent's previous position.
    var nodeEnter = node.enter().append("g").attr("class", "node").attr("transform", function(d) {
      return "translate(" + source.y0 + "," + source.x0 + ")";
    }).each(
      function() {
        this.parentNode.insertBefore(this, this.parentNode.firstChild);
      }
    );

    nodeEnter.append("circle").attr("data-nodeid", function(d) {
      if (d.isVirtual) {
        return null;
      }
      if (d.isAnchor) {
        return null;
      }
      return d.id;
    }).attr("r", 1e-6).style("fill", function(d) {
      return d._children ? "lightsteelblue" : "#fff";
    }).on("click", click);

    var fo = nodeEnter.append("g").attr("class", "svgInstanceWrapper").attr("data-fo-id", function(d) {
      return d.id
    }).attr("data-fo-is-virtual", function(d) {
      return d.isVirtual
    }).attr("data-fo-is-anchor", function(d) {
      return d.isAnchor
    }).attr("width", "100%").attr("dy", ".35em").attr("text-anchor", function(d) {
      return d.children || d._children ? "end" : "start";
    }).attr("x", function(d) {
      return 4;
    }).attr("requiredFeatures", "http://www.w3.org/TR/SVG11/feature#Extensibility");

    $("#cluster_container .instance").remove();
    $("g.svgInstanceWrapper").each(function() {
      onSvgInstanceWrapper(this, nodesMap);
    })

    // Transition nodes to their new position.
    var nodeUpdate = node
      //.transition()
      //.duration(duration)
      .attr("transform", function(d) {
        return "translate(" + d.y + "," + d.x + ")";
      });

    nodeUpdate.select("circle").attr("r", function(d) {
      if (d.isVirtual) {
        return 0;
      }
      if (d.isAnchor) {
        return 0;
      }
      return 4.5;
    }).style("fill", function(d) {
      return d._children ? "lightsteelblue" : "#fff";
    });

    // Transition exiting nodes to the parent's new position.
    var nodeExit = node.exit().transition().duration(duration).attr("transform", function(d) {
      return "translate(" + source.y + "," + source.x + ")";
    }).remove();
    nodeExit.select("circle").attr("r", 1e-6);

    // Update the links…
    var link = svg.selectAll("path.link").data(links, function(d) {
      return d.target.id;
    });

    // Enter any new links at the parent's previous position.
    link.enter().insert("path", "g").attr("class", "link").attr("d", function(d) {
      var o = {
        x: source.x0,
        y: source.y0
      };
      return diagonal({
        source: o,
        target: o
      });
    }).style("stroke-dasharray", function(o) {
      return o.source.DataCenter == o.target.DataCenter ? ("1, 0") : ("3, 3");
    });

    // Transition links to their new position.
    link.transition().duration(duration).attr("d", diagonal);

    // Transition exiting nodes to the parent's new position.
    link.exit().transition().duration(duration).attr("d", function(d) {
      var o = {
        x: source.x,
        y: source.y
      };
      return diagonal({
        source: o,
        target: o
      });
    }).remove();

    // Stash the old positions for transition.
    nodes.forEach(function(d) {
      d.x0 = d.x;
      d.y0 = d.y;
    });

    nodeUpdate.each(function(d) {
      //window.setTimeout(function(){clusterControl.repositionIntanceDiv(d.id);}, 100);
      clusterControl.repositionIntanceDiv(d.id);
    });

  }

  // Toggle children on click.
  function click(d) {
    if (d.children) {
      d._children = d.children;
      d.children = null;
    } else {
      d.children = d._children;
      d._children = null;
    }
    update(d);
  }
}
