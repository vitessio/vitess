function jQueryHelper() {
    Function.addTo(jQueryHelper, [parseSelector, createElementFromSelectorNode, getOrAppendChildBySelector, createElementFromSelector]);

    function parseSelector(s) {
        var sizzle = jQuery.find;
        var groups = sizzle.tokenize(s);
        return groups;
    }

    function createElementFromSelector(selector) {
        var nodes = parseSelector(selector);
        return createElementFromSelectorNode(nodes[0]);
    }
    function createElementFromSelectorNode(node) {
        var tagName = "div";
        var tagToken = node.first(function (t) { return t.type == "TAG"; });
        if (tagToken != null)
            tagName = tagToken.value;

        var idToken = node.first(function (t) { return t.type == "ID"; });
        var el = $("<" + tagName + "/>");
        if (idToken != null)
            el.attr("id", idToken.value.substr(1));

        var atts = node.whereEq("type", "ATTR").select(function (t) { return t.value.substr(1, t.value.length - 2).split('='); });
        if (atts.length > 0) {
            atts.forEach(function (att) {
                el.attr(att[0], att[1]);
            });
        }

        var classes = node.whereEq("type", "CLASS").select(function (t) { return t.value.substr(1); });
        if (classes.length > 0)
            el.addClass(classes.join(" "));

        return el;
    }

    function getOrAppendChildBySelector(parentEl, selector, options) {
        var childEls = parentEl.children(selector).toArray();
        var total = null;
        var list = null;
        var action = null;
        var storeDataItem = false;
        var removeRemaining = false;
        if (options != null) {
            if (options.total != null)
                total = options.total;
            if (options.list != null) {
                list = options.list;
                if (total == null)
                    total = list.length;
            }
            action = options.action;
            storeDataItem = options.storeDataItem;
            removeRemaining = options.removeRemaining;
        }
        if (total == null)
            total = 1;

        var index = childEls.length;

        if (action != null || storeDataItem) {
            var min = Math.min(index, total);
            if (list == null)
                list = [];
            for (var i = 0; i < min; i++) {
                var child = $(childEls[i]);
                var dataItem = list[i];
                if (storeDataItem)
                    child.data("DataItem", dataItem);
                if (action != null)
                    action(child, dataItem, index);
            }
        }
        if (index < total) {
            var selectorNodes = parseSelector(selector);
            if (selectorNodes.length != 1)
                throw new Error();
            var selectorNode = selectorNodes[0];
            while (index < total) {
                var dataItem = list != null ? list[index] : null;
                var child = createElementFromSelectorNode(selectorNode);
                var childEl = child[0];
                parentEl.append(childEl);
                childEls.push(childEl);
                if (storeDataItem)
                    child.data("DataItem", dataItem);
                if (action != null)
                    action(child, dataItem, index);
                index++;
            }
        }
        if (removeRemaining) {
            while (childEls.length > total) {
                var parentEl = childEls.pop();
                $(parentEl).remove();
            }
        }
        return $(childEls);
    }
}
jQueryHelper();

jQuery.fn.getAppend = function (selector, options) {
    return jQueryHelper.getOrAppendChildBySelector(this, selector, options);
}
jQuery.fn.getAppendRemove = function (selector, total) {
    if (typeof (total) == "boolean")
        total = total ? 1 : 0;
    return jQueryHelper.getOrAppendChildBySelector(this, selector, { total: total, removeRemaining: true });
}
jQuery.fn.getAppendRemoveForEach = function (selector, list, action, options) {
    if (options == null)
        options = {};
    options.list = list;
    options.action = action;
    options.removeRemaining = true;
    return jQueryHelper.getOrAppendChildBySelector(this, selector, options);
}
jQuery.create = function (selector) {
    return jQueryHelper.createElementFromSelector(selector);
}

//binds a container's children selector to a list, matching the number of elements to the list.length (creating/deleting elements where needed), optionally performing action(el, dataItem, index) on each element
//returns a new jQuery object containing all children relevant to the selector
jQuery.fn.bindChildrenToList = function (selector, list, action, options) {
    if (options == null)
        options = {};
    options.list = list;
    options.action = action;
    options.storeDataItem = true;
    options.removeRemaining = true;
    return jQueryHelper.getOrAppendChildBySelector(this, selector, options);
}

//Turns a jquery object to an array of single jquery objects
jQuery.fn.toArray$ = function (action) {
    var list = [];
    for (var i = 0; i < this.length; i++)
        list.push($(this[i]));
    return list;
}

//Turns an array of jquery objects to a single jquery object
jQuery.fromArray$ = function (list) {
    return $(list.selectMany(function (j) { return j.toArray(); }));
}
