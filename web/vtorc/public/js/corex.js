
//******** Object
(function () {
    Object.toArray = function (obj) {
        var list = [];
        for (var p in obj) {
            list.push(p, obj[p]);
        }
        return list;
    }
    Object.allKeys = function (obj) {
        var list = [];
        for (var p in obj) {
            list.push(p);
        }
        return list;
    }
    Object.keysValues = function (obj) {
        var list = [];
        for (var p in obj) {
            list.push({ key: p, value: obj[p] });
        }
        return list;
    }
    Object.pairs = function (obj) {
        var list = [];
        list.isArrayOfPairs = true;
        for (var p in obj) {
            list.push([p, obj[p]]);
        }
        return list;
    }
    Object.fromPairs = function (keysValues) {
        var obj = {};
        keysValues.forEach(function (pair) {
            obj[pair[0]] = pair[1];
        });
        return obj;
    }
    Object.fromKeysValues = function (keysValues) {
        var obj = {};
        keysValues.forEach(function (keyValue) {
            obj[keyValue.key] = keyValue.value;
        });
        return obj;
    }
    Object.reversePairs = function (obj) {
        var list = [];
        for (var i = 0; i < obj.length; i++) {
            list.push([obj[i][1], obj[i][0]]);
        }
        return list;
    }
    Object.forEach = function (obj, keyValueAction) {
        Object.keys(obj).forEach(function (p) {
            keyValueAction(p, obj[p]);
        });
    }
    Object.toSortedByKey = function (obj) {
        var sortedKeys = Object.keys(obj).sort();
        return sortedKeys.toObject(function (key) { return [key, obj[key]]; })
    }
    Object.getCreateArray = function (obj, p) {
        var value = obj[p];
        if (value == null) {
            value = [];
            obj[p] = value;
        }
        return value;
    }
    Object.jsonStringifyEquals = function (x, y) {
        return JSON.stringify(x) == JSON.stringify(y);
    }
    Object.tryGet = function (obj, indexers) {
        if (typeof (indexers) == "string")
            indexers = indexers.split(".");
        var value = obj;
        for (var i = 0; i < indexers.length; i++) {
            if (value == null)
                return null;
            value = value[indexers[i]];
        }
        return value;
    }
    Object.trySet = function (obj, indexers, value) {
        if (typeof (indexers) == "string")
            indexers = indexers.split(".");
        var obj2 = obj;
        if (indexers.length > 1) {
            obj2 = Object.tryGet(obj, indexers.take(indexers.length - 1));
        }
        if (obj2 == null)
            return;
        obj2[indexers[indexers.length - 1]] = value;
    }
    Object.select = function (obj, selector) {
        return Q.createSelectorFunction(selector)(obj);
    }
    ///Deletes all keys in obj that have the same value: Object.delete({a:"b", c:"d"}, {a:"b", c:"g"})   =>   {c:"d"};
    Object.deleteKeysWithValues = function (obj, keysValues) {
        Object.keys(keysValues).forEach(function (key) {
            var value = keysValues[key];
            if (obj[key] == value)
                delete obj[key];
        });
    }

    var __hashKeyIndex = 0;
    Object.getHashKey = function (obj) {
        if (obj == null)
            return null;
        var x = obj.valueOf();
        var type = typeof (x);
        if (type == "number")
            return x.toString();
        if (type == "string")
            return x;
        if (x.__hashKey == null) {
            x.__hashKey = "\0" + "_" + x.constructor.name + "_" + __hashKeyIndex++;
        }
        return x.__hashKey
    }
    Object.values = function (obj) {
        var list = [];
        for (var p in obj) {
            list.push(obj[p]);
        }
        return list;
    }

    Object.removeAll = function (obj, predicate) {
        var toRemove = [];
        Object.forEach(obj, function (key, value) {
            if (predicate(key, value))
                toRemove.push(key);
        });
        toRemove.forEach(function (t) { delete obj[t]; });
    }

    Object.clear = function (obj) {
        Object.keys(obj).forEach(function (p) { delete obj[p]; });
    }

})();


//******** Function
(function () {

    //Acts like Function.prototype.bind, but preserves the 'this' context that is being sent to the newly generated function.
    //function Hello(a,b,c){return [a,b,c].join();}
    //Hello.bindArgs("a")("b","c")  -> a,b,c
    Function.prototype.bindArgs = function () {
        var args = Array.from(arguments);
        var func = this;
        return function () {
            var args2 = args.concat(Array.from(arguments));
            return func.apply(this, args2);
        };
    }
    //String.hello(a,b,c) -> String.prototype.hello(b,c) (a=this)
    Function.prototype.toPrototypeFunction = function () {
        var func = this;
        return function () {
            var args2 = Array.from(arguments);
            args2.insert(0, this);
            return func.apply(null, args2);
        };
    }
    //String.prototype.hello(b,c) -> String.hello(a,b,c) (a=this)
    Function.prototype.toStaticFunction = function () {
        return Function.prototype.call.bind(this);
    }
    //converts a constructor to a function that doesn't require the new operator
    Function.prototype.toNew = function () {
        var func = this;
        return function () {
            var x = func.applyNew(Array.from(arguments));
            return x;
        };
    }
    // Similar to func.apply(thisContext, args), but creates a new object instead of just calling the function - new func(args[0], args[1], args[2]...)
    Function.prototype.applyNew = function (args) {
        var count = args == null ? 0 : args.length;
        var ctor = this;
        switch (count) {
            case 0: return new ctor();
            case 1: return new ctor(args[0]);
            case 2: return new ctor(args[0], args[1]);
            case 3: return new ctor(args[0], args[1], args[2]);
            case 4: return new ctor(args[0], args[1], args[2], args[3]);
            case 5: return new ctor(args[0], args[1], args[2], args[3], args[4]);
            case 6: return new ctor(args[0], args[1], args[2], args[3], args[4], args[5]);
            case 7: return new ctor(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
        }
        throw new Error("Function.prototype.applyNew doesn't support more than 8 parameters");
    }
    // Similar to func.call(thisContext, args), but creates a new object instead of just calling the function - new func(arguments[0], arguments[1], arguments[2]...)
    Function.prototype.callNew = function (varargs) {
        var args = Array.from(arguments);
        return this.applyNew(args);
    }
    Function.prototype.getName = function () {
        var func = this;
        if (func.name != null)
            return func.name;
        var name = func.toString().substringBetween("function ", "(").trim();
        func.name = name;
        return name;
    }
    Function.prototype.addTo = function (target) {
        return Function.addTo(target, [this]);
    }
    // Creates a combination of two functions, where the new function will invoke the two functions, if the left side is already a combination function, it will be cloned and add the new function to it
    Function.combine = function (f1, f2) {
        if (f1 == null)
            return f2;
        if (f2 == null)
            return f1;
        var funcs;
        if (f1._isCombined) {
            var funcs = f1._funcs.toArray();
            funcs.add(f2);
        }
        else {
            funcs = [f1, f2];
        }
        return Function._combined(funcs);
    }
    Function._combined = function (funcs) {
        var func = function () {
            for (var i = 0; i < func._funcs.length; i++)
                func._funcs[i].apply(this, arguments);
        }
        func._isCombined = true;
        func._funcs = funcs;
        return func;
    }


    Function._lambda_cache = {};
    Function.lambda = function (exp) {
        var cache = Function._lambda_cache;
        var func = cache[exp];
        if (func == null) {
            func = Function._lambda(exp);
            cache[exp] = func;
        }
        return func;
    }
    Function._lambda = function (exp) {
        var arrow = exp.indexOf("=>");
        var prms;
        var body;
        if (arrow > 0) {
            var tPrms = exp.substring(0, arrow).replace("(", "").replace(")", "");
            prms = tPrms.split(",").map(function (t) { return t.trim(); });
            body = exp.substring(arrow + 2);
        }
        else {
            prms = [];
            body = exp;
        }
        if (!body.contains("return"))
            body = "return " + body + ";";
        prms.push(body);
        return Function.applyNew(prms);
    }
    Function.addTo = function (target, funcs) {
        funcs = Array.wrapIfNeeded(funcs);
        funcs.forEach(function (func) {
            target[func.getName()] = func;
        });
    }

})();


//******** Array
(function () {


    //Array Extensions
    Array.prototype.forEachJoin = function (action, actionBetweenItems) {
        var first = true;
        for (var i = 0; i < this.length; i++) {
            if (first)
                first = false;
            else
                actionBetweenItems();
            action(this[i]);
        }
    }
    Array.prototype.first = function (predicate) {
        if (predicate == null)
            return this[0];
        for (var i = 0; i < this.length; i++) {
            if (predicate(this[i]))
                return this[i];
        }
        return null;
    }
    Array.prototype.toArray = function () {
        return this.slice(0);
    }
    Array.prototype.insert = function (index, item) {
        this.splice(index, 0, item);
    }
    Array.prototype.insertRange = function (index, items) {
        var args = items.toArray();
        args.insert(0, 0);
        args.insert(0, index);
        this.splice.apply(this, args);
    }
    Array.prototype.last = function (predicate) {
        var len = this.length;
        if (len == 0)
            return null;
        if (predicate == null)
            return this[len - 1];
        for (var i = len - 1; i >= 0; i--) {
            if (predicate(this[i]))
                return this[i];
        }
        return null;
    }
    Array.prototype.toObject = function (selector) {
        if (selector == null) {
            return this.copyPairsToObject();
        }
        var obj = {};
        for (var i = 0; i < this.length; i++) {
            var obj2 = selector(this[i]);
            if (obj2 instanceof Array)
                obj2.copyPairsToObject(obj);
            else {
                for (var p in obj2)
                    obj[p] = obj2[p];
            }
        }
        return obj;
    };
    Array.prototype.toObjectKeys = function (defaultValue) {
        var obj = {};
        for (var i = 0; i < this.length; i++) {
            var p = this[i];
            obj[p] = defaultValue;
        }
        return obj;
    };
    Array.prototype.keysToObject = Array.prototype.toObjectKeys;
    Array.prototype.pairsToObject = Array.prototype.toObject;
    Array.prototype.copyPairsToObject = function (obj) {
        if (obj == null)
            obj = {};
        for (var i = 0; i < this.length; i += 2) {
            obj[this[i]] = this[i + 1];
        }
        return obj;
    };
    Array.prototype.removeFirst = function () {
        return this.splice(0, 1)[0];
    }
    Array.prototype.remove = function (item) {
        for (var i = 0; i < this.length; i++) {
            if (this[i] === item) {
                this.removeAt(i);
                return true;
            }
        }
        return false;
    }
    Array.prototype.removeRange = function (items) {
        items.forEach(function (t) { this.remove(t); });
    }
    Array.prototype.contains = function (s) {
        return this.indexOf(s) >= 0;
    }
    Array.prototype.containsAny = function (items) {
        return items.any(function (t) { return this.contains(t); }.bind(this));
    }
    Array.prototype.any = function (predicate) {
        return this.some(Q.createSelectorFunction(predicate));
    }
    Array.prototype.distinct = function (keyGen) {
        if (keyGen == null)
            keyGen = Object.getHashKey;
        var list = [];
        var set = {};
        this.forEach(function (t) {
            var key = keyGen(t);
            if (set[key])
                return;
            set[key] = true;
            list.push(t);
        });
        return list;
    }
    Array.prototype.removeAll = function (predicate) {
        var toRemove = [];
        for (var i = 0; i < this.length; i++) {
            if (predicate(this[i])) {
                toRemove.push(i);
            }
        }
        while (toRemove.length > 0) {
            var index = toRemove.pop();
            this.removeAt(index);
        }
    }
    Array.prototype.removeAt = function (index) {
        this.splice(index, 1);
    }
    ///<summary>Iterates over the array, performing an async function for each item, going to the next one only when the previous one has finished (called his callback)</summary>
    Array.prototype.forEachAsyncProgressive = function (actionWithCallback, finalCallback) {
        this._forEachAsyncProgressive(actionWithCallback, finalCallback, 0);
    }
    Array.prototype.where = function (predicate) {
        return this.filter(Q.createSelectorFunction(predicate));
    }
    Array.prototype.whereEq = function (selector, value) {
        selector = Q.createSelectorFunction(selector);
        return this.filter(function (t, i) { return selector(t, i) == value; });
    }
    Array.prototype.whereNotEq = function (selector, value) {
        selector = Q.createSelectorFunction(selector);
        return this.filter(function (t, i) { return selector(t, i) != value; });
    }
    Array.prototype.firstEq = function (selector, value) {
        selector = Q.createSelectorFunction(selector);
        return this.first(function (t, i) { return selector(t, i) == value; });
    }
    Array.prototype.firstNotEq = function (selector, value) {
        selector = Q.createSelectorFunction(selector);
        return this.first(function (t, i) { return selector(t, i) != value; });
    }
    Array.prototype.addRange = function (items) {
        this.push.apply(this, items);
    }
    Array.prototype.diff = function (target) {
        var source = this;
        var res = {
            added: source.where(function (t) { return !target.contains(t); }),
            removed: target.where(function (t) { return !source.contains(t); }),
        };
        return res;
    }
    Array.prototype.hasDiff = function (target) {
        var diff = this.diff(target);
        return diff.added.length > 0 || diff.removed.length > 0;
    }
    Array.prototype._forEachAsyncProgressive = function (actionWithCallback, finalCallback, index) {
        if (index == null)
            index = 0;
        if (index >= this.length) {
            if (finalCallback != null)
                finalCallback();
            return;
        }
        var item = this[index];
        actionWithCallback(item, function () { this._forEachAsyncProgressive(actionWithCallback, finalCallback, index + 1); }.bind(this));
    }
    /// Iterates over the array, performing an async function for each item, going to the next one only when the previous one has finished (called his callback)
    Array.prototype.mapAsyncProgressive = function (actionWithCallback, finalCallback) {
        this._mapAsyncProgressive(actionWithCallback, finalCallback, 0, []);
    }
    Array.prototype._mapAsyncProgressive = function (actionWithCallbackWithResult, finalCallback, index, results) {
        if (index == null)
            index = 0;
        if (index >= this.length) {
            if (finalCallback != null)
                finalCallback(results);
            return;
        }
        var item = this[index];
        actionWithCallbackWithResult(item, function (res) {
            results.push(res);
            this._mapAsyncProgressive(actionWithCallbackWithResult, finalCallback, index + 1, results);
        }.bind(this));
    }
    Array.prototype.mapWith = function (anotherList, funcForTwoItems) {
        if (funcForTwoItems == null)
            funcForTwoItems = function (x, y) { return [x, y]; };
        var list = [];
        var maxLength = Math.max(this.length, anotherList.length);
        for (var i = 0; i < maxLength; i++)
            list.push(funcForTwoItems(this[i], anotherList[i]));
        return list;
    }
    Array.prototype.min = function () {
        var min = null;
        for (var i = 0; i < this.length; i++) {
            var value = this[i];
            if (min == null || value < min)
                min = value;
        }
        return min;
    }
    Array.prototype.max = function () {
        var max = null;
        for (var i = 0; i < this.length; i++) {
            var value = this[i];
            if (max == null || value > max)
                max = value;
        }
        return max;
    }
    Array.prototype.getEnumerator = function () {
        return new ArrayEnumerator(this);
    }
    Array.prototype.orderBy = function (selector, desc) {
        return this.toArray().sortBy(selector, desc);
    }
    Array.prototype.orderByDescending = function (selector, desc) {
        return this.orderBy(selector, true);
    }
    Array.prototype.sortBy = function (selector, desc) {
        var compareFunc;
        if (selector instanceof Array) {
            var pairs = selector;
            var funcs = pairs.map(function (pair) {
                if (pair instanceof Array)
                    return createSortFuncFromCompareFunc(pair[0], pair[1]);
                return createCompareFuncFromSelector(pair);
            });
            compareFunc = combineCompareFuncs(funcs);
        }
        else {
            compareFunc = createCompareFuncFromSelector(selector, desc);
        }
        this.sort(compareFunc);
        return this;
    }
    Array.prototype.sortByDescending = function (selector) {
        return this.sortBy(selector, true);
    }
    //Performs an async function on each item in the array, invoking a finalCallback when all are completed
    //asyncFunc -> function(item, callback -> function(result))
    //finalCallback -> function(results);
    Array.prototype.mapAsyncParallel = function (asyncFunc, finalCallback) {
        var results = [];
        var list = this;
        var length = list.length;
        if (length == 0) {
            finalCallback(results);
            return;
        }
        var finished = 0;
        var cb = function (res, index) {
            results[index] = res;
            finished++;
            if (finished == length)
                finalCallback(results);
        };
        list.forEach(function (item, i) {
            asyncFunc(item, function (res) { cb(res, i); });
        });
    }
    //Performs an async function on each item in the array, invoking a finalCallback when all are completed
    //asyncFunc -> function(item, callback -> function(result))
    //finalCallback -> function(results);
    Array.prototype.forEachAsyncParallel = function (asyncFunc, finalCallback) {
        var list = this;
        var length = list.length;
        if (length == 0) {
            finalCallback();
            return;
        }
        var finished = 0;
        var cb = function (res, index) {
            finished++;
            if (finished == length)
                finalCallback();
        };
        list.forEach(function (item, i) {
            asyncFunc(item, cb);//function () { cb(i); });
        });
    }
    Array.prototype.clear = function () {
        this.splice(0, this.length);
    }
    Array.prototype.itemsEqual = function (list) {
        if (list == this)
            return true;
        if (list.length != this.length)
            return false;
        for (var i = 0; i < this.length; i++)
            if (this[i] != list[i])
                return false;
        return true;
    }
    Array.prototype.select = function (selector) {
        var func = Q.createSelectorFunction(selector);
        return this.map(func);
    }
    Array.prototype.selectInvoke = function (name) {
        return this.map(function (t) { return t[name](); });
    }
    Array.prototype.joinWith = function (list2, keySelector1, keySelector2, resultSelector) {
        keySelector1 = Q.createSelectorFunction(keySelector1);
        keySelector2 = Q.createSelectorFunction(keySelector2);
        resultSelector = Q.createSelectorFunction(resultSelector);

        var list1 = this;

        var groups1 = list1.groupByToObject(keySelector1);
        var groups2 = list2.groupByToObject(keySelector2);

        var list = [];
        var group = {};
        for (var p in groups1) {
            if (groups2[p] != null)
                list.push(resultSelector(groups1[p], groups2[p]));
        }

        return list;
    }
    Array.prototype.all = function (predicate) {
        return this.every(Q.createSelectorFunction(predicate));
    }
    Array.prototype.flatten = function () {
        var list = [];
        this.forEach(function (t) {
            list.addRange(t);
        });
        return list;
    }
    Array.prototype.selectToObject = function (keySelector, valueSelector) {
        var obj = {};
        if (valueSelector == null) {
            var list = this.select(keySelector);
            for (var i = 0; i < list.length; i++) {
                var obj2 = this[i];
                if (obj2 != null) {
                    if (obj2 instanceof Array) {
                        for (var i = 0; i < obj2.length; i++) {
                            obj[obj2[0]] = obj2[1];
                        }
                    }
                    else {
                        Q.copy(obj2, obj, { overwrite: true });
                    }
                }
            }
        }
        else {
            keySelector = Q.createSelectorFunction(keySelector);
            valueSelector = Q.createSelectorFunction(valueSelector);

            for (var i = 0; i < this.length; i++) {
                var item = this[i];
                obj[keySelector(item)] = valueSelector(item);
            }
        }
        return obj;
    }
    Array.prototype.groupByToObject = function (keySelector, itemSelector) {
        keySelector = Q.createSelectorFunction(keySelector);
        itemSelector = Q.createSelectorFunction(itemSelector);
        var obj = {};
        for (var i = 0; i < this.length; i++) {
            var item = this[i];
            var key = keySelector(item);
            if (obj[key] == null) {
                obj[key] = [];
                obj[key].key = key;
            }
            var value = itemSelector(item);
            obj[key].push(value);
        }
        return obj;
    }
    Array.prototype.groupBy = function (keySelector, itemSelector) {
        var groupsMap = this.groupByToObject(keySelector, itemSelector);
        return Object.values(groupsMap);
    }
    Array.prototype.splitIntoChunksOf = function (countInEachChunk) {
        var chunks = Math.ceil(this.length / countInEachChunk);
        var list = [];
        for (var i = 0; i < this.length; i += countInEachChunk) {
            list.push(this.slice(i, i + countInEachChunk));
        }
        return list;
    }
    Array.prototype.avg = function () {
        if (this.length == 0)
            return null;
        return this.sum() / this.length;
    }
    Array.prototype.selectMany = function (selector) {
        var list = [];
        this.select(selector).forEach(function (t) { t.forEach(function (x) { list.push(x); }); });
        return list;
    }
    Array.prototype.sum = function () {
        if (this.length == 0)
            return 0;
        var sum = this[0];
        for (var i = 1; i < this.length; i++)
            sum += this[i];
        return sum;
    }
    Array.prototype.skip = function (count) {
        return this.slice(count);
    }
    Array.prototype.take = function (count) {
        return this.slice(0, count);
    }
    Array.prototype.toSelector = function () {
        return Q.createSelectorFunction(this);
    }
    Array.prototype.removeNulls = function () {
        return this.removeAll(function (t) { return t == null; });
    }
    Array.prototype.exceptNulls = function () {
        return this.where(function (t) { return t != null; });
    }
    Array.prototype.truncate = function (totalItems) {
        if (this.length <= totalItems)
            return;
        this.splice(totalItems, this.length - totalItems);
    }
    Array.prototype.random = function () {
        return this[Math.randomInt(0, this.length - 1)];
    }
    Array.prototype.selectRecursive = function (selector, recursiveFunc) {
        if (recursiveFunc == null) {
            recursiveFunc = selector;
            selector = null;
        }
        var list = this.select(selector);
        var children = this.select(recursiveFunc);
        var list2 = children.selectRecursive(selector, recursiveFunc);
        list.addRange(list2);
        return list;
    }
    Array.prototype.selectManyRecursive = function (selector, recursiveFunc) {
        if (recursiveFunc == null) {
            recursiveFunc = selector;
            selector = null;
        }
        var list;
        if (selector == null)
            list = this.toArray();
        else
            list = this.selectMany(selector);
        var children = this.selectMany(recursiveFunc);
        if (children.length > 0) {
            var list2 = children.selectManyRecursive(selector, recursiveFunc);
            list.addRange(list2);
        }
        return list;
    }

    //Array Extension Aliases
    Array.prototype.peek = Array.prototype.last;
    Array.prototype.removeLast = Array.prototype.pop;
    Array.prototype.add = Array.prototype.push;
    Array.prototype.forEachWith = function (list, action) {
        return Array.forEachTwice(this, list, action);
    }
    Array.prototype.selectWith = function (list, func) {
        return Array.selectTwice(this, list, action);
    }

    //Produces a cartesian product of two lists, if no selector(x1, y1) is defined, will return an array of pairs [[x1,y1],[x1,y2],[x1,y3]...]
    Array.prototype.crossJoin = function (list2, selector) {
        var list1 = this;
        var list3 = [];
        if (selector == null)
            selector = function (x, y) { return [x, y]; };
        list1.forEach(function (t1) {
            list2.forEach(function (t2) {
                list3.push(selector(t1, t2));
            });
        });
        return list3;
    }

    //Array Static Extensions
    Array.joinAll = function (lists, keySelector, resultSelector) {
        keySelector = Q.createSelectorFunction(keySelector);
        resultSelector = Q.createSelectorFunction(resultSelector);

        var groupMaps = lists.map(function (list) {
            return list.groupByToObject(keySelector);
        });

        var groupMap1 = groupMaps[0];

        var list = [];
        for (var p in groupMap1) {
            if (groupMaps.all(p))
                list.push(resultSelector(groupMaps.select(p)));
        }

        return list;
    }
    Array.outerJoin = function (list1, list2, keySelector1, keySelector2, resultSelector) {
        keySelector1 = Q.createSelectorFunction(keySelector1);
        keySelector2 = Q.createSelectorFunction(keySelector2);
        resultSelector = Q.createSelectorFunction(resultSelector);


        var groups1 = list1.groupByToObject(keySelector1);
        var groups2 = list2.groupByToObject(keySelector2);


        var allKeys = Object.keys(groups1);
        allKeys.addRange(Object.keys(groups2));
        allKeys = allKeys.distinct();
        //allKeys.sort();

        var list3 = [];
        allKeys.forEach(function (key) {
            var group1 = groups1[key] || [];
            var group2 = groups2[key] || [];
            var res = Array.selectTwice(group1, group2, function (item1, item2) { return resultSelector(key, item1, item2); });
            list3.addRange(res);
        });
        return list3;
    }
    Array.outerJoinAll = function (lists, keySelector, resultSelector) {
        keySelector = Q.createSelectorFunction(keySelector);
        resultSelector = Q.createSelectorFunction(resultSelector);


        var listsGroups = lists.select(function (list) { return list.groupByToObject(keySelector); });
        //[{key1:items, key2:items}, {key1:items, key2:items}]

        var allKeys = listsGroups.selectMany(function (t) { return Object.values(t).select("key"); }).distinct();

        var list3 = [];
        allKeys.forEach(function (key) {
            var lists = listsGroups.select(function (obj) { return obj[key] || []; });
            var list2 = Array.selectAll(lists, function (items, index) { return resultSelector(key, items); });
            list3.addRange(list2);
        });
        return list3;
    }
    Array.forEachAll = function (lists, action) {
        var max = lists.select("length").max();
        for (var i = 0; i < max; i++) {
            var values = lists.select(i);
            action(values, i);
        }
    }
    Array.selectAll = function (lists, func) {
        var list2 = [];
        Array.forEachAll(lists, function (items, i) {
            list2.push(func(items, i));
        });
        return list2;
    }
    Array.forEachTwice = function (list1, list2, action) {
        var l1 = list1.length;
        var l2 = list2.length;
        var max = Math.max(l1, l2);
        for (var i = 0; i < max; i++) {
            action(list1[i], list2[i], i);
        }
    }
    Array.selectTwice = function (list1, list2, func) {
        var list = [];
        Array.forEachTwice(list1, list2, function (t1, t2, index) {
            var item = func(t1, t2, index);
            list.push(item);
        });
        return list;
    }
    Array.generate = function (length, generator) {
        var list = new Array(length);
        for (var i = 0; i < length; i++) {
            list[i] = generator(i);
        }
        return list;
    }
    Array.wrapIfNeeded = function (obj) {
        if (obj instanceof Array)
            return obj;
        return [obj];
    }
    Array.toArray = function (arrayLike) {
        return Array.prototype.slice.call(arrayLike, 0);
    }
    Array.from = function (arrayLike) {
        return Array.prototype.slice.call(arrayLike, 0);
    }
    Array.generateNumbers = function (from, until) {
        if (arguments.length == 1) {
            until = from;
            from = 0;
        }
        var length = until - from;
        var list = new Array(length);
        for (var i = 0; i < length; i++) {
            list[i] = i + from;
        }
        return list;
    }
    Array.slice = Array.prototype.slice.toStaticFunction();
    Array.concat = Array.prototype.concat.toStaticFunction();

})();


//******** Date
(function () {

    /* Date extensions, taken from jsclr framework */
    Date.prototype.compareTo = function (value) {
        return this.valueOf() - value.valueOf();
    };
    Date.prototype.year = function (value) {
        if (value == null) {
            if (this._Kind == 1)
                return this.getUTCFullYear();
            return this.getFullYear();
        }
        var d = this.clone();
        if (d._Kind == 1)
            d.setUTCFullYear(value);
        else
            d.setFullYear(value);
        return d;
    };
    Date.prototype.totalDays = function () {
        return this.valueOf() / (24 * 60 * 60 * 1000);
    };
    Date.prototype.totalHours = function () {
        return this.valueOf() / (60 * 60 * 1000);
    };
    Date.prototype.totalMinutes = function () {
        return this.valueOf() / (60 * 1000);
    };
    Date.prototype.totalSeconds = function () {
        return this.valueOf() / 1000;
    };
    Date.prototype.month = function (value) {
        if (value == null) {
            if (this._Kind == 1)
                return this.getUTCMonth() + 1;
            return this.getMonth() + 1;
        }
        var d = this.clone();
        if (d._Kind == 1)
            d.setUTCMonth(value - 1);
        else
            d.setMonth(value - 1);
        return d;
    };
    Date.prototype.day = function (value) {
        if (value == null) {
            if (this._Kind == 1)
                return this.getUTCDate();
            return this.getDate();
        }
        var d = this.clone();
        if (d._Kind == 1)
            d.setUTCDate(value);
        else
            d.setDate(value);
        return d;
    };
    Date.prototype.hour = function (value) {
        if (value == null) {
            if (this._Kind == 1)
                return this.getUTCHours();
            return this.getHours();
        }
        var d = this.clone();
        if (d._Kind == 1)
            d.setUTCHours(value);
        else
            d.setHours(value);
        return d;
    };
    Date.prototype.minute = function (value) {
        if (value == null) {
            if (this._Kind == 1)
                return this.getUTCMinutes();
            return this.getMinutes();
        }
        var d = this.clone();
        if (d._Kind == 1)
            d.setUTCMinutes(value);
        else
            d.setMinutes(value);
        return d;
    };
    Date.prototype.second = function (value) {
        if (value == null) {
            if (this._Kind == 1)
                return this.getUTCSeconds();
            return this.getSeconds();
        }
        var d = this.clone();
        if (d._Kind == 1)
            d.setUTCSeconds(value);
        else
            d.setSeconds(value);
        return d;
    };
    Date.prototype.ms = function (value) {
        if (value == null) {
            if (this._Kind == 1)
                return this.getUTCMilliseconds();
            return this.getMilliseconds();
        }
        var d = this.clone();
        if (d._Kind == 1)
            d.setUTCMilliseconds(value);
        else
            d.setMilliseconds(value);
        return d;
    };
    Date.prototype.toUnix = function () {
        if (this._Kind == 1)
            throw new Error();
        return Math.round(this.getTime() / 1000);
    };
    Date.prototype.dayOfWeek = function () {
        return this.getDay() + 1;
    };
    Date.prototype.toLocalTime = function () {
        if (this._Kind != 1)
            return this;
        var x = this.clone();
        x._Kind = 2;
        return x;
    };
    Date.prototype.toUniversalTime = function () {
        if (this._Kind == 1)
            return this;
        var x = this.clone();
        x._Kind = 1;
        return x;
    };
    Date.prototype.subtract = function (date) {
        var diff = this.valueOf() - date.valueOf();
        return new Date(diff);
    };
    Date.prototype.Subtract$$DateTime = function (value) {
        var diff = this.valueOf() - value.valueOf();
        return new System.TimeSpan.ctor$$Int64(diff * 10000);
    };
    Date.prototype.Subtract$$TimeSpan = function (value) {
        var newDate = this.clone();
        newDate.setMilliseconds(this.getMilliseconds() + value.getTotalMilliseconds());
        return newDate;
    };
    Date.prototype.format = function (format) {
        if (typeof (format) == "object") {
            var options = format;
            if (options.noTime != null && !this.hasTime())
                return this.format(options.noTime);
            else if (options.noDate != null && !this.hasDate())
                return this.format(options.noDate);
            else if (options.fallback != null)
                return this.format(options.fallback);
            return this.toString();
        }
        var s = format;

        var s2 = s.replaceMany("yyyy yy MMMM MMM MM M dddd ddd dd d HH H mm m ss s ff f".split(" "), function (t) {
            switch (t) {
                case "yyyy": return this.year().format("0000");
                case "yy": return this.year().format("00").truncateStart(2);
                case "y": return this.year().toString();
                case "MMMM": return Date._monthNames[this.getMonth()];
                case "MMM": return Date._monthNamesAbbr[this.getMonth()];
                case "MM": return this.month().format("00");
                case "M": return this.month().toString();
                case "dd": return this.day().format("00");
                case "d": return this.day().toString();
                case "dddd": return Date._dowNames[this.getDay()];
                case "ddd": return Date._dowNamesAbbr[this.getDay()];
                case "HH": return this.hour().format("00");
                case "H": return this.hour().toString();
                case "mm": return this.minute().format("00");
                case "m": return this.minute().toString();
                case "ss": return this.second().format("00");
                case "s": return this.second().toString();
                case "ff": return this.ms().format("00");
                case "f": return this.ms().toString();
            }

        }.bind(this));
        return s2.toString();
    };
    Date.prototype.clone = function () {
        var x = new Date(this.valueOf());
        x._Kind = this._Kind;
        return x;
    };
    Date.prototype.addMs = function (miliseconds) {
        var date2 = this.clone();
        date2.setMilliseconds(date2.getMilliseconds() + miliseconds);
        return date2;
    };
    Date.prototype.addSeconds = function (seconds) {
        var date2 = this.clone();
        date2.setSeconds(date2.getSeconds() + seconds);
        return date2;
    };
    Date.prototype.addMinutes = function (minutes) {
        var date2 = this.clone();
        date2.setMinutes(date2.getMinutes() + minutes);
        return date2;
    };
    Date.prototype.addHours = function (hours) {
        var date2 = this.clone();
        date2.setHours(date2.getHours() + hours);
        return date2;
    };
    Date.prototype.addDays = function (days) {
        var date2 = this.clone();
        date2.setDate(date2.getDate() + days);
        return date2;
    };
    Date.prototype.addWeeks = function (weeks) {
        return this.addDays(weeks * 7);
    };
    Date.prototype.addMonths = function (months) {
        var date2 = this.clone();
        date2.setMonth(date2.getMonth() + months);
        return date2;
    };
    Date.prototype.addYears = function (years) {
        var date2 = this.clone();
        date2.setFullYear(date2.getFullYear() + years);
        return date2;
    };
    Date.prototype.removeTime = function () {
        var date2 = this.clone();
        date2.setHours(0, 0, 0, 0);
        return date2;
    };
    Date.prototype.hasTime = function () {
        return this.hour() != 0 && this.second() != 0 && this.ms() != 0;
    };
    Date.prototype.hasDate = function () {
        var date2 = new Date(0);
        return this.year() != date2.year() && this.month() != date2.month() && this.day() != date2.day();
    };
    Date.prototype.removeDate = function () {
        var time = this.clone();
        time.setHours(this.hour(), this.minute(), this.second(), this.ms());
        return time;
    };
    Date.prototype.extractTime = function () {
        return this.removeDate();
    };
    Date.prototype.extractDate = function () {
        return this.removeTime();
    };
    Date.prototype.equals = function (obj) {
        if (obj == null)
            return false;
        return obj.valueOf() == this.valueOf();
    };
    Date.prototype.GetHashCode = function () {
        return this.valueOf();
    };
    Date.prototype.getKind = function () {
        if (this._Kind == null)
            return 2;
        return this._Kind;
    };
    Date.prototype.round = function (part, precision) {
        return Date.roundUsing(Math.round, this, part, precision);
    }
    Date.prototype.floor = function (part, precision) {
        return Date.roundUsing(Math.floor, this, part, precision);
    }
    Date.prototype.ceil = function (part, precision) {
        return Date.roundUsing(Math.ceil, this, part, precision);
    }
    Date.prototype.add = function (value, part) {
        var map = {
            "day": "addDays",
            "month": "addMonths",
            "week": "addWeeks",
            "hour": "addHours",
            "minute": "addMinutes",
            "second": "addSeconds",
            "ms": "addMs",
        };
        if (part == null)
            part = "ms";
        var func = map[part];
        var date2 = this[func](value);
        return date2;
    };



    Date._dowNames = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
    Date._dowNamesAbbr = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    Date._monthNamesAbbr = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    Date._monthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
    Date.days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
    Date._parts = ["year", "month", "day", "hour", "minute", "second", "ms"];


    Date.fromUnix = function (value) {
        return new Date(value * 1000);
    };
    Date.today = function () {
        return new Date().removeTime();
    };
    Date.current = function () {
        return new Date();
    };
    Date.create = function (y, m, d, h, mm, s, ms) {
        if (ms != null)
            return new Date(y, m - 1, d, h, mm, s, ms);
        if (s != null)
            return new Date(y, m - 1, d, h, mm, s);
        if (mm != null)
            return new Date(y, m - 1, d, h, mm);
        if (h != null)
            return new Date(y, m - 1, d, h);
        if (d != null)
            return new Date(y, m - 1, d);
        if (m != null)
            return new Date(y, m - 1);
        if (y != null)
            return new Date(y);
        var x = new Date(1970, 0, 1);
        x.setHours(0, 0, 0, 0);
        return x;
    }
    Date._parsePart = function (ctx, part, setter) {
        if (ctx.failed)
            return;
        var index = ctx.format.indexOf(part);
        if (index < 0)
            return;
        var token = ctx.s.substr(index, part.length);
        if (token.length == 0) {
            ctx.failed = true;
            return;
        }
        var value = Q.parseInt(token);
        if (value == null) {
            ctx.failed = true;
            return;
        }
        ctx.date = setter.call(ctx.date, value);
        ctx.format = ctx.format.replaceAt(index, part.length, "".padRight(part.length));
        ctx.s = ctx.s.replaceAt(index, part.length, "".padRight(part.length));
    }
    Date.tryParseExact = function (s, formats) {
        if (typeof (formats) == "string")
            formats = [formats];
        for (var i = 0; i < formats.length; i++) {
            var x = Date._tryParseExact(s, formats[i]);
            if (x != null)
                return x;
        }
        return null;
    };
    Date._tryParseExact = function (s, format) {
        if (s.length != format.length)
            return null;
        var date = Date.create();
        var ctx = { date: date, s: s, format: format };
        Date._parsePart(ctx, "yyyy", date.year);
        Date._parsePart(ctx, "yy", date.year);
        Date._parsePart(ctx, "MM", date.month);
        Date._parsePart(ctx, "dd", date.day);
        Date._parsePart(ctx, "HH", date.hour);
        Date._parsePart(ctx, "mm", date.minute);
        Date._parsePart(ctx, "ss", date.second);
        if (ctx.failed)
            return null;
        if (ctx.s != ctx.format)
            return null;
        return ctx.date;
    };
    Date.tryParseJsonDate = function (s) {
        if (s.length == 26 && s[0] == "\"" && s[value.length - 1] == "\"") {
            s = s.substr(1, 24);
        }
        if (s.length == 24 && /[0-9]+-[0-9]+-[0-9]+T[0-9]+:[0-9]+:[0-9]+\.[0-9]+Z/.test(s)) {
            var d = new Date(s);
            if (!isNaN(d.valueOf()))
                return d;
        }
        return null;

    }
    Date.roundUsing = function (mathOp, date, part, precision) {
        var parts = Date._parts;
        if (part == null)
            part = "second";
        if (!precision)
            precision = 1;
        var partIndex = parts.indexOf(part);
        if (partIndex < 0) {
            part = "second";
            partIndex = 5;
        }
        var date2 = date.clone();
        var value = date2[part]();
        value = Number.roundUsing(mathOp, value, precision);
        date2 = date2[part](value);
        for (var i = partIndex + 1; i < parts.length; i++) {
            var part2 = parts[i];
            date2 = date2[part2](0);
        }
        return date2;
    }


})();


//******** String
(function () {
    String.prototype.forEach = Array.prototype.forEach;

    String.prototype.contains = function (s) {
        return this.indexOf(s) >= 0;
    }
    String.prototype.endsWith = function (suffix) {
        return this.indexOf(suffix, this.length - suffix.length) !== -1;
    };
    String.prototype.startsWith = function (s) {
        return this.indexOf(s) == 0;
    }
    /**
     * ReplaceAll by Fagner Brack (MIT Licensed)
     * Replaces all occurrences of a substring in a string
     */
    String.prototype.replaceAll = function (token, newToken, ignoreCase) {
        var _token;
        var str = this + "";
        var i = -1;

        if (typeof token === "string") {

            if (ignoreCase) {

                _token = token.toLowerCase();

                while ((
                    i = str.toLowerCase().indexOf(
                        token, i >= 0 ? i + newToken.length : 0
                    )) !== -1
                ) {
                    str = str.substring(0, i) +
                        newToken +
                        str.substring(i + token.length);
                }

            } else {
                return this.split(token).join(newToken);
            }

        }
        return str;
    };
    String.prototype.replaceMany = function (finds, replacer) {
        var s = this;
        var s2 = "";
        var i = 0;
        while (i < s.length) {
            var append = s[i];
            var inc = 1;
            for (var j = 0; j < finds.length; j++) {
                var find = finds[j];
                var token = s.substr(i, find.length);
                if (find == token) {
                    var replace = replacer(find, i);
                    append = replace;
                    inc = find.length;
                    break;
                }
            }
            s2 += append;
            i += inc;
        }
        return s2;
    }
    String.prototype.truncateEnd = function (finalLength) {
        if (this.length > finalLength)
            return this.substr(0, finalLength);
        return this;
    }
    String.prototype.truncateStart = function (finalLength) {
        if (this.length > finalLength)
            return this.substr(this.length - finalLength);
        return this;
    }
    String.prototype.remove = function (index, length) {
        var s = this.substr(0, index);
        s += this.substr(index + length);
        return s;
    }
    String.prototype.insert = function (index, text) {
        var s = this.substr(0, index);
        s += text;
        s += this.substr(index);
        return s;
    }
    String.prototype.replaceAt = function (index, length, text) {
        return this.remove(index, length).insert(index, text);
    }
    String.prototype.padRight = function (totalWidth, paddingChar) {
        if (paddingChar == null || paddingChar == "")
            paddingChar = " ";
        var s = this;
        while (s.length < totalWidth)
            s += paddingChar;
        return s;
    }
    String.prototype.padLeft = function (totalWidth, paddingChar) {
        if (paddingChar == null || paddingChar == "")
            paddingChar = " ";
        var s = this;
        while (s.length < totalWidth)
            s = paddingChar + s;
        return s;
    }
    String.prototype.toLambda = function () {
        return Function.lambda(this);
    }
    String.prototype.toSelector = function () {
        return Q.createSelectorFunction(this);
    }
    String.prototype.substringBetween = function (start, end) {
        var s = this;
        var i1 = s.indexOf(start);
        if (i1 < 0)
            return null;
        var i2 = s.indexOf(end, i1 + 1);
        if (i2 < 0)
            return null;
        return s.substring(i1 + start.length, i2);
    }
    String.prototype.all = Array.prototype.all;
    String.prototype.every = Array.prototype.every;
    String.prototype.isInt = function () {
        return String.isInt(this);
    }
    String.prototype.isFloat = function () {
        var floatRegex = /^[+-]?[0-9]*[\.]?[0-9]*$/;
        return String.isFloat(this);
    }
    String.isInt = function (s) {
        var intRegex = /^[+-]?[0-9]+$/;
        return intRegex.test(s);
    }
    String.isFloat = function (s) {
        var floatRegex = /^[+-]?[0-9]*[\.]?[0-9]*$/;
        return floatRegex.test(s);
    }
    String.prototype.last = function (predicate) {
        if (this.length == 0)
            return null;
        if (predicate == null)
            return this[this.length - 1];
        for (var i = this.length; i >= 0; i--) {
            if (predicate(this[i]))
                return this[i];
        }
        return null;
    }

    String.prototype.splitAt = function (index) {
        return [this.substr(0, index), this.substr(index)];
    }
    String.prototype.lines = function () {
        return this.match(/[^\r\n]+/g);
    }

})();


//******** Number
(function () {

    Number.prototype.format = function (format) {
        var s = this.toString();
        for (var i = 0; i < format.length; i++) {
            var ch = format.charAt(i);
            if (ch == "0") {
                if (s.length < i + 1)
                    s = "0" + s;
            }
            else
                throw new Error("not implemented");
        }
        return s;
    }
    Number.prototype.round = function (precision) {
        return Number.roundUsing(Math.round, this, precision);
    }
    Number.prototype.ceil = function (precision) {
        return Number.roundUsing(Math.ceil, this, precision);
    }
    Number.prototype.floor = function (precision) {
        return Number.roundUsing(Math.floor, this, precision);
    }

    Number.prototype.isInt = function () {
        return (this | 0) === this;
    }
    Number.prototype.isFloat = function () {
        return (this | 0) !== this;
    }
    Number.prototype.inRangeInclusive = function (min, max) {
        return this >= min && this <= max;
    }


    Number.generate = function (min, max, step) {
        var list = [];
        if (step == null)
            step = 1;
        for (var i = min; i <= max; i += step) {
            list.push(i);
        }
        return list;
    }
    Number.roundUsing = function (mathOp, x, precision) {
        if (precision == null)
            precision = 1;
        else if (precision < 0)
            precision *= -1;
        var mul = 1;
        while (((precision | 0) !== precision) && (mul < 10000000000)) {
            precision *= 10;
            x *= 10;
            mul *= 10;
        }
        return (mathOp(x / precision) * precision) / mul;
    }


})();


//******** JSON
(function () {
    JSON.iterateRecursively = function (obj, action) {
        if (obj == null || typeof (obj) != "object" || obj instanceof Date)
            return;

        if (obj instanceof Array) {
            var list = obj;
            list.forEach(function (item, index) {
                action(obj, index, item);
                JSON.iterateRecursively(item, action);
            });
        }
        else {
            Object.keys(obj).forEach(function (key) {
                var value = obj[key];
                action(obj, key, value);
                JSON.iterateRecursively(value, action);
            });
        }
    }
})();


//******** Math
(function () {
    Math.randomInt = function (min, max) {
        return Math.floor(Math.random() * (max - min + 1)) + min;
    }
})();


//******** Error
(function () {
    Error.prototype.wrap = function (e) {
        e.innerError = this;
        return e;
    }
    Error.prototype.causedBy = function (e) {
        this.innerError = e;
    }
})();


//******** Q
(function () {
    function Q() {
    };
    Q.copy = function (src, target, options, depth) {
        ///<summary>Copies an object into a target object,
        ///recursively cloning any native json object or array on the way, overwrite=true will overwrite a primitive field value even if exists
        ///Custom objects and functions are copied as/is by reference.
        ///</summary>
        ///<param name="src" />
        ///<param name="target" />
        ///<param name="options" type="Object">{ overwrite:false }</param>
        ///<returns type="Object">The copied object</returns>
        if (depth == null)
            depth = 0;
        if (depth == 100) {
            console.warn("Q.copy is in depth of 100 - possible circular reference")
        }
        if (src === null && target === undefined)
            return null;
        if (src == target || src == null)
            return target;
        options = options || { overwrite: false };

        if (typeof (src) != "object") {
            if (options.overwrite || target == null)
                return src;
            return target;
        }
        if (typeof (src.clone) == "function") {
            if (options.overwrite || target == null)
                return src.clone();
            return target;
        }

        if (src instanceof Array) {
            if (target == null)
                target = [];

            for (var i = 0; i < src.length; i++) {
                var item = src[i];
                var item2 = target[i];
                item2 = Q.copy(item, item2, options, depth + 1);
                target[i] = item2;
            }
            target.splice(src.length, target.length - src.length);
            return target;
        }
        if (src.constructor != Object) {
            if (options.overwrite || target == null)
                return src;
            return target;
        }

        if (target == null)
            target = {};
        for (var p in src) {
            var value = src[p];
            var value2 = target[p];
            value2 = Q.copy(value, value2, options, depth + 1);
            target[p] = value2;
        }
        return target;
    }
    Q.objectToNameValueArray = function () {
        var list = [];
        for (var p in this.obj) {
            list.push({ name: p, value: this.obj[p] });
        }
        return list;
    }
    Q.objectValuesToArray = function (obj) {
        var list = [];
        for (var p in obj) {
            list.push(obj[p]);
        }
        return list;
    }
    Q.cloneJson = function (obj) {
        if (obj == null)
            return null;
        return JSON.parse(JSON.stringify(obj));
    };
    Q.forEachValueInObject = function (obj, func, thisArg) {
        for (var p in obj) {
            func.call(thisArg, obj[p]);
        }
    };
    Q.mapKeyValueInArrayOrObject = function (objOrList, func, thisArg) {
        var list = [];
        if (objOrList instanceof Array) {
            for (var i = 0; i < objOrList.length; i++) {
                list.push(func.call(thisArg, i, objOrList[i]));
            }
        }
        else {
            for (var p in objOrList) {
                list.push(func.call(thisArg, p, objOrList[p]));
            }
        }
        return list;
    };
    //Alternative to $.map of jquery - which has array reducers overhead, and sometimes causes stackOverflow
    Q.jMap = function (objOrList, func, thisArg) {
        var list = [];
        if (objOrList instanceof Array) {
            for (var i = 0; i < objOrList.length; i++) {
                list.push(func.call(thisArg, objOrList[i], i));
            }
        }
        else {
            for (var p in objOrList) {
                list.push(func.call(thisArg, objOrList[p], p));
            }
        }
        return list;
    };
    ///Returns if the parameter is null, or an empty json object
    Q.isEmptyObject = function (obj) {
        if (obj == null)
            return true;
        if (typeof (obj) != "object")
            return false;
        for (var p in obj)
            return false;
        return true;
    };
    Q.min = function (list) {
        return Math.min.apply(null, list);
    };
    Q.max = function (list) {
        return Math.max.apply(null, list);
    };
    Q.stringifyFormatted = function (obj) {
        var sb = [];
        sb.indent = "";
        sb.indentSize = "    ";
        sb.startBlock = function (s, skipNewLine) {
            this.indent += sb.indentSize;
            this.push(s);
            if (!skipNewLine)
                this.newLine();
        };
        sb.endBlock = function (s, skipNewLine) {
            this.indent = this.indent.substr(0, this.indent.length - this.indentSize.length);
            if (!skipNewLine)
                this.newLine();
            this.push(s);
        };
        sb.newLine = function (s) {
            this.push("\n");
            this.push(this.indent);
        };
        Q.stringifyFormatted2(obj, sb);
        return sb.join("");
    }
    Q._canInlineObject = function (obj) {
        return Object.values(obj).all(function (t) { return t == null || typeof (t) != "object" });
    }
    Q._canInlineArray = function (list) {
        if (list.length == 0)
            return true;
        if (["string", "number"].contains(typeof (list[0])))
            return true;
        if (list.length == 1 && Q._canInlineObject(list[0]))
            return true;
        return false;

    }
    Q.stringifyFormatted2 = function (obj, sb) {
        if (obj === undefined) {
            sb.push("undefined");
            return;
        }
        if (obj === null) {
            sb.push("null");
            return;
        }
        var type = typeof (obj);
        if (type == "object") {
            if (obj instanceof Array) {
                var list = obj;
                if (Q._canInlineArray(list)) {
                    sb.push("[");
                    list.forEach(function (t, i) {
                        Q.stringifyFormatted2(t, sb);
                        if (i < list.length - 1)
                            sb.push(", ");
                    });
                    sb.push("]");
                }
                else {
                    sb.startBlock("[");
                    for (var i = 0; i < list.length; i++) {
                        Q.stringifyFormatted2(list[i], sb);
                        if (i < list.length - 1) {
                            sb.push(",");
                            sb.newLine();
                        }
                    }
                    sb.endBlock("]");
                }
            }
            else if (obj instanceof Date) {
                sb.push("new Date(" + obj.valueOf() + ")");
            }
            else {
                var canInline = Q._canInlineObject(obj);
                sb.startBlock("{", canInline);
                var first = true;
                for (var p in obj) {
                    if (first)
                        first = false;
                    else {
                        sb.push(",");
                        if (!canInline)
                            sb.newLine();
                    }
                    if (/^[$A-Z_][0-9A-Z_$]*$/i.test(p))
                        sb.push(p + ": ");
                    else
                        sb.push(JSON.stringify(p) + ": ");
                    Q.stringifyFormatted2(obj[p], sb);
                }
                sb.endBlock("}", canInline);
            }
        }
        else if (type == "function") {
            sb.push(obj.toString());
        }
        else {
            sb.push(JSON.stringify(obj));
        }
    }
    /* Binds all function on an object to the object, so the 'this' context will be reserved even if referencing the function alone */
    Q.bindFunctions = function (obj) {
        for (var p in obj) {
            var func = obj[p];
            if (typeof (func) != "function")
                continue;
            if (func.boundTo == obj)
                continue;
            func = func.bind(obj);
            func.boundTo = obj;
            if (func.name == null)
                func.name = p;
            obj[p] = func;
        }
    }

    Q.parseInt = function (s) {
        if (s == null)
            return null;
        if (typeof (s) == "number")
            return s;
        if (!String.isInt(s))
            return null;
        var x = parseInt(s);
        if (isNaN(x))
            return null;
        return x;
    }
    Q.parseFloat = function (s) {
        if (s == null)
            return null;
        if (typeof (s) == "number")
            return s;
        if (!String.isFloat(s))
            return null;
        var x = parseFloat(s);
        if (isNaN(x))
            return null;
        return x;
    }
    Q.createSelectorFunction = function (selector) {
        if (selector == null)
            return function (t) { return t; };
        if (typeof (selector) == "function")
            return selector;
        if (selector instanceof Array) {
            var list = selector;
            if (typeof (list[0]) == "string") {
                return function (t) {
                    var obj = {};
                    for (var i = 0; i < list.length; i++) {
                        var prop = list[i];
                        obj[prop] = t[prop];
                    }
                    return obj;
                };
            }

            var list2 = selector.map(Q.createSelectorFunction);
            return function (t) {
                var value = t;
                for (var i = 0; i < list2.length; i++) {
                    if (value == null)
                        return undefined;
                    var func = list2[i];
                    value = func(value);
                };
                return value;
            };
        }
        return function (t) { return t[selector]; };
    }
    Q.isNullOrEmpty = function (stringOrArray) {
        return stringOrArray == null || stringOrArray.length == 0;
    }
    Q.isNotNullOrEmpty = function (stringOrArray) {
        return stringOrArray != null && stringOrArray.length > 0;
    }
    Q.isNullEmptyOrZero = function (v) {
        return v == null || v == 0 || v.length == 0;
    }
    Q.isAny = function (v, vals) {
        return vals.any(function (t) { return v == t; })
    }

    Function.addTo(window, [Q]);
})();

//******** Utils
(function () {

    //function O(obj){
    //    for(var p in obj){
    //        this[p] = obj[p];
    //    }
    //}

    //O.prototype.getCreate

    function ArrayEnumerator(list) {
        this.index = -1;
        this.list = list;
    }
    ArrayEnumerator.prototype.moveNext = function () {
        if (this.index == -2)
            throw new Error("End of array");
        this.index++;
        if (this.index >= this.list.length) {
            this.index = -2;
            return false;
        }
        return true;
    }
    ArrayEnumerator.prototype.getCurrent = function () {
        if (this.index < 0)
            throw new Error("Invalid array position");
        return this.list[this.index];
    }

    function Comparer() {

    }
    Comparer.prototype.compare = function (x, y) {
        if (x > y)
            return 1;
        if (x < y)
            return -1;
        return 0;
    }
    Comparer._default = new Comparer();


    function Timer(action, ms) {
        this.action = action;
        if (ms != null)
            this.set(ms);
    }
    Timer.prototype.set = function (ms) {
        if (ms == null)
            ms = this._ms;
        else
            this._ms = ms;
        this.clear();
        if (ms == null)
            return;
        this.timeout = window.setTimeout(this.onTick.bind(this), ms);
    }
    Timer.prototype.onTick = function () {
        this.clear();
        this.action();
    }
    Timer.prototype.clear = function (ms) {
        if (this.timeout == null)
            return;
        window.clearTimeout(this.timeout);
        this.timeout = null;
    }

    function QueryString() {

    }
    QueryString.parse = function (query, obj, defaults) {
        if (query == null)
            query = window.location.search.substr(1);
        if (obj == null)
            obj = {};
        if (defaults == null)
            defaults = {};
        var index2 = query.indexOf("#");
        if (index2 >= 0)
            query = query.substr(0, index2);
        if (query.length == 0)
            return obj;
        var parts = query.split('&');
        var pairs = parts.select(function (part) { return part.split('='); });
        pairs.forEach(function (pair) {
            var key = pair[0];
            var eValue = pair[1];
            var value;
            var defaultValue = defaults[key];
            var currentValue = obj[key];
            if (currentValue == null || currentValue == defaultValue) {
                value = decodeURIComponent(eValue);
                obj[key] = value;
            }
            else if (currentValue instanceof Array || defaultValue instanceof Array) {
                if (currentValue == null) {
                    currentValue = [];
                    obj[key] = currentValue;
                }
                if (defaultValue != null && currentValue.itemsEqual(defaultValue))
                    currentValue.clear();
                if (eValue != "") {
                    var items = eValue.split(",").select(function (item) { return decodeURIComponent(item); });
                    items.forEach(function (item) {
                        if (!currentValue.contains(item))
                            currentValue.add(item);
                    });
                }
            }
            else if (currentValue != null) {
                value = decodeURIComponent(eValue);
                obj[key] = value;
                //value = decodeURIComponent(eValue);
                //obj[key] = [currentValue, value];
            }
            if (typeof (defaultValue) == "boolean" && typeof (value) != "boolean") {
                var boolValue = value == 1 || value == true || value == "1" || value == "true";
                obj[key] = boolValue;
            }

        });
        return obj;
    }
    QueryString.stringify = function (obj) {
        var sb = [];
        QueryString.write(obj, sb);
        return sb.join("&");
    }
    QueryString.write = function (obj, sb) {
        for (var p in obj) {
            var value = obj[p];
            if (value instanceof Array) {
                if (value.length > 0)
                    sb.push(p + "=" + value.select(function (item) { return encodeURIComponent(item); }).join(","));
            }
            else {
                sb.push(p + "=" + encodeURIComponent(value));
            }
        }
    }


    function ValueOfEqualityComparer() {

    }
    ValueOfEqualityComparer.prototype.equals = function (x, y) {
        if (x == y)
            return true;
        if (x == null || y == null)
            return false;
        return x.valueOf() == y.valueOf();
    }
    ValueOfEqualityComparer.prototype.getHashKey = function (x) {
        return Object.getHashKey(x);
    }

    Function.addTo(window, [ArrayEnumerator, Comparer, Timer, QueryString]);





    function combineCompareFuncs(compareFuncs) {
        return function (a, b) {
            var count = compareFuncs.length;
            for (var i = 0; i < count; i++) {
                var compare = compareFuncs[i];
                var x = compare(a, b);
                if (x != 0)
                    return x;
            }
            return 0;
        };
    }

    function createCompareFuncFromSelector(selector, desc) {
        desc = desc ? -1 : 1;
        var compare = Comparer._default.compare;
        var type = typeof (selector);
        if (type == "string" || type == "number") {
            return function (x, y) {
                return compare(x[selector], y[selector]) * desc;
            };
        }
        return function (x, y) {
            return compare(selector(x), selector(y)) * desc;
        };
    }

    function toStringOrEmpty(val) {
        return val == null ? "" : val.toString();
    }

    Function.addTo(window, [toStringOrEmpty, createCompareFuncFromSelector, combineCompareFuncs]);

})();


