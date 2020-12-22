/*eslint-disable block-scoped-var, id-length, no-control-regex, no-magic-numbers, no-prototype-builtins, no-redeclare, no-shadow, no-var, sort-vars*/
'use strict';

var $protobuf = require('protobufjs/minimal');

// Common aliases
var $Reader = $protobuf.Reader,
    $Writer = $protobuf.Writer,
    $util = $protobuf.util;

// Exported root namespace
var $root = $protobuf.roots['default'] || ($protobuf.roots['default'] = {});

$root.vtadmin = (function () {
    /**
     * Namespace vtadmin.
     * @exports vtadmin
     * @namespace
     */
    var vtadmin = {};

    vtadmin.VTAdmin = (function () {
        /**
         * Constructs a new VTAdmin service.
         * @memberof vtadmin
         * @classdesc Represents a VTAdmin
         * @extends $protobuf.rpc.Service
         * @constructor
         * @param {$protobuf.RPCImpl} rpcImpl RPC implementation
         * @param {boolean} [requestDelimited=false] Whether requests are length-delimited
         * @param {boolean} [responseDelimited=false] Whether responses are length-delimited
         */
        function VTAdmin(rpcImpl, requestDelimited, responseDelimited) {
            $protobuf.rpc.Service.call(this, rpcImpl, requestDelimited, responseDelimited);
        }

        (VTAdmin.prototype = Object.create($protobuf.rpc.Service.prototype)).constructor = VTAdmin;

        /**
         * Creates new VTAdmin service using the specified rpc implementation.
         * @function create
         * @memberof vtadmin.VTAdmin
         * @static
         * @param {$protobuf.RPCImpl} rpcImpl RPC implementation
         * @param {boolean} [requestDelimited=false] Whether requests are length-delimited
         * @param {boolean} [responseDelimited=false] Whether responses are length-delimited
         * @returns {VTAdmin} RPC service. Useful where requests and/or responses are streamed.
         */
        VTAdmin.create = function create(rpcImpl, requestDelimited, responseDelimited) {
            return new this(rpcImpl, requestDelimited, responseDelimited);
        };

        /**
         * Callback as used by {@link vtadmin.VTAdmin#getGates}.
         * @memberof vtadmin.VTAdmin
         * @typedef GetGatesCallback
         * @type {function}
         * @param {Error|null} error Error, if any
         * @param {vtadmin.GetGatesResponse} [response] GetGatesResponse
         */

        /**
         * Calls GetGates.
         * @function getGates
         * @memberof vtadmin.VTAdmin
         * @instance
         * @param {vtadmin.IGetGatesRequest} request GetGatesRequest message or plain object
         * @param {vtadmin.VTAdmin.GetGatesCallback} callback Node-style callback called with the error, if any, and GetGatesResponse
         * @returns {undefined}
         * @variation 1
         */
        Object.defineProperty(
            (VTAdmin.prototype.getGates = function getGates(request, callback) {
                return this.rpcCall(
                    getGates,
                    $root.vtadmin.GetGatesRequest,
                    $root.vtadmin.GetGatesResponse,
                    request,
                    callback
                );
            }),
            'name',
            { value: 'GetGates' }
        );

        /**
         * Calls GetGates.
         * @function getGates
         * @memberof vtadmin.VTAdmin
         * @instance
         * @param {vtadmin.IGetGatesRequest} request GetGatesRequest message or plain object
         * @returns {Promise<vtadmin.GetGatesResponse>} Promise
         * @variation 2
         */

        /**
         * Callback as used by {@link vtadmin.VTAdmin#getTablet}.
         * @memberof vtadmin.VTAdmin
         * @typedef GetTabletCallback
         * @type {function}
         * @param {Error|null} error Error, if any
         * @param {vtadmin.Tablet} [response] Tablet
         */

        /**
         * Calls GetTablet.
         * @function getTablet
         * @memberof vtadmin.VTAdmin
         * @instance
         * @param {vtadmin.IGetTabletRequest} request GetTabletRequest message or plain object
         * @param {vtadmin.VTAdmin.GetTabletCallback} callback Node-style callback called with the error, if any, and Tablet
         * @returns {undefined}
         * @variation 1
         */
        Object.defineProperty(
            (VTAdmin.prototype.getTablet = function getTablet(request, callback) {
                return this.rpcCall(getTablet, $root.vtadmin.GetTabletRequest, $root.vtadmin.Tablet, request, callback);
            }),
            'name',
            { value: 'GetTablet' }
        );

        /**
         * Calls GetTablet.
         * @function getTablet
         * @memberof vtadmin.VTAdmin
         * @instance
         * @param {vtadmin.IGetTabletRequest} request GetTabletRequest message or plain object
         * @returns {Promise<vtadmin.Tablet>} Promise
         * @variation 2
         */

        /**
         * Callback as used by {@link vtadmin.VTAdmin#getTablets}.
         * @memberof vtadmin.VTAdmin
         * @typedef GetTabletsCallback
         * @type {function}
         * @param {Error|null} error Error, if any
         * @param {vtadmin.GetTabletsResponse} [response] GetTabletsResponse
         */

        /**
         * Calls GetTablets.
         * @function getTablets
         * @memberof vtadmin.VTAdmin
         * @instance
         * @param {vtadmin.IGetTabletsRequest} request GetTabletsRequest message or plain object
         * @param {vtadmin.VTAdmin.GetTabletsCallback} callback Node-style callback called with the error, if any, and GetTabletsResponse
         * @returns {undefined}
         * @variation 1
         */
        Object.defineProperty(
            (VTAdmin.prototype.getTablets = function getTablets(request, callback) {
                return this.rpcCall(
                    getTablets,
                    $root.vtadmin.GetTabletsRequest,
                    $root.vtadmin.GetTabletsResponse,
                    request,
                    callback
                );
            }),
            'name',
            { value: 'GetTablets' }
        );

        /**
         * Calls GetTablets.
         * @function getTablets
         * @memberof vtadmin.VTAdmin
         * @instance
         * @param {vtadmin.IGetTabletsRequest} request GetTabletsRequest message or plain object
         * @returns {Promise<vtadmin.GetTabletsResponse>} Promise
         * @variation 2
         */

        return VTAdmin;
    })();

    vtadmin.Cluster = (function () {
        /**
         * Properties of a Cluster.
         * @memberof vtadmin
         * @interface ICluster
         * @property {string|null} [id] Cluster id
         * @property {string|null} [name] Cluster name
         */

        /**
         * Constructs a new Cluster.
         * @memberof vtadmin
         * @classdesc Represents a Cluster.
         * @implements ICluster
         * @constructor
         * @param {vtadmin.ICluster=} [properties] Properties to set
         */
        function Cluster(properties) {
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * Cluster id.
         * @member {string} id
         * @memberof vtadmin.Cluster
         * @instance
         */
        Cluster.prototype.id = '';

        /**
         * Cluster name.
         * @member {string} name
         * @memberof vtadmin.Cluster
         * @instance
         */
        Cluster.prototype.name = '';

        /**
         * Creates a new Cluster instance using the specified properties.
         * @function create
         * @memberof vtadmin.Cluster
         * @static
         * @param {vtadmin.ICluster=} [properties] Properties to set
         * @returns {vtadmin.Cluster} Cluster instance
         */
        Cluster.create = function create(properties) {
            return new Cluster(properties);
        };

        /**
         * Encodes the specified Cluster message. Does not implicitly {@link vtadmin.Cluster.verify|verify} messages.
         * @function encode
         * @memberof vtadmin.Cluster
         * @static
         * @param {vtadmin.ICluster} message Cluster message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        Cluster.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.id != null && Object.hasOwnProperty.call(message, 'id'))
                writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.id);
            if (message.name != null && Object.hasOwnProperty.call(message, 'name'))
                writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.name);
            return writer;
        };

        /**
         * Encodes the specified Cluster message, length delimited. Does not implicitly {@link vtadmin.Cluster.verify|verify} messages.
         * @function encodeDelimited
         * @memberof vtadmin.Cluster
         * @static
         * @param {vtadmin.ICluster} message Cluster message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        Cluster.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a Cluster message from the specified reader or buffer.
         * @function decode
         * @memberof vtadmin.Cluster
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {vtadmin.Cluster} Cluster
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        Cluster.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.vtadmin.Cluster();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        message.id = reader.string();
                        break;
                    case 2:
                        message.name = reader.string();
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a Cluster message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof vtadmin.Cluster
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {vtadmin.Cluster} Cluster
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        Cluster.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a Cluster message.
         * @function verify
         * @memberof vtadmin.Cluster
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        Cluster.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.id != null && message.hasOwnProperty('id'))
                if (!$util.isString(message.id)) return 'id: string expected';
            if (message.name != null && message.hasOwnProperty('name'))
                if (!$util.isString(message.name)) return 'name: string expected';
            return null;
        };

        /**
         * Creates a Cluster message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof vtadmin.Cluster
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {vtadmin.Cluster} Cluster
         */
        Cluster.fromObject = function fromObject(object) {
            if (object instanceof $root.vtadmin.Cluster) return object;
            var message = new $root.vtadmin.Cluster();
            if (object.id != null) message.id = String(object.id);
            if (object.name != null) message.name = String(object.name);
            return message;
        };

        /**
         * Creates a plain object from a Cluster message. Also converts values to other types if specified.
         * @function toObject
         * @memberof vtadmin.Cluster
         * @static
         * @param {vtadmin.Cluster} message Cluster
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        Cluster.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.defaults) {
                object.id = '';
                object.name = '';
            }
            if (message.id != null && message.hasOwnProperty('id')) object.id = message.id;
            if (message.name != null && message.hasOwnProperty('name')) object.name = message.name;
            return object;
        };

        /**
         * Converts this Cluster to JSON.
         * @function toJSON
         * @memberof vtadmin.Cluster
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        Cluster.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return Cluster;
    })();

    vtadmin.Tablet = (function () {
        /**
         * Properties of a Tablet.
         * @memberof vtadmin
         * @interface ITablet
         * @property {vtadmin.ICluster|null} [cluster] Tablet cluster
         * @property {topodata.ITablet|null} [tablet] Tablet tablet
         * @property {vtadmin.Tablet.ServingState|null} [state] Tablet state
         */

        /**
         * Constructs a new Tablet.
         * @memberof vtadmin
         * @classdesc Represents a Tablet.
         * @implements ITablet
         * @constructor
         * @param {vtadmin.ITablet=} [properties] Properties to set
         */
        function Tablet(properties) {
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * Tablet cluster.
         * @member {vtadmin.ICluster|null|undefined} cluster
         * @memberof vtadmin.Tablet
         * @instance
         */
        Tablet.prototype.cluster = null;

        /**
         * Tablet tablet.
         * @member {topodata.ITablet|null|undefined} tablet
         * @memberof vtadmin.Tablet
         * @instance
         */
        Tablet.prototype.tablet = null;

        /**
         * Tablet state.
         * @member {vtadmin.Tablet.ServingState} state
         * @memberof vtadmin.Tablet
         * @instance
         */
        Tablet.prototype.state = 0;

        /**
         * Creates a new Tablet instance using the specified properties.
         * @function create
         * @memberof vtadmin.Tablet
         * @static
         * @param {vtadmin.ITablet=} [properties] Properties to set
         * @returns {vtadmin.Tablet} Tablet instance
         */
        Tablet.create = function create(properties) {
            return new Tablet(properties);
        };

        /**
         * Encodes the specified Tablet message. Does not implicitly {@link vtadmin.Tablet.verify|verify} messages.
         * @function encode
         * @memberof vtadmin.Tablet
         * @static
         * @param {vtadmin.ITablet} message Tablet message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        Tablet.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.cluster != null && Object.hasOwnProperty.call(message, 'cluster'))
                $root.vtadmin.Cluster.encode(
                    message.cluster,
                    writer.uint32(/* id 1, wireType 2 =*/ 10).fork()
                ).ldelim();
            if (message.tablet != null && Object.hasOwnProperty.call(message, 'tablet'))
                $root.topodata.Tablet.encode(message.tablet, writer.uint32(/* id 2, wireType 2 =*/ 18).fork()).ldelim();
            if (message.state != null && Object.hasOwnProperty.call(message, 'state'))
                writer.uint32(/* id 3, wireType 0 =*/ 24).int32(message.state);
            return writer;
        };

        /**
         * Encodes the specified Tablet message, length delimited. Does not implicitly {@link vtadmin.Tablet.verify|verify} messages.
         * @function encodeDelimited
         * @memberof vtadmin.Tablet
         * @static
         * @param {vtadmin.ITablet} message Tablet message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        Tablet.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a Tablet message from the specified reader or buffer.
         * @function decode
         * @memberof vtadmin.Tablet
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {vtadmin.Tablet} Tablet
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        Tablet.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.vtadmin.Tablet();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        message.cluster = $root.vtadmin.Cluster.decode(reader, reader.uint32());
                        break;
                    case 2:
                        message.tablet = $root.topodata.Tablet.decode(reader, reader.uint32());
                        break;
                    case 3:
                        message.state = reader.int32();
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a Tablet message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof vtadmin.Tablet
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {vtadmin.Tablet} Tablet
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        Tablet.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a Tablet message.
         * @function verify
         * @memberof vtadmin.Tablet
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        Tablet.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.cluster != null && message.hasOwnProperty('cluster')) {
                var error = $root.vtadmin.Cluster.verify(message.cluster);
                if (error) return 'cluster.' + error;
            }
            if (message.tablet != null && message.hasOwnProperty('tablet')) {
                var error = $root.topodata.Tablet.verify(message.tablet);
                if (error) return 'tablet.' + error;
            }
            if (message.state != null && message.hasOwnProperty('state'))
                switch (message.state) {
                    default:
                        return 'state: enum value expected';
                    case 0:
                    case 1:
                    case 2:
                        break;
                }
            return null;
        };

        /**
         * Creates a Tablet message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof vtadmin.Tablet
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {vtadmin.Tablet} Tablet
         */
        Tablet.fromObject = function fromObject(object) {
            if (object instanceof $root.vtadmin.Tablet) return object;
            var message = new $root.vtadmin.Tablet();
            if (object.cluster != null) {
                if (typeof object.cluster !== 'object') throw TypeError('.vtadmin.Tablet.cluster: object expected');
                message.cluster = $root.vtadmin.Cluster.fromObject(object.cluster);
            }
            if (object.tablet != null) {
                if (typeof object.tablet !== 'object') throw TypeError('.vtadmin.Tablet.tablet: object expected');
                message.tablet = $root.topodata.Tablet.fromObject(object.tablet);
            }
            switch (object.state) {
                case 'UNKNOWN':
                case 0:
                    message.state = 0;
                    break;
                case 'SERVING':
                case 1:
                    message.state = 1;
                    break;
                case 'NOT_SERVING':
                case 2:
                    message.state = 2;
                    break;
            }
            return message;
        };

        /**
         * Creates a plain object from a Tablet message. Also converts values to other types if specified.
         * @function toObject
         * @memberof vtadmin.Tablet
         * @static
         * @param {vtadmin.Tablet} message Tablet
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        Tablet.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.defaults) {
                object.cluster = null;
                object.tablet = null;
                object.state = options.enums === String ? 'UNKNOWN' : 0;
            }
            if (message.cluster != null && message.hasOwnProperty('cluster'))
                object.cluster = $root.vtadmin.Cluster.toObject(message.cluster, options);
            if (message.tablet != null && message.hasOwnProperty('tablet'))
                object.tablet = $root.topodata.Tablet.toObject(message.tablet, options);
            if (message.state != null && message.hasOwnProperty('state'))
                object.state =
                    options.enums === String ? $root.vtadmin.Tablet.ServingState[message.state] : message.state;
            return object;
        };

        /**
         * Converts this Tablet to JSON.
         * @function toJSON
         * @memberof vtadmin.Tablet
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        Tablet.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        /**
         * ServingState enum.
         * @name vtadmin.Tablet.ServingState
         * @enum {number}
         * @property {number} UNKNOWN=0 UNKNOWN value
         * @property {number} SERVING=1 SERVING value
         * @property {number} NOT_SERVING=2 NOT_SERVING value
         */
        Tablet.ServingState = (function () {
            var valuesById = {},
                values = Object.create(valuesById);
            values[(valuesById[0] = 'UNKNOWN')] = 0;
            values[(valuesById[1] = 'SERVING')] = 1;
            values[(valuesById[2] = 'NOT_SERVING')] = 2;
            return values;
        })();

        return Tablet;
    })();

    vtadmin.VTGate = (function () {
        /**
         * Properties of a VTGate.
         * @memberof vtadmin
         * @interface IVTGate
         * @property {string|null} [hostname] VTGate hostname
         * @property {string|null} [pool] VTGate pool
         * @property {string|null} [cell] VTGate cell
         * @property {string|null} [cluster] VTGate cluster
         * @property {Array.<string>|null} [keyspaces] VTGate keyspaces
         */

        /**
         * Constructs a new VTGate.
         * @memberof vtadmin
         * @classdesc Represents a VTGate.
         * @implements IVTGate
         * @constructor
         * @param {vtadmin.IVTGate=} [properties] Properties to set
         */
        function VTGate(properties) {
            this.keyspaces = [];
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * VTGate hostname.
         * @member {string} hostname
         * @memberof vtadmin.VTGate
         * @instance
         */
        VTGate.prototype.hostname = '';

        /**
         * VTGate pool.
         * @member {string} pool
         * @memberof vtadmin.VTGate
         * @instance
         */
        VTGate.prototype.pool = '';

        /**
         * VTGate cell.
         * @member {string} cell
         * @memberof vtadmin.VTGate
         * @instance
         */
        VTGate.prototype.cell = '';

        /**
         * VTGate cluster.
         * @member {string} cluster
         * @memberof vtadmin.VTGate
         * @instance
         */
        VTGate.prototype.cluster = '';

        /**
         * VTGate keyspaces.
         * @member {Array.<string>} keyspaces
         * @memberof vtadmin.VTGate
         * @instance
         */
        VTGate.prototype.keyspaces = $util.emptyArray;

        /**
         * Creates a new VTGate instance using the specified properties.
         * @function create
         * @memberof vtadmin.VTGate
         * @static
         * @param {vtadmin.IVTGate=} [properties] Properties to set
         * @returns {vtadmin.VTGate} VTGate instance
         */
        VTGate.create = function create(properties) {
            return new VTGate(properties);
        };

        /**
         * Encodes the specified VTGate message. Does not implicitly {@link vtadmin.VTGate.verify|verify} messages.
         * @function encode
         * @memberof vtadmin.VTGate
         * @static
         * @param {vtadmin.IVTGate} message VTGate message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        VTGate.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.hostname != null && Object.hasOwnProperty.call(message, 'hostname'))
                writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.hostname);
            if (message.pool != null && Object.hasOwnProperty.call(message, 'pool'))
                writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.pool);
            if (message.cell != null && Object.hasOwnProperty.call(message, 'cell'))
                writer.uint32(/* id 3, wireType 2 =*/ 26).string(message.cell);
            if (message.cluster != null && Object.hasOwnProperty.call(message, 'cluster'))
                writer.uint32(/* id 4, wireType 2 =*/ 34).string(message.cluster);
            if (message.keyspaces != null && message.keyspaces.length)
                for (var i = 0; i < message.keyspaces.length; ++i)
                    writer.uint32(/* id 5, wireType 2 =*/ 42).string(message.keyspaces[i]);
            return writer;
        };

        /**
         * Encodes the specified VTGate message, length delimited. Does not implicitly {@link vtadmin.VTGate.verify|verify} messages.
         * @function encodeDelimited
         * @memberof vtadmin.VTGate
         * @static
         * @param {vtadmin.IVTGate} message VTGate message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        VTGate.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a VTGate message from the specified reader or buffer.
         * @function decode
         * @memberof vtadmin.VTGate
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {vtadmin.VTGate} VTGate
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        VTGate.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.vtadmin.VTGate();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        message.hostname = reader.string();
                        break;
                    case 2:
                        message.pool = reader.string();
                        break;
                    case 3:
                        message.cell = reader.string();
                        break;
                    case 4:
                        message.cluster = reader.string();
                        break;
                    case 5:
                        if (!(message.keyspaces && message.keyspaces.length)) message.keyspaces = [];
                        message.keyspaces.push(reader.string());
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a VTGate message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof vtadmin.VTGate
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {vtadmin.VTGate} VTGate
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        VTGate.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a VTGate message.
         * @function verify
         * @memberof vtadmin.VTGate
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        VTGate.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.hostname != null && message.hasOwnProperty('hostname'))
                if (!$util.isString(message.hostname)) return 'hostname: string expected';
            if (message.pool != null && message.hasOwnProperty('pool'))
                if (!$util.isString(message.pool)) return 'pool: string expected';
            if (message.cell != null && message.hasOwnProperty('cell'))
                if (!$util.isString(message.cell)) return 'cell: string expected';
            if (message.cluster != null && message.hasOwnProperty('cluster'))
                if (!$util.isString(message.cluster)) return 'cluster: string expected';
            if (message.keyspaces != null && message.hasOwnProperty('keyspaces')) {
                if (!Array.isArray(message.keyspaces)) return 'keyspaces: array expected';
                for (var i = 0; i < message.keyspaces.length; ++i)
                    if (!$util.isString(message.keyspaces[i])) return 'keyspaces: string[] expected';
            }
            return null;
        };

        /**
         * Creates a VTGate message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof vtadmin.VTGate
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {vtadmin.VTGate} VTGate
         */
        VTGate.fromObject = function fromObject(object) {
            if (object instanceof $root.vtadmin.VTGate) return object;
            var message = new $root.vtadmin.VTGate();
            if (object.hostname != null) message.hostname = String(object.hostname);
            if (object.pool != null) message.pool = String(object.pool);
            if (object.cell != null) message.cell = String(object.cell);
            if (object.cluster != null) message.cluster = String(object.cluster);
            if (object.keyspaces) {
                if (!Array.isArray(object.keyspaces)) throw TypeError('.vtadmin.VTGate.keyspaces: array expected');
                message.keyspaces = [];
                for (var i = 0; i < object.keyspaces.length; ++i) message.keyspaces[i] = String(object.keyspaces[i]);
            }
            return message;
        };

        /**
         * Creates a plain object from a VTGate message. Also converts values to other types if specified.
         * @function toObject
         * @memberof vtadmin.VTGate
         * @static
         * @param {vtadmin.VTGate} message VTGate
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        VTGate.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.arrays || options.defaults) object.keyspaces = [];
            if (options.defaults) {
                object.hostname = '';
                object.pool = '';
                object.cell = '';
                object.cluster = '';
            }
            if (message.hostname != null && message.hasOwnProperty('hostname')) object.hostname = message.hostname;
            if (message.pool != null && message.hasOwnProperty('pool')) object.pool = message.pool;
            if (message.cell != null && message.hasOwnProperty('cell')) object.cell = message.cell;
            if (message.cluster != null && message.hasOwnProperty('cluster')) object.cluster = message.cluster;
            if (message.keyspaces && message.keyspaces.length) {
                object.keyspaces = [];
                for (var j = 0; j < message.keyspaces.length; ++j) object.keyspaces[j] = message.keyspaces[j];
            }
            return object;
        };

        /**
         * Converts this VTGate to JSON.
         * @function toJSON
         * @memberof vtadmin.VTGate
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        VTGate.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return VTGate;
    })();

    vtadmin.GetGatesRequest = (function () {
        /**
         * Properties of a GetGatesRequest.
         * @memberof vtadmin
         * @interface IGetGatesRequest
         * @property {Array.<string>|null} [cluster_ids] GetGatesRequest cluster_ids
         */

        /**
         * Constructs a new GetGatesRequest.
         * @memberof vtadmin
         * @classdesc Represents a GetGatesRequest.
         * @implements IGetGatesRequest
         * @constructor
         * @param {vtadmin.IGetGatesRequest=} [properties] Properties to set
         */
        function GetGatesRequest(properties) {
            this.cluster_ids = [];
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * GetGatesRequest cluster_ids.
         * @member {Array.<string>} cluster_ids
         * @memberof vtadmin.GetGatesRequest
         * @instance
         */
        GetGatesRequest.prototype.cluster_ids = $util.emptyArray;

        /**
         * Creates a new GetGatesRequest instance using the specified properties.
         * @function create
         * @memberof vtadmin.GetGatesRequest
         * @static
         * @param {vtadmin.IGetGatesRequest=} [properties] Properties to set
         * @returns {vtadmin.GetGatesRequest} GetGatesRequest instance
         */
        GetGatesRequest.create = function create(properties) {
            return new GetGatesRequest(properties);
        };

        /**
         * Encodes the specified GetGatesRequest message. Does not implicitly {@link vtadmin.GetGatesRequest.verify|verify} messages.
         * @function encode
         * @memberof vtadmin.GetGatesRequest
         * @static
         * @param {vtadmin.IGetGatesRequest} message GetGatesRequest message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        GetGatesRequest.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.cluster_ids != null && message.cluster_ids.length)
                for (var i = 0; i < message.cluster_ids.length; ++i)
                    writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.cluster_ids[i]);
            return writer;
        };

        /**
         * Encodes the specified GetGatesRequest message, length delimited. Does not implicitly {@link vtadmin.GetGatesRequest.verify|verify} messages.
         * @function encodeDelimited
         * @memberof vtadmin.GetGatesRequest
         * @static
         * @param {vtadmin.IGetGatesRequest} message GetGatesRequest message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        GetGatesRequest.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a GetGatesRequest message from the specified reader or buffer.
         * @function decode
         * @memberof vtadmin.GetGatesRequest
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {vtadmin.GetGatesRequest} GetGatesRequest
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        GetGatesRequest.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.vtadmin.GetGatesRequest();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        if (!(message.cluster_ids && message.cluster_ids.length)) message.cluster_ids = [];
                        message.cluster_ids.push(reader.string());
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a GetGatesRequest message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof vtadmin.GetGatesRequest
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {vtadmin.GetGatesRequest} GetGatesRequest
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        GetGatesRequest.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a GetGatesRequest message.
         * @function verify
         * @memberof vtadmin.GetGatesRequest
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        GetGatesRequest.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.cluster_ids != null && message.hasOwnProperty('cluster_ids')) {
                if (!Array.isArray(message.cluster_ids)) return 'cluster_ids: array expected';
                for (var i = 0; i < message.cluster_ids.length; ++i)
                    if (!$util.isString(message.cluster_ids[i])) return 'cluster_ids: string[] expected';
            }
            return null;
        };

        /**
         * Creates a GetGatesRequest message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof vtadmin.GetGatesRequest
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {vtadmin.GetGatesRequest} GetGatesRequest
         */
        GetGatesRequest.fromObject = function fromObject(object) {
            if (object instanceof $root.vtadmin.GetGatesRequest) return object;
            var message = new $root.vtadmin.GetGatesRequest();
            if (object.cluster_ids) {
                if (!Array.isArray(object.cluster_ids))
                    throw TypeError('.vtadmin.GetGatesRequest.cluster_ids: array expected');
                message.cluster_ids = [];
                for (var i = 0; i < object.cluster_ids.length; ++i)
                    message.cluster_ids[i] = String(object.cluster_ids[i]);
            }
            return message;
        };

        /**
         * Creates a plain object from a GetGatesRequest message. Also converts values to other types if specified.
         * @function toObject
         * @memberof vtadmin.GetGatesRequest
         * @static
         * @param {vtadmin.GetGatesRequest} message GetGatesRequest
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        GetGatesRequest.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.arrays || options.defaults) object.cluster_ids = [];
            if (message.cluster_ids && message.cluster_ids.length) {
                object.cluster_ids = [];
                for (var j = 0; j < message.cluster_ids.length; ++j) object.cluster_ids[j] = message.cluster_ids[j];
            }
            return object;
        };

        /**
         * Converts this GetGatesRequest to JSON.
         * @function toJSON
         * @memberof vtadmin.GetGatesRequest
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        GetGatesRequest.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return GetGatesRequest;
    })();

    vtadmin.GetGatesResponse = (function () {
        /**
         * Properties of a GetGatesResponse.
         * @memberof vtadmin
         * @interface IGetGatesResponse
         * @property {Array.<vtadmin.IVTGate>|null} [gates] GetGatesResponse gates
         */

        /**
         * Constructs a new GetGatesResponse.
         * @memberof vtadmin
         * @classdesc Represents a GetGatesResponse.
         * @implements IGetGatesResponse
         * @constructor
         * @param {vtadmin.IGetGatesResponse=} [properties] Properties to set
         */
        function GetGatesResponse(properties) {
            this.gates = [];
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * GetGatesResponse gates.
         * @member {Array.<vtadmin.IVTGate>} gates
         * @memberof vtadmin.GetGatesResponse
         * @instance
         */
        GetGatesResponse.prototype.gates = $util.emptyArray;

        /**
         * Creates a new GetGatesResponse instance using the specified properties.
         * @function create
         * @memberof vtadmin.GetGatesResponse
         * @static
         * @param {vtadmin.IGetGatesResponse=} [properties] Properties to set
         * @returns {vtadmin.GetGatesResponse} GetGatesResponse instance
         */
        GetGatesResponse.create = function create(properties) {
            return new GetGatesResponse(properties);
        };

        /**
         * Encodes the specified GetGatesResponse message. Does not implicitly {@link vtadmin.GetGatesResponse.verify|verify} messages.
         * @function encode
         * @memberof vtadmin.GetGatesResponse
         * @static
         * @param {vtadmin.IGetGatesResponse} message GetGatesResponse message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        GetGatesResponse.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.gates != null && message.gates.length)
                for (var i = 0; i < message.gates.length; ++i)
                    $root.vtadmin.VTGate.encode(
                        message.gates[i],
                        writer.uint32(/* id 1, wireType 2 =*/ 10).fork()
                    ).ldelim();
            return writer;
        };

        /**
         * Encodes the specified GetGatesResponse message, length delimited. Does not implicitly {@link vtadmin.GetGatesResponse.verify|verify} messages.
         * @function encodeDelimited
         * @memberof vtadmin.GetGatesResponse
         * @static
         * @param {vtadmin.IGetGatesResponse} message GetGatesResponse message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        GetGatesResponse.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a GetGatesResponse message from the specified reader or buffer.
         * @function decode
         * @memberof vtadmin.GetGatesResponse
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {vtadmin.GetGatesResponse} GetGatesResponse
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        GetGatesResponse.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.vtadmin.GetGatesResponse();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        if (!(message.gates && message.gates.length)) message.gates = [];
                        message.gates.push($root.vtadmin.VTGate.decode(reader, reader.uint32()));
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a GetGatesResponse message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof vtadmin.GetGatesResponse
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {vtadmin.GetGatesResponse} GetGatesResponse
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        GetGatesResponse.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a GetGatesResponse message.
         * @function verify
         * @memberof vtadmin.GetGatesResponse
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        GetGatesResponse.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.gates != null && message.hasOwnProperty('gates')) {
                if (!Array.isArray(message.gates)) return 'gates: array expected';
                for (var i = 0; i < message.gates.length; ++i) {
                    var error = $root.vtadmin.VTGate.verify(message.gates[i]);
                    if (error) return 'gates.' + error;
                }
            }
            return null;
        };

        /**
         * Creates a GetGatesResponse message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof vtadmin.GetGatesResponse
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {vtadmin.GetGatesResponse} GetGatesResponse
         */
        GetGatesResponse.fromObject = function fromObject(object) {
            if (object instanceof $root.vtadmin.GetGatesResponse) return object;
            var message = new $root.vtadmin.GetGatesResponse();
            if (object.gates) {
                if (!Array.isArray(object.gates)) throw TypeError('.vtadmin.GetGatesResponse.gates: array expected');
                message.gates = [];
                for (var i = 0; i < object.gates.length; ++i) {
                    if (typeof object.gates[i] !== 'object')
                        throw TypeError('.vtadmin.GetGatesResponse.gates: object expected');
                    message.gates[i] = $root.vtadmin.VTGate.fromObject(object.gates[i]);
                }
            }
            return message;
        };

        /**
         * Creates a plain object from a GetGatesResponse message. Also converts values to other types if specified.
         * @function toObject
         * @memberof vtadmin.GetGatesResponse
         * @static
         * @param {vtadmin.GetGatesResponse} message GetGatesResponse
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        GetGatesResponse.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.arrays || options.defaults) object.gates = [];
            if (message.gates && message.gates.length) {
                object.gates = [];
                for (var j = 0; j < message.gates.length; ++j)
                    object.gates[j] = $root.vtadmin.VTGate.toObject(message.gates[j], options);
            }
            return object;
        };

        /**
         * Converts this GetGatesResponse to JSON.
         * @function toJSON
         * @memberof vtadmin.GetGatesResponse
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        GetGatesResponse.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return GetGatesResponse;
    })();

    vtadmin.GetTabletRequest = (function () {
        /**
         * Properties of a GetTabletRequest.
         * @memberof vtadmin
         * @interface IGetTabletRequest
         * @property {string|null} [hostname] GetTabletRequest hostname
         * @property {Array.<string>|null} [cluster_ids] GetTabletRequest cluster_ids
         */

        /**
         * Constructs a new GetTabletRequest.
         * @memberof vtadmin
         * @classdesc Represents a GetTabletRequest.
         * @implements IGetTabletRequest
         * @constructor
         * @param {vtadmin.IGetTabletRequest=} [properties] Properties to set
         */
        function GetTabletRequest(properties) {
            this.cluster_ids = [];
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * GetTabletRequest hostname.
         * @member {string} hostname
         * @memberof vtadmin.GetTabletRequest
         * @instance
         */
        GetTabletRequest.prototype.hostname = '';

        /**
         * GetTabletRequest cluster_ids.
         * @member {Array.<string>} cluster_ids
         * @memberof vtadmin.GetTabletRequest
         * @instance
         */
        GetTabletRequest.prototype.cluster_ids = $util.emptyArray;

        /**
         * Creates a new GetTabletRequest instance using the specified properties.
         * @function create
         * @memberof vtadmin.GetTabletRequest
         * @static
         * @param {vtadmin.IGetTabletRequest=} [properties] Properties to set
         * @returns {vtadmin.GetTabletRequest} GetTabletRequest instance
         */
        GetTabletRequest.create = function create(properties) {
            return new GetTabletRequest(properties);
        };

        /**
         * Encodes the specified GetTabletRequest message. Does not implicitly {@link vtadmin.GetTabletRequest.verify|verify} messages.
         * @function encode
         * @memberof vtadmin.GetTabletRequest
         * @static
         * @param {vtadmin.IGetTabletRequest} message GetTabletRequest message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        GetTabletRequest.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.hostname != null && Object.hasOwnProperty.call(message, 'hostname'))
                writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.hostname);
            if (message.cluster_ids != null && message.cluster_ids.length)
                for (var i = 0; i < message.cluster_ids.length; ++i)
                    writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.cluster_ids[i]);
            return writer;
        };

        /**
         * Encodes the specified GetTabletRequest message, length delimited. Does not implicitly {@link vtadmin.GetTabletRequest.verify|verify} messages.
         * @function encodeDelimited
         * @memberof vtadmin.GetTabletRequest
         * @static
         * @param {vtadmin.IGetTabletRequest} message GetTabletRequest message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        GetTabletRequest.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a GetTabletRequest message from the specified reader or buffer.
         * @function decode
         * @memberof vtadmin.GetTabletRequest
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {vtadmin.GetTabletRequest} GetTabletRequest
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        GetTabletRequest.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.vtadmin.GetTabletRequest();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        message.hostname = reader.string();
                        break;
                    case 2:
                        if (!(message.cluster_ids && message.cluster_ids.length)) message.cluster_ids = [];
                        message.cluster_ids.push(reader.string());
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a GetTabletRequest message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof vtadmin.GetTabletRequest
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {vtadmin.GetTabletRequest} GetTabletRequest
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        GetTabletRequest.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a GetTabletRequest message.
         * @function verify
         * @memberof vtadmin.GetTabletRequest
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        GetTabletRequest.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.hostname != null && message.hasOwnProperty('hostname'))
                if (!$util.isString(message.hostname)) return 'hostname: string expected';
            if (message.cluster_ids != null && message.hasOwnProperty('cluster_ids')) {
                if (!Array.isArray(message.cluster_ids)) return 'cluster_ids: array expected';
                for (var i = 0; i < message.cluster_ids.length; ++i)
                    if (!$util.isString(message.cluster_ids[i])) return 'cluster_ids: string[] expected';
            }
            return null;
        };

        /**
         * Creates a GetTabletRequest message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof vtadmin.GetTabletRequest
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {vtadmin.GetTabletRequest} GetTabletRequest
         */
        GetTabletRequest.fromObject = function fromObject(object) {
            if (object instanceof $root.vtadmin.GetTabletRequest) return object;
            var message = new $root.vtadmin.GetTabletRequest();
            if (object.hostname != null) message.hostname = String(object.hostname);
            if (object.cluster_ids) {
                if (!Array.isArray(object.cluster_ids))
                    throw TypeError('.vtadmin.GetTabletRequest.cluster_ids: array expected');
                message.cluster_ids = [];
                for (var i = 0; i < object.cluster_ids.length; ++i)
                    message.cluster_ids[i] = String(object.cluster_ids[i]);
            }
            return message;
        };

        /**
         * Creates a plain object from a GetTabletRequest message. Also converts values to other types if specified.
         * @function toObject
         * @memberof vtadmin.GetTabletRequest
         * @static
         * @param {vtadmin.GetTabletRequest} message GetTabletRequest
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        GetTabletRequest.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.arrays || options.defaults) object.cluster_ids = [];
            if (options.defaults) object.hostname = '';
            if (message.hostname != null && message.hasOwnProperty('hostname')) object.hostname = message.hostname;
            if (message.cluster_ids && message.cluster_ids.length) {
                object.cluster_ids = [];
                for (var j = 0; j < message.cluster_ids.length; ++j) object.cluster_ids[j] = message.cluster_ids[j];
            }
            return object;
        };

        /**
         * Converts this GetTabletRequest to JSON.
         * @function toJSON
         * @memberof vtadmin.GetTabletRequest
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        GetTabletRequest.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return GetTabletRequest;
    })();

    vtadmin.GetTabletsRequest = (function () {
        /**
         * Properties of a GetTabletsRequest.
         * @memberof vtadmin
         * @interface IGetTabletsRequest
         * @property {Array.<string>|null} [cluster_ids] GetTabletsRequest cluster_ids
         */

        /**
         * Constructs a new GetTabletsRequest.
         * @memberof vtadmin
         * @classdesc Represents a GetTabletsRequest.
         * @implements IGetTabletsRequest
         * @constructor
         * @param {vtadmin.IGetTabletsRequest=} [properties] Properties to set
         */
        function GetTabletsRequest(properties) {
            this.cluster_ids = [];
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * GetTabletsRequest cluster_ids.
         * @member {Array.<string>} cluster_ids
         * @memberof vtadmin.GetTabletsRequest
         * @instance
         */
        GetTabletsRequest.prototype.cluster_ids = $util.emptyArray;

        /**
         * Creates a new GetTabletsRequest instance using the specified properties.
         * @function create
         * @memberof vtadmin.GetTabletsRequest
         * @static
         * @param {vtadmin.IGetTabletsRequest=} [properties] Properties to set
         * @returns {vtadmin.GetTabletsRequest} GetTabletsRequest instance
         */
        GetTabletsRequest.create = function create(properties) {
            return new GetTabletsRequest(properties);
        };

        /**
         * Encodes the specified GetTabletsRequest message. Does not implicitly {@link vtadmin.GetTabletsRequest.verify|verify} messages.
         * @function encode
         * @memberof vtadmin.GetTabletsRequest
         * @static
         * @param {vtadmin.IGetTabletsRequest} message GetTabletsRequest message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        GetTabletsRequest.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.cluster_ids != null && message.cluster_ids.length)
                for (var i = 0; i < message.cluster_ids.length; ++i)
                    writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.cluster_ids[i]);
            return writer;
        };

        /**
         * Encodes the specified GetTabletsRequest message, length delimited. Does not implicitly {@link vtadmin.GetTabletsRequest.verify|verify} messages.
         * @function encodeDelimited
         * @memberof vtadmin.GetTabletsRequest
         * @static
         * @param {vtadmin.IGetTabletsRequest} message GetTabletsRequest message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        GetTabletsRequest.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a GetTabletsRequest message from the specified reader or buffer.
         * @function decode
         * @memberof vtadmin.GetTabletsRequest
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {vtadmin.GetTabletsRequest} GetTabletsRequest
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        GetTabletsRequest.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.vtadmin.GetTabletsRequest();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        if (!(message.cluster_ids && message.cluster_ids.length)) message.cluster_ids = [];
                        message.cluster_ids.push(reader.string());
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a GetTabletsRequest message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof vtadmin.GetTabletsRequest
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {vtadmin.GetTabletsRequest} GetTabletsRequest
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        GetTabletsRequest.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a GetTabletsRequest message.
         * @function verify
         * @memberof vtadmin.GetTabletsRequest
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        GetTabletsRequest.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.cluster_ids != null && message.hasOwnProperty('cluster_ids')) {
                if (!Array.isArray(message.cluster_ids)) return 'cluster_ids: array expected';
                for (var i = 0; i < message.cluster_ids.length; ++i)
                    if (!$util.isString(message.cluster_ids[i])) return 'cluster_ids: string[] expected';
            }
            return null;
        };

        /**
         * Creates a GetTabletsRequest message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof vtadmin.GetTabletsRequest
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {vtadmin.GetTabletsRequest} GetTabletsRequest
         */
        GetTabletsRequest.fromObject = function fromObject(object) {
            if (object instanceof $root.vtadmin.GetTabletsRequest) return object;
            var message = new $root.vtadmin.GetTabletsRequest();
            if (object.cluster_ids) {
                if (!Array.isArray(object.cluster_ids))
                    throw TypeError('.vtadmin.GetTabletsRequest.cluster_ids: array expected');
                message.cluster_ids = [];
                for (var i = 0; i < object.cluster_ids.length; ++i)
                    message.cluster_ids[i] = String(object.cluster_ids[i]);
            }
            return message;
        };

        /**
         * Creates a plain object from a GetTabletsRequest message. Also converts values to other types if specified.
         * @function toObject
         * @memberof vtadmin.GetTabletsRequest
         * @static
         * @param {vtadmin.GetTabletsRequest} message GetTabletsRequest
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        GetTabletsRequest.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.arrays || options.defaults) object.cluster_ids = [];
            if (message.cluster_ids && message.cluster_ids.length) {
                object.cluster_ids = [];
                for (var j = 0; j < message.cluster_ids.length; ++j) object.cluster_ids[j] = message.cluster_ids[j];
            }
            return object;
        };

        /**
         * Converts this GetTabletsRequest to JSON.
         * @function toJSON
         * @memberof vtadmin.GetTabletsRequest
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        GetTabletsRequest.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return GetTabletsRequest;
    })();

    vtadmin.GetTabletsResponse = (function () {
        /**
         * Properties of a GetTabletsResponse.
         * @memberof vtadmin
         * @interface IGetTabletsResponse
         * @property {Array.<vtadmin.ITablet>|null} [tablets] GetTabletsResponse tablets
         */

        /**
         * Constructs a new GetTabletsResponse.
         * @memberof vtadmin
         * @classdesc Represents a GetTabletsResponse.
         * @implements IGetTabletsResponse
         * @constructor
         * @param {vtadmin.IGetTabletsResponse=} [properties] Properties to set
         */
        function GetTabletsResponse(properties) {
            this.tablets = [];
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * GetTabletsResponse tablets.
         * @member {Array.<vtadmin.ITablet>} tablets
         * @memberof vtadmin.GetTabletsResponse
         * @instance
         */
        GetTabletsResponse.prototype.tablets = $util.emptyArray;

        /**
         * Creates a new GetTabletsResponse instance using the specified properties.
         * @function create
         * @memberof vtadmin.GetTabletsResponse
         * @static
         * @param {vtadmin.IGetTabletsResponse=} [properties] Properties to set
         * @returns {vtadmin.GetTabletsResponse} GetTabletsResponse instance
         */
        GetTabletsResponse.create = function create(properties) {
            return new GetTabletsResponse(properties);
        };

        /**
         * Encodes the specified GetTabletsResponse message. Does not implicitly {@link vtadmin.GetTabletsResponse.verify|verify} messages.
         * @function encode
         * @memberof vtadmin.GetTabletsResponse
         * @static
         * @param {vtadmin.IGetTabletsResponse} message GetTabletsResponse message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        GetTabletsResponse.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.tablets != null && message.tablets.length)
                for (var i = 0; i < message.tablets.length; ++i)
                    $root.vtadmin.Tablet.encode(
                        message.tablets[i],
                        writer.uint32(/* id 1, wireType 2 =*/ 10).fork()
                    ).ldelim();
            return writer;
        };

        /**
         * Encodes the specified GetTabletsResponse message, length delimited. Does not implicitly {@link vtadmin.GetTabletsResponse.verify|verify} messages.
         * @function encodeDelimited
         * @memberof vtadmin.GetTabletsResponse
         * @static
         * @param {vtadmin.IGetTabletsResponse} message GetTabletsResponse message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        GetTabletsResponse.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a GetTabletsResponse message from the specified reader or buffer.
         * @function decode
         * @memberof vtadmin.GetTabletsResponse
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {vtadmin.GetTabletsResponse} GetTabletsResponse
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        GetTabletsResponse.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.vtadmin.GetTabletsResponse();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        if (!(message.tablets && message.tablets.length)) message.tablets = [];
                        message.tablets.push($root.vtadmin.Tablet.decode(reader, reader.uint32()));
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a GetTabletsResponse message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof vtadmin.GetTabletsResponse
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {vtadmin.GetTabletsResponse} GetTabletsResponse
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        GetTabletsResponse.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a GetTabletsResponse message.
         * @function verify
         * @memberof vtadmin.GetTabletsResponse
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        GetTabletsResponse.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.tablets != null && message.hasOwnProperty('tablets')) {
                if (!Array.isArray(message.tablets)) return 'tablets: array expected';
                for (var i = 0; i < message.tablets.length; ++i) {
                    var error = $root.vtadmin.Tablet.verify(message.tablets[i]);
                    if (error) return 'tablets.' + error;
                }
            }
            return null;
        };

        /**
         * Creates a GetTabletsResponse message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof vtadmin.GetTabletsResponse
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {vtadmin.GetTabletsResponse} GetTabletsResponse
         */
        GetTabletsResponse.fromObject = function fromObject(object) {
            if (object instanceof $root.vtadmin.GetTabletsResponse) return object;
            var message = new $root.vtadmin.GetTabletsResponse();
            if (object.tablets) {
                if (!Array.isArray(object.tablets))
                    throw TypeError('.vtadmin.GetTabletsResponse.tablets: array expected');
                message.tablets = [];
                for (var i = 0; i < object.tablets.length; ++i) {
                    if (typeof object.tablets[i] !== 'object')
                        throw TypeError('.vtadmin.GetTabletsResponse.tablets: object expected');
                    message.tablets[i] = $root.vtadmin.Tablet.fromObject(object.tablets[i]);
                }
            }
            return message;
        };

        /**
         * Creates a plain object from a GetTabletsResponse message. Also converts values to other types if specified.
         * @function toObject
         * @memberof vtadmin.GetTabletsResponse
         * @static
         * @param {vtadmin.GetTabletsResponse} message GetTabletsResponse
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        GetTabletsResponse.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.arrays || options.defaults) object.tablets = [];
            if (message.tablets && message.tablets.length) {
                object.tablets = [];
                for (var j = 0; j < message.tablets.length; ++j)
                    object.tablets[j] = $root.vtadmin.Tablet.toObject(message.tablets[j], options);
            }
            return object;
        };

        /**
         * Converts this GetTabletsResponse to JSON.
         * @function toJSON
         * @memberof vtadmin.GetTabletsResponse
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        GetTabletsResponse.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return GetTabletsResponse;
    })();

    return vtadmin;
})();

$root.topodata = (function () {
    /**
     * Namespace topodata.
     * @exports topodata
     * @namespace
     */
    var topodata = {};

    topodata.KeyRange = (function () {
        /**
         * Properties of a KeyRange.
         * @memberof topodata
         * @interface IKeyRange
         * @property {Uint8Array|null} [start] KeyRange start
         * @property {Uint8Array|null} [end] KeyRange end
         */

        /**
         * Constructs a new KeyRange.
         * @memberof topodata
         * @classdesc Represents a KeyRange.
         * @implements IKeyRange
         * @constructor
         * @param {topodata.IKeyRange=} [properties] Properties to set
         */
        function KeyRange(properties) {
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * KeyRange start.
         * @member {Uint8Array} start
         * @memberof topodata.KeyRange
         * @instance
         */
        KeyRange.prototype.start = $util.newBuffer([]);

        /**
         * KeyRange end.
         * @member {Uint8Array} end
         * @memberof topodata.KeyRange
         * @instance
         */
        KeyRange.prototype.end = $util.newBuffer([]);

        /**
         * Creates a new KeyRange instance using the specified properties.
         * @function create
         * @memberof topodata.KeyRange
         * @static
         * @param {topodata.IKeyRange=} [properties] Properties to set
         * @returns {topodata.KeyRange} KeyRange instance
         */
        KeyRange.create = function create(properties) {
            return new KeyRange(properties);
        };

        /**
         * Encodes the specified KeyRange message. Does not implicitly {@link topodata.KeyRange.verify|verify} messages.
         * @function encode
         * @memberof topodata.KeyRange
         * @static
         * @param {topodata.IKeyRange} message KeyRange message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        KeyRange.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.start != null && Object.hasOwnProperty.call(message, 'start'))
                writer.uint32(/* id 1, wireType 2 =*/ 10).bytes(message.start);
            if (message.end != null && Object.hasOwnProperty.call(message, 'end'))
                writer.uint32(/* id 2, wireType 2 =*/ 18).bytes(message.end);
            return writer;
        };

        /**
         * Encodes the specified KeyRange message, length delimited. Does not implicitly {@link topodata.KeyRange.verify|verify} messages.
         * @function encodeDelimited
         * @memberof topodata.KeyRange
         * @static
         * @param {topodata.IKeyRange} message KeyRange message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        KeyRange.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a KeyRange message from the specified reader or buffer.
         * @function decode
         * @memberof topodata.KeyRange
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {topodata.KeyRange} KeyRange
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        KeyRange.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.topodata.KeyRange();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        message.start = reader.bytes();
                        break;
                    case 2:
                        message.end = reader.bytes();
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a KeyRange message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof topodata.KeyRange
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {topodata.KeyRange} KeyRange
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        KeyRange.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a KeyRange message.
         * @function verify
         * @memberof topodata.KeyRange
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        KeyRange.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.start != null && message.hasOwnProperty('start'))
                if (!((message.start && typeof message.start.length === 'number') || $util.isString(message.start)))
                    return 'start: buffer expected';
            if (message.end != null && message.hasOwnProperty('end'))
                if (!((message.end && typeof message.end.length === 'number') || $util.isString(message.end)))
                    return 'end: buffer expected';
            return null;
        };

        /**
         * Creates a KeyRange message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof topodata.KeyRange
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {topodata.KeyRange} KeyRange
         */
        KeyRange.fromObject = function fromObject(object) {
            if (object instanceof $root.topodata.KeyRange) return object;
            var message = new $root.topodata.KeyRange();
            if (object.start != null)
                if (typeof object.start === 'string')
                    $util.base64.decode(
                        object.start,
                        (message.start = $util.newBuffer($util.base64.length(object.start))),
                        0
                    );
                else if (object.start.length) message.start = object.start;
            if (object.end != null)
                if (typeof object.end === 'string')
                    $util.base64.decode(
                        object.end,
                        (message.end = $util.newBuffer($util.base64.length(object.end))),
                        0
                    );
                else if (object.end.length) message.end = object.end;
            return message;
        };

        /**
         * Creates a plain object from a KeyRange message. Also converts values to other types if specified.
         * @function toObject
         * @memberof topodata.KeyRange
         * @static
         * @param {topodata.KeyRange} message KeyRange
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        KeyRange.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.defaults) {
                if (options.bytes === String) object.start = '';
                else {
                    object.start = [];
                    if (options.bytes !== Array) object.start = $util.newBuffer(object.start);
                }
                if (options.bytes === String) object.end = '';
                else {
                    object.end = [];
                    if (options.bytes !== Array) object.end = $util.newBuffer(object.end);
                }
            }
            if (message.start != null && message.hasOwnProperty('start'))
                object.start =
                    options.bytes === String
                        ? $util.base64.encode(message.start, 0, message.start.length)
                        : options.bytes === Array
                        ? Array.prototype.slice.call(message.start)
                        : message.start;
            if (message.end != null && message.hasOwnProperty('end'))
                object.end =
                    options.bytes === String
                        ? $util.base64.encode(message.end, 0, message.end.length)
                        : options.bytes === Array
                        ? Array.prototype.slice.call(message.end)
                        : message.end;
            return object;
        };

        /**
         * Converts this KeyRange to JSON.
         * @function toJSON
         * @memberof topodata.KeyRange
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        KeyRange.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return KeyRange;
    })();

    /**
     * KeyspaceType enum.
     * @name topodata.KeyspaceType
     * @enum {number}
     * @property {number} NORMAL=0 NORMAL value
     * @property {number} SNAPSHOT=1 SNAPSHOT value
     */
    topodata.KeyspaceType = (function () {
        var valuesById = {},
            values = Object.create(valuesById);
        values[(valuesById[0] = 'NORMAL')] = 0;
        values[(valuesById[1] = 'SNAPSHOT')] = 1;
        return values;
    })();

    /**
     * KeyspaceIdType enum.
     * @name topodata.KeyspaceIdType
     * @enum {number}
     * @property {number} UNSET=0 UNSET value
     * @property {number} UINT64=1 UINT64 value
     * @property {number} BYTES=2 BYTES value
     */
    topodata.KeyspaceIdType = (function () {
        var valuesById = {},
            values = Object.create(valuesById);
        values[(valuesById[0] = 'UNSET')] = 0;
        values[(valuesById[1] = 'UINT64')] = 1;
        values[(valuesById[2] = 'BYTES')] = 2;
        return values;
    })();

    topodata.TabletAlias = (function () {
        /**
         * Properties of a TabletAlias.
         * @memberof topodata
         * @interface ITabletAlias
         * @property {string|null} [cell] TabletAlias cell
         * @property {number|null} [uid] TabletAlias uid
         */

        /**
         * Constructs a new TabletAlias.
         * @memberof topodata
         * @classdesc Represents a TabletAlias.
         * @implements ITabletAlias
         * @constructor
         * @param {topodata.ITabletAlias=} [properties] Properties to set
         */
        function TabletAlias(properties) {
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * TabletAlias cell.
         * @member {string} cell
         * @memberof topodata.TabletAlias
         * @instance
         */
        TabletAlias.prototype.cell = '';

        /**
         * TabletAlias uid.
         * @member {number} uid
         * @memberof topodata.TabletAlias
         * @instance
         */
        TabletAlias.prototype.uid = 0;

        /**
         * Creates a new TabletAlias instance using the specified properties.
         * @function create
         * @memberof topodata.TabletAlias
         * @static
         * @param {topodata.ITabletAlias=} [properties] Properties to set
         * @returns {topodata.TabletAlias} TabletAlias instance
         */
        TabletAlias.create = function create(properties) {
            return new TabletAlias(properties);
        };

        /**
         * Encodes the specified TabletAlias message. Does not implicitly {@link topodata.TabletAlias.verify|verify} messages.
         * @function encode
         * @memberof topodata.TabletAlias
         * @static
         * @param {topodata.ITabletAlias} message TabletAlias message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        TabletAlias.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.cell != null && Object.hasOwnProperty.call(message, 'cell'))
                writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.cell);
            if (message.uid != null && Object.hasOwnProperty.call(message, 'uid'))
                writer.uint32(/* id 2, wireType 0 =*/ 16).uint32(message.uid);
            return writer;
        };

        /**
         * Encodes the specified TabletAlias message, length delimited. Does not implicitly {@link topodata.TabletAlias.verify|verify} messages.
         * @function encodeDelimited
         * @memberof topodata.TabletAlias
         * @static
         * @param {topodata.ITabletAlias} message TabletAlias message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        TabletAlias.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a TabletAlias message from the specified reader or buffer.
         * @function decode
         * @memberof topodata.TabletAlias
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {topodata.TabletAlias} TabletAlias
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        TabletAlias.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.topodata.TabletAlias();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        message.cell = reader.string();
                        break;
                    case 2:
                        message.uid = reader.uint32();
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a TabletAlias message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof topodata.TabletAlias
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {topodata.TabletAlias} TabletAlias
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        TabletAlias.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a TabletAlias message.
         * @function verify
         * @memberof topodata.TabletAlias
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        TabletAlias.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.cell != null && message.hasOwnProperty('cell'))
                if (!$util.isString(message.cell)) return 'cell: string expected';
            if (message.uid != null && message.hasOwnProperty('uid'))
                if (!$util.isInteger(message.uid)) return 'uid: integer expected';
            return null;
        };

        /**
         * Creates a TabletAlias message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof topodata.TabletAlias
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {topodata.TabletAlias} TabletAlias
         */
        TabletAlias.fromObject = function fromObject(object) {
            if (object instanceof $root.topodata.TabletAlias) return object;
            var message = new $root.topodata.TabletAlias();
            if (object.cell != null) message.cell = String(object.cell);
            if (object.uid != null) message.uid = object.uid >>> 0;
            return message;
        };

        /**
         * Creates a plain object from a TabletAlias message. Also converts values to other types if specified.
         * @function toObject
         * @memberof topodata.TabletAlias
         * @static
         * @param {topodata.TabletAlias} message TabletAlias
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        TabletAlias.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.defaults) {
                object.cell = '';
                object.uid = 0;
            }
            if (message.cell != null && message.hasOwnProperty('cell')) object.cell = message.cell;
            if (message.uid != null && message.hasOwnProperty('uid')) object.uid = message.uid;
            return object;
        };

        /**
         * Converts this TabletAlias to JSON.
         * @function toJSON
         * @memberof topodata.TabletAlias
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        TabletAlias.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return TabletAlias;
    })();

    /**
     * TabletType enum.
     * @name topodata.TabletType
     * @enum {number}
     * @property {number} UNKNOWN=0 UNKNOWN value
     * @property {number} MASTER=1 MASTER value
     * @property {number} REPLICA=2 REPLICA value
     * @property {number} RDONLY=3 RDONLY value
     * @property {number} BATCH=3 BATCH value
     * @property {number} SPARE=4 SPARE value
     * @property {number} EXPERIMENTAL=5 EXPERIMENTAL value
     * @property {number} BACKUP=6 BACKUP value
     * @property {number} RESTORE=7 RESTORE value
     * @property {number} DRAINED=8 DRAINED value
     */
    topodata.TabletType = (function () {
        var valuesById = {},
            values = Object.create(valuesById);
        values[(valuesById[0] = 'UNKNOWN')] = 0;
        values[(valuesById[1] = 'MASTER')] = 1;
        values[(valuesById[2] = 'REPLICA')] = 2;
        values[(valuesById[3] = 'RDONLY')] = 3;
        values['BATCH'] = 3;
        values[(valuesById[4] = 'SPARE')] = 4;
        values[(valuesById[5] = 'EXPERIMENTAL')] = 5;
        values[(valuesById[6] = 'BACKUP')] = 6;
        values[(valuesById[7] = 'RESTORE')] = 7;
        values[(valuesById[8] = 'DRAINED')] = 8;
        return values;
    })();

    topodata.Tablet = (function () {
        /**
         * Properties of a Tablet.
         * @memberof topodata
         * @interface ITablet
         * @property {topodata.ITabletAlias|null} [alias] Tablet alias
         * @property {string|null} [hostname] Tablet hostname
         * @property {Object.<string,number>|null} [port_map] Tablet port_map
         * @property {string|null} [keyspace] Tablet keyspace
         * @property {string|null} [shard] Tablet shard
         * @property {topodata.IKeyRange|null} [key_range] Tablet key_range
         * @property {topodata.TabletType|null} [type] Tablet type
         * @property {string|null} [db_name_override] Tablet db_name_override
         * @property {Object.<string,string>|null} [tags] Tablet tags
         * @property {string|null} [mysql_hostname] Tablet mysql_hostname
         * @property {number|null} [mysql_port] Tablet mysql_port
         * @property {vttime.ITime|null} [master_term_start_time] Tablet master_term_start_time
         */

        /**
         * Constructs a new Tablet.
         * @memberof topodata
         * @classdesc Represents a Tablet.
         * @implements ITablet
         * @constructor
         * @param {topodata.ITablet=} [properties] Properties to set
         */
        function Tablet(properties) {
            this.port_map = {};
            this.tags = {};
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * Tablet alias.
         * @member {topodata.ITabletAlias|null|undefined} alias
         * @memberof topodata.Tablet
         * @instance
         */
        Tablet.prototype.alias = null;

        /**
         * Tablet hostname.
         * @member {string} hostname
         * @memberof topodata.Tablet
         * @instance
         */
        Tablet.prototype.hostname = '';

        /**
         * Tablet port_map.
         * @member {Object.<string,number>} port_map
         * @memberof topodata.Tablet
         * @instance
         */
        Tablet.prototype.port_map = $util.emptyObject;

        /**
         * Tablet keyspace.
         * @member {string} keyspace
         * @memberof topodata.Tablet
         * @instance
         */
        Tablet.prototype.keyspace = '';

        /**
         * Tablet shard.
         * @member {string} shard
         * @memberof topodata.Tablet
         * @instance
         */
        Tablet.prototype.shard = '';

        /**
         * Tablet key_range.
         * @member {topodata.IKeyRange|null|undefined} key_range
         * @memberof topodata.Tablet
         * @instance
         */
        Tablet.prototype.key_range = null;

        /**
         * Tablet type.
         * @member {topodata.TabletType} type
         * @memberof topodata.Tablet
         * @instance
         */
        Tablet.prototype.type = 0;

        /**
         * Tablet db_name_override.
         * @member {string} db_name_override
         * @memberof topodata.Tablet
         * @instance
         */
        Tablet.prototype.db_name_override = '';

        /**
         * Tablet tags.
         * @member {Object.<string,string>} tags
         * @memberof topodata.Tablet
         * @instance
         */
        Tablet.prototype.tags = $util.emptyObject;

        /**
         * Tablet mysql_hostname.
         * @member {string} mysql_hostname
         * @memberof topodata.Tablet
         * @instance
         */
        Tablet.prototype.mysql_hostname = '';

        /**
         * Tablet mysql_port.
         * @member {number} mysql_port
         * @memberof topodata.Tablet
         * @instance
         */
        Tablet.prototype.mysql_port = 0;

        /**
         * Tablet master_term_start_time.
         * @member {vttime.ITime|null|undefined} master_term_start_time
         * @memberof topodata.Tablet
         * @instance
         */
        Tablet.prototype.master_term_start_time = null;

        /**
         * Creates a new Tablet instance using the specified properties.
         * @function create
         * @memberof topodata.Tablet
         * @static
         * @param {topodata.ITablet=} [properties] Properties to set
         * @returns {topodata.Tablet} Tablet instance
         */
        Tablet.create = function create(properties) {
            return new Tablet(properties);
        };

        /**
         * Encodes the specified Tablet message. Does not implicitly {@link topodata.Tablet.verify|verify} messages.
         * @function encode
         * @memberof topodata.Tablet
         * @static
         * @param {topodata.ITablet} message Tablet message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        Tablet.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.alias != null && Object.hasOwnProperty.call(message, 'alias'))
                $root.topodata.TabletAlias.encode(
                    message.alias,
                    writer.uint32(/* id 1, wireType 2 =*/ 10).fork()
                ).ldelim();
            if (message.hostname != null && Object.hasOwnProperty.call(message, 'hostname'))
                writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.hostname);
            if (message.port_map != null && Object.hasOwnProperty.call(message, 'port_map'))
                for (var keys = Object.keys(message.port_map), i = 0; i < keys.length; ++i)
                    writer
                        .uint32(/* id 4, wireType 2 =*/ 34)
                        .fork()
                        .uint32(/* id 1, wireType 2 =*/ 10)
                        .string(keys[i])
                        .uint32(/* id 2, wireType 0 =*/ 16)
                        .int32(message.port_map[keys[i]])
                        .ldelim();
            if (message.keyspace != null && Object.hasOwnProperty.call(message, 'keyspace'))
                writer.uint32(/* id 5, wireType 2 =*/ 42).string(message.keyspace);
            if (message.shard != null && Object.hasOwnProperty.call(message, 'shard'))
                writer.uint32(/* id 6, wireType 2 =*/ 50).string(message.shard);
            if (message.key_range != null && Object.hasOwnProperty.call(message, 'key_range'))
                $root.topodata.KeyRange.encode(
                    message.key_range,
                    writer.uint32(/* id 7, wireType 2 =*/ 58).fork()
                ).ldelim();
            if (message.type != null && Object.hasOwnProperty.call(message, 'type'))
                writer.uint32(/* id 8, wireType 0 =*/ 64).int32(message.type);
            if (message.db_name_override != null && Object.hasOwnProperty.call(message, 'db_name_override'))
                writer.uint32(/* id 9, wireType 2 =*/ 74).string(message.db_name_override);
            if (message.tags != null && Object.hasOwnProperty.call(message, 'tags'))
                for (var keys = Object.keys(message.tags), i = 0; i < keys.length; ++i)
                    writer
                        .uint32(/* id 10, wireType 2 =*/ 82)
                        .fork()
                        .uint32(/* id 1, wireType 2 =*/ 10)
                        .string(keys[i])
                        .uint32(/* id 2, wireType 2 =*/ 18)
                        .string(message.tags[keys[i]])
                        .ldelim();
            if (message.mysql_hostname != null && Object.hasOwnProperty.call(message, 'mysql_hostname'))
                writer.uint32(/* id 12, wireType 2 =*/ 98).string(message.mysql_hostname);
            if (message.mysql_port != null && Object.hasOwnProperty.call(message, 'mysql_port'))
                writer.uint32(/* id 13, wireType 0 =*/ 104).int32(message.mysql_port);
            if (message.master_term_start_time != null && Object.hasOwnProperty.call(message, 'master_term_start_time'))
                $root.vttime.Time.encode(
                    message.master_term_start_time,
                    writer.uint32(/* id 14, wireType 2 =*/ 114).fork()
                ).ldelim();
            return writer;
        };

        /**
         * Encodes the specified Tablet message, length delimited. Does not implicitly {@link topodata.Tablet.verify|verify} messages.
         * @function encodeDelimited
         * @memberof topodata.Tablet
         * @static
         * @param {topodata.ITablet} message Tablet message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        Tablet.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a Tablet message from the specified reader or buffer.
         * @function decode
         * @memberof topodata.Tablet
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {topodata.Tablet} Tablet
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        Tablet.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.topodata.Tablet(),
                key,
                value;
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        message.alias = $root.topodata.TabletAlias.decode(reader, reader.uint32());
                        break;
                    case 2:
                        message.hostname = reader.string();
                        break;
                    case 4:
                        if (message.port_map === $util.emptyObject) message.port_map = {};
                        var end2 = reader.uint32() + reader.pos;
                        key = '';
                        value = 0;
                        while (reader.pos < end2) {
                            var tag2 = reader.uint32();
                            switch (tag2 >>> 3) {
                                case 1:
                                    key = reader.string();
                                    break;
                                case 2:
                                    value = reader.int32();
                                    break;
                                default:
                                    reader.skipType(tag2 & 7);
                                    break;
                            }
                        }
                        message.port_map[key] = value;
                        break;
                    case 5:
                        message.keyspace = reader.string();
                        break;
                    case 6:
                        message.shard = reader.string();
                        break;
                    case 7:
                        message.key_range = $root.topodata.KeyRange.decode(reader, reader.uint32());
                        break;
                    case 8:
                        message.type = reader.int32();
                        break;
                    case 9:
                        message.db_name_override = reader.string();
                        break;
                    case 10:
                        if (message.tags === $util.emptyObject) message.tags = {};
                        var end2 = reader.uint32() + reader.pos;
                        key = '';
                        value = '';
                        while (reader.pos < end2) {
                            var tag2 = reader.uint32();
                            switch (tag2 >>> 3) {
                                case 1:
                                    key = reader.string();
                                    break;
                                case 2:
                                    value = reader.string();
                                    break;
                                default:
                                    reader.skipType(tag2 & 7);
                                    break;
                            }
                        }
                        message.tags[key] = value;
                        break;
                    case 12:
                        message.mysql_hostname = reader.string();
                        break;
                    case 13:
                        message.mysql_port = reader.int32();
                        break;
                    case 14:
                        message.master_term_start_time = $root.vttime.Time.decode(reader, reader.uint32());
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a Tablet message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof topodata.Tablet
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {topodata.Tablet} Tablet
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        Tablet.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a Tablet message.
         * @function verify
         * @memberof topodata.Tablet
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        Tablet.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.alias != null && message.hasOwnProperty('alias')) {
                var error = $root.topodata.TabletAlias.verify(message.alias);
                if (error) return 'alias.' + error;
            }
            if (message.hostname != null && message.hasOwnProperty('hostname'))
                if (!$util.isString(message.hostname)) return 'hostname: string expected';
            if (message.port_map != null && message.hasOwnProperty('port_map')) {
                if (!$util.isObject(message.port_map)) return 'port_map: object expected';
                var key = Object.keys(message.port_map);
                for (var i = 0; i < key.length; ++i)
                    if (!$util.isInteger(message.port_map[key[i]])) return 'port_map: integer{k:string} expected';
            }
            if (message.keyspace != null && message.hasOwnProperty('keyspace'))
                if (!$util.isString(message.keyspace)) return 'keyspace: string expected';
            if (message.shard != null && message.hasOwnProperty('shard'))
                if (!$util.isString(message.shard)) return 'shard: string expected';
            if (message.key_range != null && message.hasOwnProperty('key_range')) {
                var error = $root.topodata.KeyRange.verify(message.key_range);
                if (error) return 'key_range.' + error;
            }
            if (message.type != null && message.hasOwnProperty('type'))
                switch (message.type) {
                    default:
                        return 'type: enum value expected';
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                    case 8:
                        break;
                }
            if (message.db_name_override != null && message.hasOwnProperty('db_name_override'))
                if (!$util.isString(message.db_name_override)) return 'db_name_override: string expected';
            if (message.tags != null && message.hasOwnProperty('tags')) {
                if (!$util.isObject(message.tags)) return 'tags: object expected';
                var key = Object.keys(message.tags);
                for (var i = 0; i < key.length; ++i)
                    if (!$util.isString(message.tags[key[i]])) return 'tags: string{k:string} expected';
            }
            if (message.mysql_hostname != null && message.hasOwnProperty('mysql_hostname'))
                if (!$util.isString(message.mysql_hostname)) return 'mysql_hostname: string expected';
            if (message.mysql_port != null && message.hasOwnProperty('mysql_port'))
                if (!$util.isInteger(message.mysql_port)) return 'mysql_port: integer expected';
            if (message.master_term_start_time != null && message.hasOwnProperty('master_term_start_time')) {
                var error = $root.vttime.Time.verify(message.master_term_start_time);
                if (error) return 'master_term_start_time.' + error;
            }
            return null;
        };

        /**
         * Creates a Tablet message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof topodata.Tablet
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {topodata.Tablet} Tablet
         */
        Tablet.fromObject = function fromObject(object) {
            if (object instanceof $root.topodata.Tablet) return object;
            var message = new $root.topodata.Tablet();
            if (object.alias != null) {
                if (typeof object.alias !== 'object') throw TypeError('.topodata.Tablet.alias: object expected');
                message.alias = $root.topodata.TabletAlias.fromObject(object.alias);
            }
            if (object.hostname != null) message.hostname = String(object.hostname);
            if (object.port_map) {
                if (typeof object.port_map !== 'object') throw TypeError('.topodata.Tablet.port_map: object expected');
                message.port_map = {};
                for (var keys = Object.keys(object.port_map), i = 0; i < keys.length; ++i)
                    message.port_map[keys[i]] = object.port_map[keys[i]] | 0;
            }
            if (object.keyspace != null) message.keyspace = String(object.keyspace);
            if (object.shard != null) message.shard = String(object.shard);
            if (object.key_range != null) {
                if (typeof object.key_range !== 'object')
                    throw TypeError('.topodata.Tablet.key_range: object expected');
                message.key_range = $root.topodata.KeyRange.fromObject(object.key_range);
            }
            switch (object.type) {
                case 'UNKNOWN':
                case 0:
                    message.type = 0;
                    break;
                case 'MASTER':
                case 1:
                    message.type = 1;
                    break;
                case 'REPLICA':
                case 2:
                    message.type = 2;
                    break;
                case 'RDONLY':
                case 3:
                    message.type = 3;
                    break;
                case 'BATCH':
                case 3:
                    message.type = 3;
                    break;
                case 'SPARE':
                case 4:
                    message.type = 4;
                    break;
                case 'EXPERIMENTAL':
                case 5:
                    message.type = 5;
                    break;
                case 'BACKUP':
                case 6:
                    message.type = 6;
                    break;
                case 'RESTORE':
                case 7:
                    message.type = 7;
                    break;
                case 'DRAINED':
                case 8:
                    message.type = 8;
                    break;
            }
            if (object.db_name_override != null) message.db_name_override = String(object.db_name_override);
            if (object.tags) {
                if (typeof object.tags !== 'object') throw TypeError('.topodata.Tablet.tags: object expected');
                message.tags = {};
                for (var keys = Object.keys(object.tags), i = 0; i < keys.length; ++i)
                    message.tags[keys[i]] = String(object.tags[keys[i]]);
            }
            if (object.mysql_hostname != null) message.mysql_hostname = String(object.mysql_hostname);
            if (object.mysql_port != null) message.mysql_port = object.mysql_port | 0;
            if (object.master_term_start_time != null) {
                if (typeof object.master_term_start_time !== 'object')
                    throw TypeError('.topodata.Tablet.master_term_start_time: object expected');
                message.master_term_start_time = $root.vttime.Time.fromObject(object.master_term_start_time);
            }
            return message;
        };

        /**
         * Creates a plain object from a Tablet message. Also converts values to other types if specified.
         * @function toObject
         * @memberof topodata.Tablet
         * @static
         * @param {topodata.Tablet} message Tablet
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        Tablet.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.objects || options.defaults) {
                object.port_map = {};
                object.tags = {};
            }
            if (options.defaults) {
                object.alias = null;
                object.hostname = '';
                object.keyspace = '';
                object.shard = '';
                object.key_range = null;
                object.type = options.enums === String ? 'UNKNOWN' : 0;
                object.db_name_override = '';
                object.mysql_hostname = '';
                object.mysql_port = 0;
                object.master_term_start_time = null;
            }
            if (message.alias != null && message.hasOwnProperty('alias'))
                object.alias = $root.topodata.TabletAlias.toObject(message.alias, options);
            if (message.hostname != null && message.hasOwnProperty('hostname')) object.hostname = message.hostname;
            var keys2;
            if (message.port_map && (keys2 = Object.keys(message.port_map)).length) {
                object.port_map = {};
                for (var j = 0; j < keys2.length; ++j) object.port_map[keys2[j]] = message.port_map[keys2[j]];
            }
            if (message.keyspace != null && message.hasOwnProperty('keyspace')) object.keyspace = message.keyspace;
            if (message.shard != null && message.hasOwnProperty('shard')) object.shard = message.shard;
            if (message.key_range != null && message.hasOwnProperty('key_range'))
                object.key_range = $root.topodata.KeyRange.toObject(message.key_range, options);
            if (message.type != null && message.hasOwnProperty('type'))
                object.type = options.enums === String ? $root.topodata.TabletType[message.type] : message.type;
            if (message.db_name_override != null && message.hasOwnProperty('db_name_override'))
                object.db_name_override = message.db_name_override;
            if (message.tags && (keys2 = Object.keys(message.tags)).length) {
                object.tags = {};
                for (var j = 0; j < keys2.length; ++j) object.tags[keys2[j]] = message.tags[keys2[j]];
            }
            if (message.mysql_hostname != null && message.hasOwnProperty('mysql_hostname'))
                object.mysql_hostname = message.mysql_hostname;
            if (message.mysql_port != null && message.hasOwnProperty('mysql_port'))
                object.mysql_port = message.mysql_port;
            if (message.master_term_start_time != null && message.hasOwnProperty('master_term_start_time'))
                object.master_term_start_time = $root.vttime.Time.toObject(message.master_term_start_time, options);
            return object;
        };

        /**
         * Converts this Tablet to JSON.
         * @function toJSON
         * @memberof topodata.Tablet
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        Tablet.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return Tablet;
    })();

    topodata.Shard = (function () {
        /**
         * Properties of a Shard.
         * @memberof topodata
         * @interface IShard
         * @property {topodata.ITabletAlias|null} [master_alias] Shard master_alias
         * @property {vttime.ITime|null} [master_term_start_time] Shard master_term_start_time
         * @property {topodata.IKeyRange|null} [key_range] Shard key_range
         * @property {Array.<topodata.Shard.IServedType>|null} [served_types] Shard served_types
         * @property {Array.<topodata.Shard.ISourceShard>|null} [source_shards] Shard source_shards
         * @property {Array.<topodata.Shard.ITabletControl>|null} [tablet_controls] Shard tablet_controls
         * @property {boolean|null} [is_master_serving] Shard is_master_serving
         */

        /**
         * Constructs a new Shard.
         * @memberof topodata
         * @classdesc Represents a Shard.
         * @implements IShard
         * @constructor
         * @param {topodata.IShard=} [properties] Properties to set
         */
        function Shard(properties) {
            this.served_types = [];
            this.source_shards = [];
            this.tablet_controls = [];
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * Shard master_alias.
         * @member {topodata.ITabletAlias|null|undefined} master_alias
         * @memberof topodata.Shard
         * @instance
         */
        Shard.prototype.master_alias = null;

        /**
         * Shard master_term_start_time.
         * @member {vttime.ITime|null|undefined} master_term_start_time
         * @memberof topodata.Shard
         * @instance
         */
        Shard.prototype.master_term_start_time = null;

        /**
         * Shard key_range.
         * @member {topodata.IKeyRange|null|undefined} key_range
         * @memberof topodata.Shard
         * @instance
         */
        Shard.prototype.key_range = null;

        /**
         * Shard served_types.
         * @member {Array.<topodata.Shard.IServedType>} served_types
         * @memberof topodata.Shard
         * @instance
         */
        Shard.prototype.served_types = $util.emptyArray;

        /**
         * Shard source_shards.
         * @member {Array.<topodata.Shard.ISourceShard>} source_shards
         * @memberof topodata.Shard
         * @instance
         */
        Shard.prototype.source_shards = $util.emptyArray;

        /**
         * Shard tablet_controls.
         * @member {Array.<topodata.Shard.ITabletControl>} tablet_controls
         * @memberof topodata.Shard
         * @instance
         */
        Shard.prototype.tablet_controls = $util.emptyArray;

        /**
         * Shard is_master_serving.
         * @member {boolean} is_master_serving
         * @memberof topodata.Shard
         * @instance
         */
        Shard.prototype.is_master_serving = false;

        /**
         * Creates a new Shard instance using the specified properties.
         * @function create
         * @memberof topodata.Shard
         * @static
         * @param {topodata.IShard=} [properties] Properties to set
         * @returns {topodata.Shard} Shard instance
         */
        Shard.create = function create(properties) {
            return new Shard(properties);
        };

        /**
         * Encodes the specified Shard message. Does not implicitly {@link topodata.Shard.verify|verify} messages.
         * @function encode
         * @memberof topodata.Shard
         * @static
         * @param {topodata.IShard} message Shard message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        Shard.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.master_alias != null && Object.hasOwnProperty.call(message, 'master_alias'))
                $root.topodata.TabletAlias.encode(
                    message.master_alias,
                    writer.uint32(/* id 1, wireType 2 =*/ 10).fork()
                ).ldelim();
            if (message.key_range != null && Object.hasOwnProperty.call(message, 'key_range'))
                $root.topodata.KeyRange.encode(
                    message.key_range,
                    writer.uint32(/* id 2, wireType 2 =*/ 18).fork()
                ).ldelim();
            if (message.served_types != null && message.served_types.length)
                for (var i = 0; i < message.served_types.length; ++i)
                    $root.topodata.Shard.ServedType.encode(
                        message.served_types[i],
                        writer.uint32(/* id 3, wireType 2 =*/ 26).fork()
                    ).ldelim();
            if (message.source_shards != null && message.source_shards.length)
                for (var i = 0; i < message.source_shards.length; ++i)
                    $root.topodata.Shard.SourceShard.encode(
                        message.source_shards[i],
                        writer.uint32(/* id 4, wireType 2 =*/ 34).fork()
                    ).ldelim();
            if (message.tablet_controls != null && message.tablet_controls.length)
                for (var i = 0; i < message.tablet_controls.length; ++i)
                    $root.topodata.Shard.TabletControl.encode(
                        message.tablet_controls[i],
                        writer.uint32(/* id 6, wireType 2 =*/ 50).fork()
                    ).ldelim();
            if (message.is_master_serving != null && Object.hasOwnProperty.call(message, 'is_master_serving'))
                writer.uint32(/* id 7, wireType 0 =*/ 56).bool(message.is_master_serving);
            if (message.master_term_start_time != null && Object.hasOwnProperty.call(message, 'master_term_start_time'))
                $root.vttime.Time.encode(
                    message.master_term_start_time,
                    writer.uint32(/* id 8, wireType 2 =*/ 66).fork()
                ).ldelim();
            return writer;
        };

        /**
         * Encodes the specified Shard message, length delimited. Does not implicitly {@link topodata.Shard.verify|verify} messages.
         * @function encodeDelimited
         * @memberof topodata.Shard
         * @static
         * @param {topodata.IShard} message Shard message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        Shard.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a Shard message from the specified reader or buffer.
         * @function decode
         * @memberof topodata.Shard
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {topodata.Shard} Shard
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        Shard.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.topodata.Shard();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        message.master_alias = $root.topodata.TabletAlias.decode(reader, reader.uint32());
                        break;
                    case 8:
                        message.master_term_start_time = $root.vttime.Time.decode(reader, reader.uint32());
                        break;
                    case 2:
                        message.key_range = $root.topodata.KeyRange.decode(reader, reader.uint32());
                        break;
                    case 3:
                        if (!(message.served_types && message.served_types.length)) message.served_types = [];
                        message.served_types.push($root.topodata.Shard.ServedType.decode(reader, reader.uint32()));
                        break;
                    case 4:
                        if (!(message.source_shards && message.source_shards.length)) message.source_shards = [];
                        message.source_shards.push($root.topodata.Shard.SourceShard.decode(reader, reader.uint32()));
                        break;
                    case 6:
                        if (!(message.tablet_controls && message.tablet_controls.length)) message.tablet_controls = [];
                        message.tablet_controls.push(
                            $root.topodata.Shard.TabletControl.decode(reader, reader.uint32())
                        );
                        break;
                    case 7:
                        message.is_master_serving = reader.bool();
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a Shard message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof topodata.Shard
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {topodata.Shard} Shard
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        Shard.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a Shard message.
         * @function verify
         * @memberof topodata.Shard
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        Shard.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.master_alias != null && message.hasOwnProperty('master_alias')) {
                var error = $root.topodata.TabletAlias.verify(message.master_alias);
                if (error) return 'master_alias.' + error;
            }
            if (message.master_term_start_time != null && message.hasOwnProperty('master_term_start_time')) {
                var error = $root.vttime.Time.verify(message.master_term_start_time);
                if (error) return 'master_term_start_time.' + error;
            }
            if (message.key_range != null && message.hasOwnProperty('key_range')) {
                var error = $root.topodata.KeyRange.verify(message.key_range);
                if (error) return 'key_range.' + error;
            }
            if (message.served_types != null && message.hasOwnProperty('served_types')) {
                if (!Array.isArray(message.served_types)) return 'served_types: array expected';
                for (var i = 0; i < message.served_types.length; ++i) {
                    var error = $root.topodata.Shard.ServedType.verify(message.served_types[i]);
                    if (error) return 'served_types.' + error;
                }
            }
            if (message.source_shards != null && message.hasOwnProperty('source_shards')) {
                if (!Array.isArray(message.source_shards)) return 'source_shards: array expected';
                for (var i = 0; i < message.source_shards.length; ++i) {
                    var error = $root.topodata.Shard.SourceShard.verify(message.source_shards[i]);
                    if (error) return 'source_shards.' + error;
                }
            }
            if (message.tablet_controls != null && message.hasOwnProperty('tablet_controls')) {
                if (!Array.isArray(message.tablet_controls)) return 'tablet_controls: array expected';
                for (var i = 0; i < message.tablet_controls.length; ++i) {
                    var error = $root.topodata.Shard.TabletControl.verify(message.tablet_controls[i]);
                    if (error) return 'tablet_controls.' + error;
                }
            }
            if (message.is_master_serving != null && message.hasOwnProperty('is_master_serving'))
                if (typeof message.is_master_serving !== 'boolean') return 'is_master_serving: boolean expected';
            return null;
        };

        /**
         * Creates a Shard message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof topodata.Shard
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {topodata.Shard} Shard
         */
        Shard.fromObject = function fromObject(object) {
            if (object instanceof $root.topodata.Shard) return object;
            var message = new $root.topodata.Shard();
            if (object.master_alias != null) {
                if (typeof object.master_alias !== 'object')
                    throw TypeError('.topodata.Shard.master_alias: object expected');
                message.master_alias = $root.topodata.TabletAlias.fromObject(object.master_alias);
            }
            if (object.master_term_start_time != null) {
                if (typeof object.master_term_start_time !== 'object')
                    throw TypeError('.topodata.Shard.master_term_start_time: object expected');
                message.master_term_start_time = $root.vttime.Time.fromObject(object.master_term_start_time);
            }
            if (object.key_range != null) {
                if (typeof object.key_range !== 'object') throw TypeError('.topodata.Shard.key_range: object expected');
                message.key_range = $root.topodata.KeyRange.fromObject(object.key_range);
            }
            if (object.served_types) {
                if (!Array.isArray(object.served_types))
                    throw TypeError('.topodata.Shard.served_types: array expected');
                message.served_types = [];
                for (var i = 0; i < object.served_types.length; ++i) {
                    if (typeof object.served_types[i] !== 'object')
                        throw TypeError('.topodata.Shard.served_types: object expected');
                    message.served_types[i] = $root.topodata.Shard.ServedType.fromObject(object.served_types[i]);
                }
            }
            if (object.source_shards) {
                if (!Array.isArray(object.source_shards))
                    throw TypeError('.topodata.Shard.source_shards: array expected');
                message.source_shards = [];
                for (var i = 0; i < object.source_shards.length; ++i) {
                    if (typeof object.source_shards[i] !== 'object')
                        throw TypeError('.topodata.Shard.source_shards: object expected');
                    message.source_shards[i] = $root.topodata.Shard.SourceShard.fromObject(object.source_shards[i]);
                }
            }
            if (object.tablet_controls) {
                if (!Array.isArray(object.tablet_controls))
                    throw TypeError('.topodata.Shard.tablet_controls: array expected');
                message.tablet_controls = [];
                for (var i = 0; i < object.tablet_controls.length; ++i) {
                    if (typeof object.tablet_controls[i] !== 'object')
                        throw TypeError('.topodata.Shard.tablet_controls: object expected');
                    message.tablet_controls[i] = $root.topodata.Shard.TabletControl.fromObject(
                        object.tablet_controls[i]
                    );
                }
            }
            if (object.is_master_serving != null) message.is_master_serving = Boolean(object.is_master_serving);
            return message;
        };

        /**
         * Creates a plain object from a Shard message. Also converts values to other types if specified.
         * @function toObject
         * @memberof topodata.Shard
         * @static
         * @param {topodata.Shard} message Shard
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        Shard.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.arrays || options.defaults) {
                object.served_types = [];
                object.source_shards = [];
                object.tablet_controls = [];
            }
            if (options.defaults) {
                object.master_alias = null;
                object.key_range = null;
                object.is_master_serving = false;
                object.master_term_start_time = null;
            }
            if (message.master_alias != null && message.hasOwnProperty('master_alias'))
                object.master_alias = $root.topodata.TabletAlias.toObject(message.master_alias, options);
            if (message.key_range != null && message.hasOwnProperty('key_range'))
                object.key_range = $root.topodata.KeyRange.toObject(message.key_range, options);
            if (message.served_types && message.served_types.length) {
                object.served_types = [];
                for (var j = 0; j < message.served_types.length; ++j)
                    object.served_types[j] = $root.topodata.Shard.ServedType.toObject(message.served_types[j], options);
            }
            if (message.source_shards && message.source_shards.length) {
                object.source_shards = [];
                for (var j = 0; j < message.source_shards.length; ++j)
                    object.source_shards[j] = $root.topodata.Shard.SourceShard.toObject(
                        message.source_shards[j],
                        options
                    );
            }
            if (message.tablet_controls && message.tablet_controls.length) {
                object.tablet_controls = [];
                for (var j = 0; j < message.tablet_controls.length; ++j)
                    object.tablet_controls[j] = $root.topodata.Shard.TabletControl.toObject(
                        message.tablet_controls[j],
                        options
                    );
            }
            if (message.is_master_serving != null && message.hasOwnProperty('is_master_serving'))
                object.is_master_serving = message.is_master_serving;
            if (message.master_term_start_time != null && message.hasOwnProperty('master_term_start_time'))
                object.master_term_start_time = $root.vttime.Time.toObject(message.master_term_start_time, options);
            return object;
        };

        /**
         * Converts this Shard to JSON.
         * @function toJSON
         * @memberof topodata.Shard
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        Shard.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        Shard.ServedType = (function () {
            /**
             * Properties of a ServedType.
             * @memberof topodata.Shard
             * @interface IServedType
             * @property {topodata.TabletType|null} [tablet_type] ServedType tablet_type
             * @property {Array.<string>|null} [cells] ServedType cells
             */

            /**
             * Constructs a new ServedType.
             * @memberof topodata.Shard
             * @classdesc Represents a ServedType.
             * @implements IServedType
             * @constructor
             * @param {topodata.Shard.IServedType=} [properties] Properties to set
             */
            function ServedType(properties) {
                this.cells = [];
                if (properties)
                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                        if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
            }

            /**
             * ServedType tablet_type.
             * @member {topodata.TabletType} tablet_type
             * @memberof topodata.Shard.ServedType
             * @instance
             */
            ServedType.prototype.tablet_type = 0;

            /**
             * ServedType cells.
             * @member {Array.<string>} cells
             * @memberof topodata.Shard.ServedType
             * @instance
             */
            ServedType.prototype.cells = $util.emptyArray;

            /**
             * Creates a new ServedType instance using the specified properties.
             * @function create
             * @memberof topodata.Shard.ServedType
             * @static
             * @param {topodata.Shard.IServedType=} [properties] Properties to set
             * @returns {topodata.Shard.ServedType} ServedType instance
             */
            ServedType.create = function create(properties) {
                return new ServedType(properties);
            };

            /**
             * Encodes the specified ServedType message. Does not implicitly {@link topodata.Shard.ServedType.verify|verify} messages.
             * @function encode
             * @memberof topodata.Shard.ServedType
             * @static
             * @param {topodata.Shard.IServedType} message ServedType message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            ServedType.encode = function encode(message, writer) {
                if (!writer) writer = $Writer.create();
                if (message.tablet_type != null && Object.hasOwnProperty.call(message, 'tablet_type'))
                    writer.uint32(/* id 1, wireType 0 =*/ 8).int32(message.tablet_type);
                if (message.cells != null && message.cells.length)
                    for (var i = 0; i < message.cells.length; ++i)
                        writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.cells[i]);
                return writer;
            };

            /**
             * Encodes the specified ServedType message, length delimited. Does not implicitly {@link topodata.Shard.ServedType.verify|verify} messages.
             * @function encodeDelimited
             * @memberof topodata.Shard.ServedType
             * @static
             * @param {topodata.Shard.IServedType} message ServedType message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            ServedType.encodeDelimited = function encodeDelimited(message, writer) {
                return this.encode(message, writer).ldelim();
            };

            /**
             * Decodes a ServedType message from the specified reader or buffer.
             * @function decode
             * @memberof topodata.Shard.ServedType
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {topodata.Shard.ServedType} ServedType
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            ServedType.decode = function decode(reader, length) {
                if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
                var end = length === undefined ? reader.len : reader.pos + length,
                    message = new $root.topodata.Shard.ServedType();
                while (reader.pos < end) {
                    var tag = reader.uint32();
                    switch (tag >>> 3) {
                        case 1:
                            message.tablet_type = reader.int32();
                            break;
                        case 2:
                            if (!(message.cells && message.cells.length)) message.cells = [];
                            message.cells.push(reader.string());
                            break;
                        default:
                            reader.skipType(tag & 7);
                            break;
                    }
                }
                return message;
            };

            /**
             * Decodes a ServedType message from the specified reader or buffer, length delimited.
             * @function decodeDelimited
             * @memberof topodata.Shard.ServedType
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {topodata.Shard.ServedType} ServedType
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            ServedType.decodeDelimited = function decodeDelimited(reader) {
                if (!(reader instanceof $Reader)) reader = new $Reader(reader);
                return this.decode(reader, reader.uint32());
            };

            /**
             * Verifies a ServedType message.
             * @function verify
             * @memberof topodata.Shard.ServedType
             * @static
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {string|null} `null` if valid, otherwise the reason why it is not
             */
            ServedType.verify = function verify(message) {
                if (typeof message !== 'object' || message === null) return 'object expected';
                if (message.tablet_type != null && message.hasOwnProperty('tablet_type'))
                    switch (message.tablet_type) {
                        default:
                            return 'tablet_type: enum value expected';
                        case 0:
                        case 1:
                        case 2:
                        case 3:
                        case 3:
                        case 4:
                        case 5:
                        case 6:
                        case 7:
                        case 8:
                            break;
                    }
                if (message.cells != null && message.hasOwnProperty('cells')) {
                    if (!Array.isArray(message.cells)) return 'cells: array expected';
                    for (var i = 0; i < message.cells.length; ++i)
                        if (!$util.isString(message.cells[i])) return 'cells: string[] expected';
                }
                return null;
            };

            /**
             * Creates a ServedType message from a plain object. Also converts values to their respective internal types.
             * @function fromObject
             * @memberof topodata.Shard.ServedType
             * @static
             * @param {Object.<string,*>} object Plain object
             * @returns {topodata.Shard.ServedType} ServedType
             */
            ServedType.fromObject = function fromObject(object) {
                if (object instanceof $root.topodata.Shard.ServedType) return object;
                var message = new $root.topodata.Shard.ServedType();
                switch (object.tablet_type) {
                    case 'UNKNOWN':
                    case 0:
                        message.tablet_type = 0;
                        break;
                    case 'MASTER':
                    case 1:
                        message.tablet_type = 1;
                        break;
                    case 'REPLICA':
                    case 2:
                        message.tablet_type = 2;
                        break;
                    case 'RDONLY':
                    case 3:
                        message.tablet_type = 3;
                        break;
                    case 'BATCH':
                    case 3:
                        message.tablet_type = 3;
                        break;
                    case 'SPARE':
                    case 4:
                        message.tablet_type = 4;
                        break;
                    case 'EXPERIMENTAL':
                    case 5:
                        message.tablet_type = 5;
                        break;
                    case 'BACKUP':
                    case 6:
                        message.tablet_type = 6;
                        break;
                    case 'RESTORE':
                    case 7:
                        message.tablet_type = 7;
                        break;
                    case 'DRAINED':
                    case 8:
                        message.tablet_type = 8;
                        break;
                }
                if (object.cells) {
                    if (!Array.isArray(object.cells))
                        throw TypeError('.topodata.Shard.ServedType.cells: array expected');
                    message.cells = [];
                    for (var i = 0; i < object.cells.length; ++i) message.cells[i] = String(object.cells[i]);
                }
                return message;
            };

            /**
             * Creates a plain object from a ServedType message. Also converts values to other types if specified.
             * @function toObject
             * @memberof topodata.Shard.ServedType
             * @static
             * @param {topodata.Shard.ServedType} message ServedType
             * @param {$protobuf.IConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            ServedType.toObject = function toObject(message, options) {
                if (!options) options = {};
                var object = {};
                if (options.arrays || options.defaults) object.cells = [];
                if (options.defaults) object.tablet_type = options.enums === String ? 'UNKNOWN' : 0;
                if (message.tablet_type != null && message.hasOwnProperty('tablet_type'))
                    object.tablet_type =
                        options.enums === String ? $root.topodata.TabletType[message.tablet_type] : message.tablet_type;
                if (message.cells && message.cells.length) {
                    object.cells = [];
                    for (var j = 0; j < message.cells.length; ++j) object.cells[j] = message.cells[j];
                }
                return object;
            };

            /**
             * Converts this ServedType to JSON.
             * @function toJSON
             * @memberof topodata.Shard.ServedType
             * @instance
             * @returns {Object.<string,*>} JSON object
             */
            ServedType.prototype.toJSON = function toJSON() {
                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
            };

            return ServedType;
        })();

        Shard.SourceShard = (function () {
            /**
             * Properties of a SourceShard.
             * @memberof topodata.Shard
             * @interface ISourceShard
             * @property {number|null} [uid] SourceShard uid
             * @property {string|null} [keyspace] SourceShard keyspace
             * @property {string|null} [shard] SourceShard shard
             * @property {topodata.IKeyRange|null} [key_range] SourceShard key_range
             * @property {Array.<string>|null} [tables] SourceShard tables
             */

            /**
             * Constructs a new SourceShard.
             * @memberof topodata.Shard
             * @classdesc Represents a SourceShard.
             * @implements ISourceShard
             * @constructor
             * @param {topodata.Shard.ISourceShard=} [properties] Properties to set
             */
            function SourceShard(properties) {
                this.tables = [];
                if (properties)
                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                        if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
            }

            /**
             * SourceShard uid.
             * @member {number} uid
             * @memberof topodata.Shard.SourceShard
             * @instance
             */
            SourceShard.prototype.uid = 0;

            /**
             * SourceShard keyspace.
             * @member {string} keyspace
             * @memberof topodata.Shard.SourceShard
             * @instance
             */
            SourceShard.prototype.keyspace = '';

            /**
             * SourceShard shard.
             * @member {string} shard
             * @memberof topodata.Shard.SourceShard
             * @instance
             */
            SourceShard.prototype.shard = '';

            /**
             * SourceShard key_range.
             * @member {topodata.IKeyRange|null|undefined} key_range
             * @memberof topodata.Shard.SourceShard
             * @instance
             */
            SourceShard.prototype.key_range = null;

            /**
             * SourceShard tables.
             * @member {Array.<string>} tables
             * @memberof topodata.Shard.SourceShard
             * @instance
             */
            SourceShard.prototype.tables = $util.emptyArray;

            /**
             * Creates a new SourceShard instance using the specified properties.
             * @function create
             * @memberof topodata.Shard.SourceShard
             * @static
             * @param {topodata.Shard.ISourceShard=} [properties] Properties to set
             * @returns {topodata.Shard.SourceShard} SourceShard instance
             */
            SourceShard.create = function create(properties) {
                return new SourceShard(properties);
            };

            /**
             * Encodes the specified SourceShard message. Does not implicitly {@link topodata.Shard.SourceShard.verify|verify} messages.
             * @function encode
             * @memberof topodata.Shard.SourceShard
             * @static
             * @param {topodata.Shard.ISourceShard} message SourceShard message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            SourceShard.encode = function encode(message, writer) {
                if (!writer) writer = $Writer.create();
                if (message.uid != null && Object.hasOwnProperty.call(message, 'uid'))
                    writer.uint32(/* id 1, wireType 0 =*/ 8).uint32(message.uid);
                if (message.keyspace != null && Object.hasOwnProperty.call(message, 'keyspace'))
                    writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.keyspace);
                if (message.shard != null && Object.hasOwnProperty.call(message, 'shard'))
                    writer.uint32(/* id 3, wireType 2 =*/ 26).string(message.shard);
                if (message.key_range != null && Object.hasOwnProperty.call(message, 'key_range'))
                    $root.topodata.KeyRange.encode(
                        message.key_range,
                        writer.uint32(/* id 4, wireType 2 =*/ 34).fork()
                    ).ldelim();
                if (message.tables != null && message.tables.length)
                    for (var i = 0; i < message.tables.length; ++i)
                        writer.uint32(/* id 5, wireType 2 =*/ 42).string(message.tables[i]);
                return writer;
            };

            /**
             * Encodes the specified SourceShard message, length delimited. Does not implicitly {@link topodata.Shard.SourceShard.verify|verify} messages.
             * @function encodeDelimited
             * @memberof topodata.Shard.SourceShard
             * @static
             * @param {topodata.Shard.ISourceShard} message SourceShard message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            SourceShard.encodeDelimited = function encodeDelimited(message, writer) {
                return this.encode(message, writer).ldelim();
            };

            /**
             * Decodes a SourceShard message from the specified reader or buffer.
             * @function decode
             * @memberof topodata.Shard.SourceShard
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {topodata.Shard.SourceShard} SourceShard
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            SourceShard.decode = function decode(reader, length) {
                if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
                var end = length === undefined ? reader.len : reader.pos + length,
                    message = new $root.topodata.Shard.SourceShard();
                while (reader.pos < end) {
                    var tag = reader.uint32();
                    switch (tag >>> 3) {
                        case 1:
                            message.uid = reader.uint32();
                            break;
                        case 2:
                            message.keyspace = reader.string();
                            break;
                        case 3:
                            message.shard = reader.string();
                            break;
                        case 4:
                            message.key_range = $root.topodata.KeyRange.decode(reader, reader.uint32());
                            break;
                        case 5:
                            if (!(message.tables && message.tables.length)) message.tables = [];
                            message.tables.push(reader.string());
                            break;
                        default:
                            reader.skipType(tag & 7);
                            break;
                    }
                }
                return message;
            };

            /**
             * Decodes a SourceShard message from the specified reader or buffer, length delimited.
             * @function decodeDelimited
             * @memberof topodata.Shard.SourceShard
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {topodata.Shard.SourceShard} SourceShard
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            SourceShard.decodeDelimited = function decodeDelimited(reader) {
                if (!(reader instanceof $Reader)) reader = new $Reader(reader);
                return this.decode(reader, reader.uint32());
            };

            /**
             * Verifies a SourceShard message.
             * @function verify
             * @memberof topodata.Shard.SourceShard
             * @static
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {string|null} `null` if valid, otherwise the reason why it is not
             */
            SourceShard.verify = function verify(message) {
                if (typeof message !== 'object' || message === null) return 'object expected';
                if (message.uid != null && message.hasOwnProperty('uid'))
                    if (!$util.isInteger(message.uid)) return 'uid: integer expected';
                if (message.keyspace != null && message.hasOwnProperty('keyspace'))
                    if (!$util.isString(message.keyspace)) return 'keyspace: string expected';
                if (message.shard != null && message.hasOwnProperty('shard'))
                    if (!$util.isString(message.shard)) return 'shard: string expected';
                if (message.key_range != null && message.hasOwnProperty('key_range')) {
                    var error = $root.topodata.KeyRange.verify(message.key_range);
                    if (error) return 'key_range.' + error;
                }
                if (message.tables != null && message.hasOwnProperty('tables')) {
                    if (!Array.isArray(message.tables)) return 'tables: array expected';
                    for (var i = 0; i < message.tables.length; ++i)
                        if (!$util.isString(message.tables[i])) return 'tables: string[] expected';
                }
                return null;
            };

            /**
             * Creates a SourceShard message from a plain object. Also converts values to their respective internal types.
             * @function fromObject
             * @memberof topodata.Shard.SourceShard
             * @static
             * @param {Object.<string,*>} object Plain object
             * @returns {topodata.Shard.SourceShard} SourceShard
             */
            SourceShard.fromObject = function fromObject(object) {
                if (object instanceof $root.topodata.Shard.SourceShard) return object;
                var message = new $root.topodata.Shard.SourceShard();
                if (object.uid != null) message.uid = object.uid >>> 0;
                if (object.keyspace != null) message.keyspace = String(object.keyspace);
                if (object.shard != null) message.shard = String(object.shard);
                if (object.key_range != null) {
                    if (typeof object.key_range !== 'object')
                        throw TypeError('.topodata.Shard.SourceShard.key_range: object expected');
                    message.key_range = $root.topodata.KeyRange.fromObject(object.key_range);
                }
                if (object.tables) {
                    if (!Array.isArray(object.tables))
                        throw TypeError('.topodata.Shard.SourceShard.tables: array expected');
                    message.tables = [];
                    for (var i = 0; i < object.tables.length; ++i) message.tables[i] = String(object.tables[i]);
                }
                return message;
            };

            /**
             * Creates a plain object from a SourceShard message. Also converts values to other types if specified.
             * @function toObject
             * @memberof topodata.Shard.SourceShard
             * @static
             * @param {topodata.Shard.SourceShard} message SourceShard
             * @param {$protobuf.IConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            SourceShard.toObject = function toObject(message, options) {
                if (!options) options = {};
                var object = {};
                if (options.arrays || options.defaults) object.tables = [];
                if (options.defaults) {
                    object.uid = 0;
                    object.keyspace = '';
                    object.shard = '';
                    object.key_range = null;
                }
                if (message.uid != null && message.hasOwnProperty('uid')) object.uid = message.uid;
                if (message.keyspace != null && message.hasOwnProperty('keyspace')) object.keyspace = message.keyspace;
                if (message.shard != null && message.hasOwnProperty('shard')) object.shard = message.shard;
                if (message.key_range != null && message.hasOwnProperty('key_range'))
                    object.key_range = $root.topodata.KeyRange.toObject(message.key_range, options);
                if (message.tables && message.tables.length) {
                    object.tables = [];
                    for (var j = 0; j < message.tables.length; ++j) object.tables[j] = message.tables[j];
                }
                return object;
            };

            /**
             * Converts this SourceShard to JSON.
             * @function toJSON
             * @memberof topodata.Shard.SourceShard
             * @instance
             * @returns {Object.<string,*>} JSON object
             */
            SourceShard.prototype.toJSON = function toJSON() {
                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
            };

            return SourceShard;
        })();

        Shard.TabletControl = (function () {
            /**
             * Properties of a TabletControl.
             * @memberof topodata.Shard
             * @interface ITabletControl
             * @property {topodata.TabletType|null} [tablet_type] TabletControl tablet_type
             * @property {Array.<string>|null} [cells] TabletControl cells
             * @property {Array.<string>|null} [blacklisted_tables] TabletControl blacklisted_tables
             * @property {boolean|null} [frozen] TabletControl frozen
             */

            /**
             * Constructs a new TabletControl.
             * @memberof topodata.Shard
             * @classdesc Represents a TabletControl.
             * @implements ITabletControl
             * @constructor
             * @param {topodata.Shard.ITabletControl=} [properties] Properties to set
             */
            function TabletControl(properties) {
                this.cells = [];
                this.blacklisted_tables = [];
                if (properties)
                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                        if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
            }

            /**
             * TabletControl tablet_type.
             * @member {topodata.TabletType} tablet_type
             * @memberof topodata.Shard.TabletControl
             * @instance
             */
            TabletControl.prototype.tablet_type = 0;

            /**
             * TabletControl cells.
             * @member {Array.<string>} cells
             * @memberof topodata.Shard.TabletControl
             * @instance
             */
            TabletControl.prototype.cells = $util.emptyArray;

            /**
             * TabletControl blacklisted_tables.
             * @member {Array.<string>} blacklisted_tables
             * @memberof topodata.Shard.TabletControl
             * @instance
             */
            TabletControl.prototype.blacklisted_tables = $util.emptyArray;

            /**
             * TabletControl frozen.
             * @member {boolean} frozen
             * @memberof topodata.Shard.TabletControl
             * @instance
             */
            TabletControl.prototype.frozen = false;

            /**
             * Creates a new TabletControl instance using the specified properties.
             * @function create
             * @memberof topodata.Shard.TabletControl
             * @static
             * @param {topodata.Shard.ITabletControl=} [properties] Properties to set
             * @returns {topodata.Shard.TabletControl} TabletControl instance
             */
            TabletControl.create = function create(properties) {
                return new TabletControl(properties);
            };

            /**
             * Encodes the specified TabletControl message. Does not implicitly {@link topodata.Shard.TabletControl.verify|verify} messages.
             * @function encode
             * @memberof topodata.Shard.TabletControl
             * @static
             * @param {topodata.Shard.ITabletControl} message TabletControl message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            TabletControl.encode = function encode(message, writer) {
                if (!writer) writer = $Writer.create();
                if (message.tablet_type != null && Object.hasOwnProperty.call(message, 'tablet_type'))
                    writer.uint32(/* id 1, wireType 0 =*/ 8).int32(message.tablet_type);
                if (message.cells != null && message.cells.length)
                    for (var i = 0; i < message.cells.length; ++i)
                        writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.cells[i]);
                if (message.blacklisted_tables != null && message.blacklisted_tables.length)
                    for (var i = 0; i < message.blacklisted_tables.length; ++i)
                        writer.uint32(/* id 4, wireType 2 =*/ 34).string(message.blacklisted_tables[i]);
                if (message.frozen != null && Object.hasOwnProperty.call(message, 'frozen'))
                    writer.uint32(/* id 5, wireType 0 =*/ 40).bool(message.frozen);
                return writer;
            };

            /**
             * Encodes the specified TabletControl message, length delimited. Does not implicitly {@link topodata.Shard.TabletControl.verify|verify} messages.
             * @function encodeDelimited
             * @memberof topodata.Shard.TabletControl
             * @static
             * @param {topodata.Shard.ITabletControl} message TabletControl message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            TabletControl.encodeDelimited = function encodeDelimited(message, writer) {
                return this.encode(message, writer).ldelim();
            };

            /**
             * Decodes a TabletControl message from the specified reader or buffer.
             * @function decode
             * @memberof topodata.Shard.TabletControl
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {topodata.Shard.TabletControl} TabletControl
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            TabletControl.decode = function decode(reader, length) {
                if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
                var end = length === undefined ? reader.len : reader.pos + length,
                    message = new $root.topodata.Shard.TabletControl();
                while (reader.pos < end) {
                    var tag = reader.uint32();
                    switch (tag >>> 3) {
                        case 1:
                            message.tablet_type = reader.int32();
                            break;
                        case 2:
                            if (!(message.cells && message.cells.length)) message.cells = [];
                            message.cells.push(reader.string());
                            break;
                        case 4:
                            if (!(message.blacklisted_tables && message.blacklisted_tables.length))
                                message.blacklisted_tables = [];
                            message.blacklisted_tables.push(reader.string());
                            break;
                        case 5:
                            message.frozen = reader.bool();
                            break;
                        default:
                            reader.skipType(tag & 7);
                            break;
                    }
                }
                return message;
            };

            /**
             * Decodes a TabletControl message from the specified reader or buffer, length delimited.
             * @function decodeDelimited
             * @memberof topodata.Shard.TabletControl
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {topodata.Shard.TabletControl} TabletControl
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            TabletControl.decodeDelimited = function decodeDelimited(reader) {
                if (!(reader instanceof $Reader)) reader = new $Reader(reader);
                return this.decode(reader, reader.uint32());
            };

            /**
             * Verifies a TabletControl message.
             * @function verify
             * @memberof topodata.Shard.TabletControl
             * @static
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {string|null} `null` if valid, otherwise the reason why it is not
             */
            TabletControl.verify = function verify(message) {
                if (typeof message !== 'object' || message === null) return 'object expected';
                if (message.tablet_type != null && message.hasOwnProperty('tablet_type'))
                    switch (message.tablet_type) {
                        default:
                            return 'tablet_type: enum value expected';
                        case 0:
                        case 1:
                        case 2:
                        case 3:
                        case 3:
                        case 4:
                        case 5:
                        case 6:
                        case 7:
                        case 8:
                            break;
                    }
                if (message.cells != null && message.hasOwnProperty('cells')) {
                    if (!Array.isArray(message.cells)) return 'cells: array expected';
                    for (var i = 0; i < message.cells.length; ++i)
                        if (!$util.isString(message.cells[i])) return 'cells: string[] expected';
                }
                if (message.blacklisted_tables != null && message.hasOwnProperty('blacklisted_tables')) {
                    if (!Array.isArray(message.blacklisted_tables)) return 'blacklisted_tables: array expected';
                    for (var i = 0; i < message.blacklisted_tables.length; ++i)
                        if (!$util.isString(message.blacklisted_tables[i]))
                            return 'blacklisted_tables: string[] expected';
                }
                if (message.frozen != null && message.hasOwnProperty('frozen'))
                    if (typeof message.frozen !== 'boolean') return 'frozen: boolean expected';
                return null;
            };

            /**
             * Creates a TabletControl message from a plain object. Also converts values to their respective internal types.
             * @function fromObject
             * @memberof topodata.Shard.TabletControl
             * @static
             * @param {Object.<string,*>} object Plain object
             * @returns {topodata.Shard.TabletControl} TabletControl
             */
            TabletControl.fromObject = function fromObject(object) {
                if (object instanceof $root.topodata.Shard.TabletControl) return object;
                var message = new $root.topodata.Shard.TabletControl();
                switch (object.tablet_type) {
                    case 'UNKNOWN':
                    case 0:
                        message.tablet_type = 0;
                        break;
                    case 'MASTER':
                    case 1:
                        message.tablet_type = 1;
                        break;
                    case 'REPLICA':
                    case 2:
                        message.tablet_type = 2;
                        break;
                    case 'RDONLY':
                    case 3:
                        message.tablet_type = 3;
                        break;
                    case 'BATCH':
                    case 3:
                        message.tablet_type = 3;
                        break;
                    case 'SPARE':
                    case 4:
                        message.tablet_type = 4;
                        break;
                    case 'EXPERIMENTAL':
                    case 5:
                        message.tablet_type = 5;
                        break;
                    case 'BACKUP':
                    case 6:
                        message.tablet_type = 6;
                        break;
                    case 'RESTORE':
                    case 7:
                        message.tablet_type = 7;
                        break;
                    case 'DRAINED':
                    case 8:
                        message.tablet_type = 8;
                        break;
                }
                if (object.cells) {
                    if (!Array.isArray(object.cells))
                        throw TypeError('.topodata.Shard.TabletControl.cells: array expected');
                    message.cells = [];
                    for (var i = 0; i < object.cells.length; ++i) message.cells[i] = String(object.cells[i]);
                }
                if (object.blacklisted_tables) {
                    if (!Array.isArray(object.blacklisted_tables))
                        throw TypeError('.topodata.Shard.TabletControl.blacklisted_tables: array expected');
                    message.blacklisted_tables = [];
                    for (var i = 0; i < object.blacklisted_tables.length; ++i)
                        message.blacklisted_tables[i] = String(object.blacklisted_tables[i]);
                }
                if (object.frozen != null) message.frozen = Boolean(object.frozen);
                return message;
            };

            /**
             * Creates a plain object from a TabletControl message. Also converts values to other types if specified.
             * @function toObject
             * @memberof topodata.Shard.TabletControl
             * @static
             * @param {topodata.Shard.TabletControl} message TabletControl
             * @param {$protobuf.IConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            TabletControl.toObject = function toObject(message, options) {
                if (!options) options = {};
                var object = {};
                if (options.arrays || options.defaults) {
                    object.cells = [];
                    object.blacklisted_tables = [];
                }
                if (options.defaults) {
                    object.tablet_type = options.enums === String ? 'UNKNOWN' : 0;
                    object.frozen = false;
                }
                if (message.tablet_type != null && message.hasOwnProperty('tablet_type'))
                    object.tablet_type =
                        options.enums === String ? $root.topodata.TabletType[message.tablet_type] : message.tablet_type;
                if (message.cells && message.cells.length) {
                    object.cells = [];
                    for (var j = 0; j < message.cells.length; ++j) object.cells[j] = message.cells[j];
                }
                if (message.blacklisted_tables && message.blacklisted_tables.length) {
                    object.blacklisted_tables = [];
                    for (var j = 0; j < message.blacklisted_tables.length; ++j)
                        object.blacklisted_tables[j] = message.blacklisted_tables[j];
                }
                if (message.frozen != null && message.hasOwnProperty('frozen')) object.frozen = message.frozen;
                return object;
            };

            /**
             * Converts this TabletControl to JSON.
             * @function toJSON
             * @memberof topodata.Shard.TabletControl
             * @instance
             * @returns {Object.<string,*>} JSON object
             */
            TabletControl.prototype.toJSON = function toJSON() {
                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
            };

            return TabletControl;
        })();

        return Shard;
    })();

    topodata.Keyspace = (function () {
        /**
         * Properties of a Keyspace.
         * @memberof topodata
         * @interface IKeyspace
         * @property {string|null} [sharding_column_name] Keyspace sharding_column_name
         * @property {topodata.KeyspaceIdType|null} [sharding_column_type] Keyspace sharding_column_type
         * @property {Array.<topodata.Keyspace.IServedFrom>|null} [served_froms] Keyspace served_froms
         * @property {topodata.KeyspaceType|null} [keyspace_type] Keyspace keyspace_type
         * @property {string|null} [base_keyspace] Keyspace base_keyspace
         * @property {vttime.ITime|null} [snapshot_time] Keyspace snapshot_time
         */

        /**
         * Constructs a new Keyspace.
         * @memberof topodata
         * @classdesc Represents a Keyspace.
         * @implements IKeyspace
         * @constructor
         * @param {topodata.IKeyspace=} [properties] Properties to set
         */
        function Keyspace(properties) {
            this.served_froms = [];
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * Keyspace sharding_column_name.
         * @member {string} sharding_column_name
         * @memberof topodata.Keyspace
         * @instance
         */
        Keyspace.prototype.sharding_column_name = '';

        /**
         * Keyspace sharding_column_type.
         * @member {topodata.KeyspaceIdType} sharding_column_type
         * @memberof topodata.Keyspace
         * @instance
         */
        Keyspace.prototype.sharding_column_type = 0;

        /**
         * Keyspace served_froms.
         * @member {Array.<topodata.Keyspace.IServedFrom>} served_froms
         * @memberof topodata.Keyspace
         * @instance
         */
        Keyspace.prototype.served_froms = $util.emptyArray;

        /**
         * Keyspace keyspace_type.
         * @member {topodata.KeyspaceType} keyspace_type
         * @memberof topodata.Keyspace
         * @instance
         */
        Keyspace.prototype.keyspace_type = 0;

        /**
         * Keyspace base_keyspace.
         * @member {string} base_keyspace
         * @memberof topodata.Keyspace
         * @instance
         */
        Keyspace.prototype.base_keyspace = '';

        /**
         * Keyspace snapshot_time.
         * @member {vttime.ITime|null|undefined} snapshot_time
         * @memberof topodata.Keyspace
         * @instance
         */
        Keyspace.prototype.snapshot_time = null;

        /**
         * Creates a new Keyspace instance using the specified properties.
         * @function create
         * @memberof topodata.Keyspace
         * @static
         * @param {topodata.IKeyspace=} [properties] Properties to set
         * @returns {topodata.Keyspace} Keyspace instance
         */
        Keyspace.create = function create(properties) {
            return new Keyspace(properties);
        };

        /**
         * Encodes the specified Keyspace message. Does not implicitly {@link topodata.Keyspace.verify|verify} messages.
         * @function encode
         * @memberof topodata.Keyspace
         * @static
         * @param {topodata.IKeyspace} message Keyspace message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        Keyspace.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.sharding_column_name != null && Object.hasOwnProperty.call(message, 'sharding_column_name'))
                writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.sharding_column_name);
            if (message.sharding_column_type != null && Object.hasOwnProperty.call(message, 'sharding_column_type'))
                writer.uint32(/* id 2, wireType 0 =*/ 16).int32(message.sharding_column_type);
            if (message.served_froms != null && message.served_froms.length)
                for (var i = 0; i < message.served_froms.length; ++i)
                    $root.topodata.Keyspace.ServedFrom.encode(
                        message.served_froms[i],
                        writer.uint32(/* id 4, wireType 2 =*/ 34).fork()
                    ).ldelim();
            if (message.keyspace_type != null && Object.hasOwnProperty.call(message, 'keyspace_type'))
                writer.uint32(/* id 5, wireType 0 =*/ 40).int32(message.keyspace_type);
            if (message.base_keyspace != null && Object.hasOwnProperty.call(message, 'base_keyspace'))
                writer.uint32(/* id 6, wireType 2 =*/ 50).string(message.base_keyspace);
            if (message.snapshot_time != null && Object.hasOwnProperty.call(message, 'snapshot_time'))
                $root.vttime.Time.encode(
                    message.snapshot_time,
                    writer.uint32(/* id 7, wireType 2 =*/ 58).fork()
                ).ldelim();
            return writer;
        };

        /**
         * Encodes the specified Keyspace message, length delimited. Does not implicitly {@link topodata.Keyspace.verify|verify} messages.
         * @function encodeDelimited
         * @memberof topodata.Keyspace
         * @static
         * @param {topodata.IKeyspace} message Keyspace message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        Keyspace.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a Keyspace message from the specified reader or buffer.
         * @function decode
         * @memberof topodata.Keyspace
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {topodata.Keyspace} Keyspace
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        Keyspace.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.topodata.Keyspace();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        message.sharding_column_name = reader.string();
                        break;
                    case 2:
                        message.sharding_column_type = reader.int32();
                        break;
                    case 4:
                        if (!(message.served_froms && message.served_froms.length)) message.served_froms = [];
                        message.served_froms.push($root.topodata.Keyspace.ServedFrom.decode(reader, reader.uint32()));
                        break;
                    case 5:
                        message.keyspace_type = reader.int32();
                        break;
                    case 6:
                        message.base_keyspace = reader.string();
                        break;
                    case 7:
                        message.snapshot_time = $root.vttime.Time.decode(reader, reader.uint32());
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a Keyspace message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof topodata.Keyspace
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {topodata.Keyspace} Keyspace
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        Keyspace.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a Keyspace message.
         * @function verify
         * @memberof topodata.Keyspace
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        Keyspace.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.sharding_column_name != null && message.hasOwnProperty('sharding_column_name'))
                if (!$util.isString(message.sharding_column_name)) return 'sharding_column_name: string expected';
            if (message.sharding_column_type != null && message.hasOwnProperty('sharding_column_type'))
                switch (message.sharding_column_type) {
                    default:
                        return 'sharding_column_type: enum value expected';
                    case 0:
                    case 1:
                    case 2:
                        break;
                }
            if (message.served_froms != null && message.hasOwnProperty('served_froms')) {
                if (!Array.isArray(message.served_froms)) return 'served_froms: array expected';
                for (var i = 0; i < message.served_froms.length; ++i) {
                    var error = $root.topodata.Keyspace.ServedFrom.verify(message.served_froms[i]);
                    if (error) return 'served_froms.' + error;
                }
            }
            if (message.keyspace_type != null && message.hasOwnProperty('keyspace_type'))
                switch (message.keyspace_type) {
                    default:
                        return 'keyspace_type: enum value expected';
                    case 0:
                    case 1:
                        break;
                }
            if (message.base_keyspace != null && message.hasOwnProperty('base_keyspace'))
                if (!$util.isString(message.base_keyspace)) return 'base_keyspace: string expected';
            if (message.snapshot_time != null && message.hasOwnProperty('snapshot_time')) {
                var error = $root.vttime.Time.verify(message.snapshot_time);
                if (error) return 'snapshot_time.' + error;
            }
            return null;
        };

        /**
         * Creates a Keyspace message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof topodata.Keyspace
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {topodata.Keyspace} Keyspace
         */
        Keyspace.fromObject = function fromObject(object) {
            if (object instanceof $root.topodata.Keyspace) return object;
            var message = new $root.topodata.Keyspace();
            if (object.sharding_column_name != null) message.sharding_column_name = String(object.sharding_column_name);
            switch (object.sharding_column_type) {
                case 'UNSET':
                case 0:
                    message.sharding_column_type = 0;
                    break;
                case 'UINT64':
                case 1:
                    message.sharding_column_type = 1;
                    break;
                case 'BYTES':
                case 2:
                    message.sharding_column_type = 2;
                    break;
            }
            if (object.served_froms) {
                if (!Array.isArray(object.served_froms))
                    throw TypeError('.topodata.Keyspace.served_froms: array expected');
                message.served_froms = [];
                for (var i = 0; i < object.served_froms.length; ++i) {
                    if (typeof object.served_froms[i] !== 'object')
                        throw TypeError('.topodata.Keyspace.served_froms: object expected');
                    message.served_froms[i] = $root.topodata.Keyspace.ServedFrom.fromObject(object.served_froms[i]);
                }
            }
            switch (object.keyspace_type) {
                case 'NORMAL':
                case 0:
                    message.keyspace_type = 0;
                    break;
                case 'SNAPSHOT':
                case 1:
                    message.keyspace_type = 1;
                    break;
            }
            if (object.base_keyspace != null) message.base_keyspace = String(object.base_keyspace);
            if (object.snapshot_time != null) {
                if (typeof object.snapshot_time !== 'object')
                    throw TypeError('.topodata.Keyspace.snapshot_time: object expected');
                message.snapshot_time = $root.vttime.Time.fromObject(object.snapshot_time);
            }
            return message;
        };

        /**
         * Creates a plain object from a Keyspace message. Also converts values to other types if specified.
         * @function toObject
         * @memberof topodata.Keyspace
         * @static
         * @param {topodata.Keyspace} message Keyspace
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        Keyspace.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.arrays || options.defaults) object.served_froms = [];
            if (options.defaults) {
                object.sharding_column_name = '';
                object.sharding_column_type = options.enums === String ? 'UNSET' : 0;
                object.keyspace_type = options.enums === String ? 'NORMAL' : 0;
                object.base_keyspace = '';
                object.snapshot_time = null;
            }
            if (message.sharding_column_name != null && message.hasOwnProperty('sharding_column_name'))
                object.sharding_column_name = message.sharding_column_name;
            if (message.sharding_column_type != null && message.hasOwnProperty('sharding_column_type'))
                object.sharding_column_type =
                    options.enums === String
                        ? $root.topodata.KeyspaceIdType[message.sharding_column_type]
                        : message.sharding_column_type;
            if (message.served_froms && message.served_froms.length) {
                object.served_froms = [];
                for (var j = 0; j < message.served_froms.length; ++j)
                    object.served_froms[j] = $root.topodata.Keyspace.ServedFrom.toObject(
                        message.served_froms[j],
                        options
                    );
            }
            if (message.keyspace_type != null && message.hasOwnProperty('keyspace_type'))
                object.keyspace_type =
                    options.enums === String
                        ? $root.topodata.KeyspaceType[message.keyspace_type]
                        : message.keyspace_type;
            if (message.base_keyspace != null && message.hasOwnProperty('base_keyspace'))
                object.base_keyspace = message.base_keyspace;
            if (message.snapshot_time != null && message.hasOwnProperty('snapshot_time'))
                object.snapshot_time = $root.vttime.Time.toObject(message.snapshot_time, options);
            return object;
        };

        /**
         * Converts this Keyspace to JSON.
         * @function toJSON
         * @memberof topodata.Keyspace
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        Keyspace.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        Keyspace.ServedFrom = (function () {
            /**
             * Properties of a ServedFrom.
             * @memberof topodata.Keyspace
             * @interface IServedFrom
             * @property {topodata.TabletType|null} [tablet_type] ServedFrom tablet_type
             * @property {Array.<string>|null} [cells] ServedFrom cells
             * @property {string|null} [keyspace] ServedFrom keyspace
             */

            /**
             * Constructs a new ServedFrom.
             * @memberof topodata.Keyspace
             * @classdesc Represents a ServedFrom.
             * @implements IServedFrom
             * @constructor
             * @param {topodata.Keyspace.IServedFrom=} [properties] Properties to set
             */
            function ServedFrom(properties) {
                this.cells = [];
                if (properties)
                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                        if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
            }

            /**
             * ServedFrom tablet_type.
             * @member {topodata.TabletType} tablet_type
             * @memberof topodata.Keyspace.ServedFrom
             * @instance
             */
            ServedFrom.prototype.tablet_type = 0;

            /**
             * ServedFrom cells.
             * @member {Array.<string>} cells
             * @memberof topodata.Keyspace.ServedFrom
             * @instance
             */
            ServedFrom.prototype.cells = $util.emptyArray;

            /**
             * ServedFrom keyspace.
             * @member {string} keyspace
             * @memberof topodata.Keyspace.ServedFrom
             * @instance
             */
            ServedFrom.prototype.keyspace = '';

            /**
             * Creates a new ServedFrom instance using the specified properties.
             * @function create
             * @memberof topodata.Keyspace.ServedFrom
             * @static
             * @param {topodata.Keyspace.IServedFrom=} [properties] Properties to set
             * @returns {topodata.Keyspace.ServedFrom} ServedFrom instance
             */
            ServedFrom.create = function create(properties) {
                return new ServedFrom(properties);
            };

            /**
             * Encodes the specified ServedFrom message. Does not implicitly {@link topodata.Keyspace.ServedFrom.verify|verify} messages.
             * @function encode
             * @memberof topodata.Keyspace.ServedFrom
             * @static
             * @param {topodata.Keyspace.IServedFrom} message ServedFrom message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            ServedFrom.encode = function encode(message, writer) {
                if (!writer) writer = $Writer.create();
                if (message.tablet_type != null && Object.hasOwnProperty.call(message, 'tablet_type'))
                    writer.uint32(/* id 1, wireType 0 =*/ 8).int32(message.tablet_type);
                if (message.cells != null && message.cells.length)
                    for (var i = 0; i < message.cells.length; ++i)
                        writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.cells[i]);
                if (message.keyspace != null && Object.hasOwnProperty.call(message, 'keyspace'))
                    writer.uint32(/* id 3, wireType 2 =*/ 26).string(message.keyspace);
                return writer;
            };

            /**
             * Encodes the specified ServedFrom message, length delimited. Does not implicitly {@link topodata.Keyspace.ServedFrom.verify|verify} messages.
             * @function encodeDelimited
             * @memberof topodata.Keyspace.ServedFrom
             * @static
             * @param {topodata.Keyspace.IServedFrom} message ServedFrom message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            ServedFrom.encodeDelimited = function encodeDelimited(message, writer) {
                return this.encode(message, writer).ldelim();
            };

            /**
             * Decodes a ServedFrom message from the specified reader or buffer.
             * @function decode
             * @memberof topodata.Keyspace.ServedFrom
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {topodata.Keyspace.ServedFrom} ServedFrom
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            ServedFrom.decode = function decode(reader, length) {
                if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
                var end = length === undefined ? reader.len : reader.pos + length,
                    message = new $root.topodata.Keyspace.ServedFrom();
                while (reader.pos < end) {
                    var tag = reader.uint32();
                    switch (tag >>> 3) {
                        case 1:
                            message.tablet_type = reader.int32();
                            break;
                        case 2:
                            if (!(message.cells && message.cells.length)) message.cells = [];
                            message.cells.push(reader.string());
                            break;
                        case 3:
                            message.keyspace = reader.string();
                            break;
                        default:
                            reader.skipType(tag & 7);
                            break;
                    }
                }
                return message;
            };

            /**
             * Decodes a ServedFrom message from the specified reader or buffer, length delimited.
             * @function decodeDelimited
             * @memberof topodata.Keyspace.ServedFrom
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {topodata.Keyspace.ServedFrom} ServedFrom
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            ServedFrom.decodeDelimited = function decodeDelimited(reader) {
                if (!(reader instanceof $Reader)) reader = new $Reader(reader);
                return this.decode(reader, reader.uint32());
            };

            /**
             * Verifies a ServedFrom message.
             * @function verify
             * @memberof topodata.Keyspace.ServedFrom
             * @static
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {string|null} `null` if valid, otherwise the reason why it is not
             */
            ServedFrom.verify = function verify(message) {
                if (typeof message !== 'object' || message === null) return 'object expected';
                if (message.tablet_type != null && message.hasOwnProperty('tablet_type'))
                    switch (message.tablet_type) {
                        default:
                            return 'tablet_type: enum value expected';
                        case 0:
                        case 1:
                        case 2:
                        case 3:
                        case 3:
                        case 4:
                        case 5:
                        case 6:
                        case 7:
                        case 8:
                            break;
                    }
                if (message.cells != null && message.hasOwnProperty('cells')) {
                    if (!Array.isArray(message.cells)) return 'cells: array expected';
                    for (var i = 0; i < message.cells.length; ++i)
                        if (!$util.isString(message.cells[i])) return 'cells: string[] expected';
                }
                if (message.keyspace != null && message.hasOwnProperty('keyspace'))
                    if (!$util.isString(message.keyspace)) return 'keyspace: string expected';
                return null;
            };

            /**
             * Creates a ServedFrom message from a plain object. Also converts values to their respective internal types.
             * @function fromObject
             * @memberof topodata.Keyspace.ServedFrom
             * @static
             * @param {Object.<string,*>} object Plain object
             * @returns {topodata.Keyspace.ServedFrom} ServedFrom
             */
            ServedFrom.fromObject = function fromObject(object) {
                if (object instanceof $root.topodata.Keyspace.ServedFrom) return object;
                var message = new $root.topodata.Keyspace.ServedFrom();
                switch (object.tablet_type) {
                    case 'UNKNOWN':
                    case 0:
                        message.tablet_type = 0;
                        break;
                    case 'MASTER':
                    case 1:
                        message.tablet_type = 1;
                        break;
                    case 'REPLICA':
                    case 2:
                        message.tablet_type = 2;
                        break;
                    case 'RDONLY':
                    case 3:
                        message.tablet_type = 3;
                        break;
                    case 'BATCH':
                    case 3:
                        message.tablet_type = 3;
                        break;
                    case 'SPARE':
                    case 4:
                        message.tablet_type = 4;
                        break;
                    case 'EXPERIMENTAL':
                    case 5:
                        message.tablet_type = 5;
                        break;
                    case 'BACKUP':
                    case 6:
                        message.tablet_type = 6;
                        break;
                    case 'RESTORE':
                    case 7:
                        message.tablet_type = 7;
                        break;
                    case 'DRAINED':
                    case 8:
                        message.tablet_type = 8;
                        break;
                }
                if (object.cells) {
                    if (!Array.isArray(object.cells))
                        throw TypeError('.topodata.Keyspace.ServedFrom.cells: array expected');
                    message.cells = [];
                    for (var i = 0; i < object.cells.length; ++i) message.cells[i] = String(object.cells[i]);
                }
                if (object.keyspace != null) message.keyspace = String(object.keyspace);
                return message;
            };

            /**
             * Creates a plain object from a ServedFrom message. Also converts values to other types if specified.
             * @function toObject
             * @memberof topodata.Keyspace.ServedFrom
             * @static
             * @param {topodata.Keyspace.ServedFrom} message ServedFrom
             * @param {$protobuf.IConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            ServedFrom.toObject = function toObject(message, options) {
                if (!options) options = {};
                var object = {};
                if (options.arrays || options.defaults) object.cells = [];
                if (options.defaults) {
                    object.tablet_type = options.enums === String ? 'UNKNOWN' : 0;
                    object.keyspace = '';
                }
                if (message.tablet_type != null && message.hasOwnProperty('tablet_type'))
                    object.tablet_type =
                        options.enums === String ? $root.topodata.TabletType[message.tablet_type] : message.tablet_type;
                if (message.cells && message.cells.length) {
                    object.cells = [];
                    for (var j = 0; j < message.cells.length; ++j) object.cells[j] = message.cells[j];
                }
                if (message.keyspace != null && message.hasOwnProperty('keyspace')) object.keyspace = message.keyspace;
                return object;
            };

            /**
             * Converts this ServedFrom to JSON.
             * @function toJSON
             * @memberof topodata.Keyspace.ServedFrom
             * @instance
             * @returns {Object.<string,*>} JSON object
             */
            ServedFrom.prototype.toJSON = function toJSON() {
                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
            };

            return ServedFrom;
        })();

        return Keyspace;
    })();

    topodata.ShardReplication = (function () {
        /**
         * Properties of a ShardReplication.
         * @memberof topodata
         * @interface IShardReplication
         * @property {Array.<topodata.ShardReplication.INode>|null} [nodes] ShardReplication nodes
         */

        /**
         * Constructs a new ShardReplication.
         * @memberof topodata
         * @classdesc Represents a ShardReplication.
         * @implements IShardReplication
         * @constructor
         * @param {topodata.IShardReplication=} [properties] Properties to set
         */
        function ShardReplication(properties) {
            this.nodes = [];
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * ShardReplication nodes.
         * @member {Array.<topodata.ShardReplication.INode>} nodes
         * @memberof topodata.ShardReplication
         * @instance
         */
        ShardReplication.prototype.nodes = $util.emptyArray;

        /**
         * Creates a new ShardReplication instance using the specified properties.
         * @function create
         * @memberof topodata.ShardReplication
         * @static
         * @param {topodata.IShardReplication=} [properties] Properties to set
         * @returns {topodata.ShardReplication} ShardReplication instance
         */
        ShardReplication.create = function create(properties) {
            return new ShardReplication(properties);
        };

        /**
         * Encodes the specified ShardReplication message. Does not implicitly {@link topodata.ShardReplication.verify|verify} messages.
         * @function encode
         * @memberof topodata.ShardReplication
         * @static
         * @param {topodata.IShardReplication} message ShardReplication message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ShardReplication.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.nodes != null && message.nodes.length)
                for (var i = 0; i < message.nodes.length; ++i)
                    $root.topodata.ShardReplication.Node.encode(
                        message.nodes[i],
                        writer.uint32(/* id 1, wireType 2 =*/ 10).fork()
                    ).ldelim();
            return writer;
        };

        /**
         * Encodes the specified ShardReplication message, length delimited. Does not implicitly {@link topodata.ShardReplication.verify|verify} messages.
         * @function encodeDelimited
         * @memberof topodata.ShardReplication
         * @static
         * @param {topodata.IShardReplication} message ShardReplication message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ShardReplication.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a ShardReplication message from the specified reader or buffer.
         * @function decode
         * @memberof topodata.ShardReplication
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {topodata.ShardReplication} ShardReplication
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ShardReplication.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.topodata.ShardReplication();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        if (!(message.nodes && message.nodes.length)) message.nodes = [];
                        message.nodes.push($root.topodata.ShardReplication.Node.decode(reader, reader.uint32()));
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a ShardReplication message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof topodata.ShardReplication
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {topodata.ShardReplication} ShardReplication
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ShardReplication.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a ShardReplication message.
         * @function verify
         * @memberof topodata.ShardReplication
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        ShardReplication.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.nodes != null && message.hasOwnProperty('nodes')) {
                if (!Array.isArray(message.nodes)) return 'nodes: array expected';
                for (var i = 0; i < message.nodes.length; ++i) {
                    var error = $root.topodata.ShardReplication.Node.verify(message.nodes[i]);
                    if (error) return 'nodes.' + error;
                }
            }
            return null;
        };

        /**
         * Creates a ShardReplication message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof topodata.ShardReplication
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {topodata.ShardReplication} ShardReplication
         */
        ShardReplication.fromObject = function fromObject(object) {
            if (object instanceof $root.topodata.ShardReplication) return object;
            var message = new $root.topodata.ShardReplication();
            if (object.nodes) {
                if (!Array.isArray(object.nodes)) throw TypeError('.topodata.ShardReplication.nodes: array expected');
                message.nodes = [];
                for (var i = 0; i < object.nodes.length; ++i) {
                    if (typeof object.nodes[i] !== 'object')
                        throw TypeError('.topodata.ShardReplication.nodes: object expected');
                    message.nodes[i] = $root.topodata.ShardReplication.Node.fromObject(object.nodes[i]);
                }
            }
            return message;
        };

        /**
         * Creates a plain object from a ShardReplication message. Also converts values to other types if specified.
         * @function toObject
         * @memberof topodata.ShardReplication
         * @static
         * @param {topodata.ShardReplication} message ShardReplication
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        ShardReplication.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.arrays || options.defaults) object.nodes = [];
            if (message.nodes && message.nodes.length) {
                object.nodes = [];
                for (var j = 0; j < message.nodes.length; ++j)
                    object.nodes[j] = $root.topodata.ShardReplication.Node.toObject(message.nodes[j], options);
            }
            return object;
        };

        /**
         * Converts this ShardReplication to JSON.
         * @function toJSON
         * @memberof topodata.ShardReplication
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        ShardReplication.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        ShardReplication.Node = (function () {
            /**
             * Properties of a Node.
             * @memberof topodata.ShardReplication
             * @interface INode
             * @property {topodata.ITabletAlias|null} [tablet_alias] Node tablet_alias
             */

            /**
             * Constructs a new Node.
             * @memberof topodata.ShardReplication
             * @classdesc Represents a Node.
             * @implements INode
             * @constructor
             * @param {topodata.ShardReplication.INode=} [properties] Properties to set
             */
            function Node(properties) {
                if (properties)
                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                        if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
            }

            /**
             * Node tablet_alias.
             * @member {topodata.ITabletAlias|null|undefined} tablet_alias
             * @memberof topodata.ShardReplication.Node
             * @instance
             */
            Node.prototype.tablet_alias = null;

            /**
             * Creates a new Node instance using the specified properties.
             * @function create
             * @memberof topodata.ShardReplication.Node
             * @static
             * @param {topodata.ShardReplication.INode=} [properties] Properties to set
             * @returns {topodata.ShardReplication.Node} Node instance
             */
            Node.create = function create(properties) {
                return new Node(properties);
            };

            /**
             * Encodes the specified Node message. Does not implicitly {@link topodata.ShardReplication.Node.verify|verify} messages.
             * @function encode
             * @memberof topodata.ShardReplication.Node
             * @static
             * @param {topodata.ShardReplication.INode} message Node message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            Node.encode = function encode(message, writer) {
                if (!writer) writer = $Writer.create();
                if (message.tablet_alias != null && Object.hasOwnProperty.call(message, 'tablet_alias'))
                    $root.topodata.TabletAlias.encode(
                        message.tablet_alias,
                        writer.uint32(/* id 1, wireType 2 =*/ 10).fork()
                    ).ldelim();
                return writer;
            };

            /**
             * Encodes the specified Node message, length delimited. Does not implicitly {@link topodata.ShardReplication.Node.verify|verify} messages.
             * @function encodeDelimited
             * @memberof topodata.ShardReplication.Node
             * @static
             * @param {topodata.ShardReplication.INode} message Node message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            Node.encodeDelimited = function encodeDelimited(message, writer) {
                return this.encode(message, writer).ldelim();
            };

            /**
             * Decodes a Node message from the specified reader or buffer.
             * @function decode
             * @memberof topodata.ShardReplication.Node
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {topodata.ShardReplication.Node} Node
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            Node.decode = function decode(reader, length) {
                if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
                var end = length === undefined ? reader.len : reader.pos + length,
                    message = new $root.topodata.ShardReplication.Node();
                while (reader.pos < end) {
                    var tag = reader.uint32();
                    switch (tag >>> 3) {
                        case 1:
                            message.tablet_alias = $root.topodata.TabletAlias.decode(reader, reader.uint32());
                            break;
                        default:
                            reader.skipType(tag & 7);
                            break;
                    }
                }
                return message;
            };

            /**
             * Decodes a Node message from the specified reader or buffer, length delimited.
             * @function decodeDelimited
             * @memberof topodata.ShardReplication.Node
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {topodata.ShardReplication.Node} Node
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            Node.decodeDelimited = function decodeDelimited(reader) {
                if (!(reader instanceof $Reader)) reader = new $Reader(reader);
                return this.decode(reader, reader.uint32());
            };

            /**
             * Verifies a Node message.
             * @function verify
             * @memberof topodata.ShardReplication.Node
             * @static
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {string|null} `null` if valid, otherwise the reason why it is not
             */
            Node.verify = function verify(message) {
                if (typeof message !== 'object' || message === null) return 'object expected';
                if (message.tablet_alias != null && message.hasOwnProperty('tablet_alias')) {
                    var error = $root.topodata.TabletAlias.verify(message.tablet_alias);
                    if (error) return 'tablet_alias.' + error;
                }
                return null;
            };

            /**
             * Creates a Node message from a plain object. Also converts values to their respective internal types.
             * @function fromObject
             * @memberof topodata.ShardReplication.Node
             * @static
             * @param {Object.<string,*>} object Plain object
             * @returns {topodata.ShardReplication.Node} Node
             */
            Node.fromObject = function fromObject(object) {
                if (object instanceof $root.topodata.ShardReplication.Node) return object;
                var message = new $root.topodata.ShardReplication.Node();
                if (object.tablet_alias != null) {
                    if (typeof object.tablet_alias !== 'object')
                        throw TypeError('.topodata.ShardReplication.Node.tablet_alias: object expected');
                    message.tablet_alias = $root.topodata.TabletAlias.fromObject(object.tablet_alias);
                }
                return message;
            };

            /**
             * Creates a plain object from a Node message. Also converts values to other types if specified.
             * @function toObject
             * @memberof topodata.ShardReplication.Node
             * @static
             * @param {topodata.ShardReplication.Node} message Node
             * @param {$protobuf.IConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            Node.toObject = function toObject(message, options) {
                if (!options) options = {};
                var object = {};
                if (options.defaults) object.tablet_alias = null;
                if (message.tablet_alias != null && message.hasOwnProperty('tablet_alias'))
                    object.tablet_alias = $root.topodata.TabletAlias.toObject(message.tablet_alias, options);
                return object;
            };

            /**
             * Converts this Node to JSON.
             * @function toJSON
             * @memberof topodata.ShardReplication.Node
             * @instance
             * @returns {Object.<string,*>} JSON object
             */
            Node.prototype.toJSON = function toJSON() {
                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
            };

            return Node;
        })();

        return ShardReplication;
    })();

    topodata.ShardReference = (function () {
        /**
         * Properties of a ShardReference.
         * @memberof topodata
         * @interface IShardReference
         * @property {string|null} [name] ShardReference name
         * @property {topodata.IKeyRange|null} [key_range] ShardReference key_range
         */

        /**
         * Constructs a new ShardReference.
         * @memberof topodata
         * @classdesc Represents a ShardReference.
         * @implements IShardReference
         * @constructor
         * @param {topodata.IShardReference=} [properties] Properties to set
         */
        function ShardReference(properties) {
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * ShardReference name.
         * @member {string} name
         * @memberof topodata.ShardReference
         * @instance
         */
        ShardReference.prototype.name = '';

        /**
         * ShardReference key_range.
         * @member {topodata.IKeyRange|null|undefined} key_range
         * @memberof topodata.ShardReference
         * @instance
         */
        ShardReference.prototype.key_range = null;

        /**
         * Creates a new ShardReference instance using the specified properties.
         * @function create
         * @memberof topodata.ShardReference
         * @static
         * @param {topodata.IShardReference=} [properties] Properties to set
         * @returns {topodata.ShardReference} ShardReference instance
         */
        ShardReference.create = function create(properties) {
            return new ShardReference(properties);
        };

        /**
         * Encodes the specified ShardReference message. Does not implicitly {@link topodata.ShardReference.verify|verify} messages.
         * @function encode
         * @memberof topodata.ShardReference
         * @static
         * @param {topodata.IShardReference} message ShardReference message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ShardReference.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.name != null && Object.hasOwnProperty.call(message, 'name'))
                writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.name);
            if (message.key_range != null && Object.hasOwnProperty.call(message, 'key_range'))
                $root.topodata.KeyRange.encode(
                    message.key_range,
                    writer.uint32(/* id 2, wireType 2 =*/ 18).fork()
                ).ldelim();
            return writer;
        };

        /**
         * Encodes the specified ShardReference message, length delimited. Does not implicitly {@link topodata.ShardReference.verify|verify} messages.
         * @function encodeDelimited
         * @memberof topodata.ShardReference
         * @static
         * @param {topodata.IShardReference} message ShardReference message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ShardReference.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a ShardReference message from the specified reader or buffer.
         * @function decode
         * @memberof topodata.ShardReference
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {topodata.ShardReference} ShardReference
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ShardReference.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.topodata.ShardReference();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        message.name = reader.string();
                        break;
                    case 2:
                        message.key_range = $root.topodata.KeyRange.decode(reader, reader.uint32());
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a ShardReference message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof topodata.ShardReference
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {topodata.ShardReference} ShardReference
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ShardReference.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a ShardReference message.
         * @function verify
         * @memberof topodata.ShardReference
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        ShardReference.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.name != null && message.hasOwnProperty('name'))
                if (!$util.isString(message.name)) return 'name: string expected';
            if (message.key_range != null && message.hasOwnProperty('key_range')) {
                var error = $root.topodata.KeyRange.verify(message.key_range);
                if (error) return 'key_range.' + error;
            }
            return null;
        };

        /**
         * Creates a ShardReference message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof topodata.ShardReference
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {topodata.ShardReference} ShardReference
         */
        ShardReference.fromObject = function fromObject(object) {
            if (object instanceof $root.topodata.ShardReference) return object;
            var message = new $root.topodata.ShardReference();
            if (object.name != null) message.name = String(object.name);
            if (object.key_range != null) {
                if (typeof object.key_range !== 'object')
                    throw TypeError('.topodata.ShardReference.key_range: object expected');
                message.key_range = $root.topodata.KeyRange.fromObject(object.key_range);
            }
            return message;
        };

        /**
         * Creates a plain object from a ShardReference message. Also converts values to other types if specified.
         * @function toObject
         * @memberof topodata.ShardReference
         * @static
         * @param {topodata.ShardReference} message ShardReference
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        ShardReference.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.defaults) {
                object.name = '';
                object.key_range = null;
            }
            if (message.name != null && message.hasOwnProperty('name')) object.name = message.name;
            if (message.key_range != null && message.hasOwnProperty('key_range'))
                object.key_range = $root.topodata.KeyRange.toObject(message.key_range, options);
            return object;
        };

        /**
         * Converts this ShardReference to JSON.
         * @function toJSON
         * @memberof topodata.ShardReference
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        ShardReference.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return ShardReference;
    })();

    topodata.ShardTabletControl = (function () {
        /**
         * Properties of a ShardTabletControl.
         * @memberof topodata
         * @interface IShardTabletControl
         * @property {string|null} [name] ShardTabletControl name
         * @property {topodata.IKeyRange|null} [key_range] ShardTabletControl key_range
         * @property {boolean|null} [query_service_disabled] ShardTabletControl query_service_disabled
         */

        /**
         * Constructs a new ShardTabletControl.
         * @memberof topodata
         * @classdesc Represents a ShardTabletControl.
         * @implements IShardTabletControl
         * @constructor
         * @param {topodata.IShardTabletControl=} [properties] Properties to set
         */
        function ShardTabletControl(properties) {
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * ShardTabletControl name.
         * @member {string} name
         * @memberof topodata.ShardTabletControl
         * @instance
         */
        ShardTabletControl.prototype.name = '';

        /**
         * ShardTabletControl key_range.
         * @member {topodata.IKeyRange|null|undefined} key_range
         * @memberof topodata.ShardTabletControl
         * @instance
         */
        ShardTabletControl.prototype.key_range = null;

        /**
         * ShardTabletControl query_service_disabled.
         * @member {boolean} query_service_disabled
         * @memberof topodata.ShardTabletControl
         * @instance
         */
        ShardTabletControl.prototype.query_service_disabled = false;

        /**
         * Creates a new ShardTabletControl instance using the specified properties.
         * @function create
         * @memberof topodata.ShardTabletControl
         * @static
         * @param {topodata.IShardTabletControl=} [properties] Properties to set
         * @returns {topodata.ShardTabletControl} ShardTabletControl instance
         */
        ShardTabletControl.create = function create(properties) {
            return new ShardTabletControl(properties);
        };

        /**
         * Encodes the specified ShardTabletControl message. Does not implicitly {@link topodata.ShardTabletControl.verify|verify} messages.
         * @function encode
         * @memberof topodata.ShardTabletControl
         * @static
         * @param {topodata.IShardTabletControl} message ShardTabletControl message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ShardTabletControl.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.name != null && Object.hasOwnProperty.call(message, 'name'))
                writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.name);
            if (message.key_range != null && Object.hasOwnProperty.call(message, 'key_range'))
                $root.topodata.KeyRange.encode(
                    message.key_range,
                    writer.uint32(/* id 2, wireType 2 =*/ 18).fork()
                ).ldelim();
            if (message.query_service_disabled != null && Object.hasOwnProperty.call(message, 'query_service_disabled'))
                writer.uint32(/* id 3, wireType 0 =*/ 24).bool(message.query_service_disabled);
            return writer;
        };

        /**
         * Encodes the specified ShardTabletControl message, length delimited. Does not implicitly {@link topodata.ShardTabletControl.verify|verify} messages.
         * @function encodeDelimited
         * @memberof topodata.ShardTabletControl
         * @static
         * @param {topodata.IShardTabletControl} message ShardTabletControl message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        ShardTabletControl.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a ShardTabletControl message from the specified reader or buffer.
         * @function decode
         * @memberof topodata.ShardTabletControl
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {topodata.ShardTabletControl} ShardTabletControl
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ShardTabletControl.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.topodata.ShardTabletControl();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        message.name = reader.string();
                        break;
                    case 2:
                        message.key_range = $root.topodata.KeyRange.decode(reader, reader.uint32());
                        break;
                    case 3:
                        message.query_service_disabled = reader.bool();
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a ShardTabletControl message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof topodata.ShardTabletControl
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {topodata.ShardTabletControl} ShardTabletControl
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        ShardTabletControl.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a ShardTabletControl message.
         * @function verify
         * @memberof topodata.ShardTabletControl
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        ShardTabletControl.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.name != null && message.hasOwnProperty('name'))
                if (!$util.isString(message.name)) return 'name: string expected';
            if (message.key_range != null && message.hasOwnProperty('key_range')) {
                var error = $root.topodata.KeyRange.verify(message.key_range);
                if (error) return 'key_range.' + error;
            }
            if (message.query_service_disabled != null && message.hasOwnProperty('query_service_disabled'))
                if (typeof message.query_service_disabled !== 'boolean')
                    return 'query_service_disabled: boolean expected';
            return null;
        };

        /**
         * Creates a ShardTabletControl message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof topodata.ShardTabletControl
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {topodata.ShardTabletControl} ShardTabletControl
         */
        ShardTabletControl.fromObject = function fromObject(object) {
            if (object instanceof $root.topodata.ShardTabletControl) return object;
            var message = new $root.topodata.ShardTabletControl();
            if (object.name != null) message.name = String(object.name);
            if (object.key_range != null) {
                if (typeof object.key_range !== 'object')
                    throw TypeError('.topodata.ShardTabletControl.key_range: object expected');
                message.key_range = $root.topodata.KeyRange.fromObject(object.key_range);
            }
            if (object.query_service_disabled != null)
                message.query_service_disabled = Boolean(object.query_service_disabled);
            return message;
        };

        /**
         * Creates a plain object from a ShardTabletControl message. Also converts values to other types if specified.
         * @function toObject
         * @memberof topodata.ShardTabletControl
         * @static
         * @param {topodata.ShardTabletControl} message ShardTabletControl
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        ShardTabletControl.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.defaults) {
                object.name = '';
                object.key_range = null;
                object.query_service_disabled = false;
            }
            if (message.name != null && message.hasOwnProperty('name')) object.name = message.name;
            if (message.key_range != null && message.hasOwnProperty('key_range'))
                object.key_range = $root.topodata.KeyRange.toObject(message.key_range, options);
            if (message.query_service_disabled != null && message.hasOwnProperty('query_service_disabled'))
                object.query_service_disabled = message.query_service_disabled;
            return object;
        };

        /**
         * Converts this ShardTabletControl to JSON.
         * @function toJSON
         * @memberof topodata.ShardTabletControl
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        ShardTabletControl.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return ShardTabletControl;
    })();

    topodata.SrvKeyspace = (function () {
        /**
         * Properties of a SrvKeyspace.
         * @memberof topodata
         * @interface ISrvKeyspace
         * @property {Array.<topodata.SrvKeyspace.IKeyspacePartition>|null} [partitions] SrvKeyspace partitions
         * @property {string|null} [sharding_column_name] SrvKeyspace sharding_column_name
         * @property {topodata.KeyspaceIdType|null} [sharding_column_type] SrvKeyspace sharding_column_type
         * @property {Array.<topodata.SrvKeyspace.IServedFrom>|null} [served_from] SrvKeyspace served_from
         */

        /**
         * Constructs a new SrvKeyspace.
         * @memberof topodata
         * @classdesc Represents a SrvKeyspace.
         * @implements ISrvKeyspace
         * @constructor
         * @param {topodata.ISrvKeyspace=} [properties] Properties to set
         */
        function SrvKeyspace(properties) {
            this.partitions = [];
            this.served_from = [];
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * SrvKeyspace partitions.
         * @member {Array.<topodata.SrvKeyspace.IKeyspacePartition>} partitions
         * @memberof topodata.SrvKeyspace
         * @instance
         */
        SrvKeyspace.prototype.partitions = $util.emptyArray;

        /**
         * SrvKeyspace sharding_column_name.
         * @member {string} sharding_column_name
         * @memberof topodata.SrvKeyspace
         * @instance
         */
        SrvKeyspace.prototype.sharding_column_name = '';

        /**
         * SrvKeyspace sharding_column_type.
         * @member {topodata.KeyspaceIdType} sharding_column_type
         * @memberof topodata.SrvKeyspace
         * @instance
         */
        SrvKeyspace.prototype.sharding_column_type = 0;

        /**
         * SrvKeyspace served_from.
         * @member {Array.<topodata.SrvKeyspace.IServedFrom>} served_from
         * @memberof topodata.SrvKeyspace
         * @instance
         */
        SrvKeyspace.prototype.served_from = $util.emptyArray;

        /**
         * Creates a new SrvKeyspace instance using the specified properties.
         * @function create
         * @memberof topodata.SrvKeyspace
         * @static
         * @param {topodata.ISrvKeyspace=} [properties] Properties to set
         * @returns {topodata.SrvKeyspace} SrvKeyspace instance
         */
        SrvKeyspace.create = function create(properties) {
            return new SrvKeyspace(properties);
        };

        /**
         * Encodes the specified SrvKeyspace message. Does not implicitly {@link topodata.SrvKeyspace.verify|verify} messages.
         * @function encode
         * @memberof topodata.SrvKeyspace
         * @static
         * @param {topodata.ISrvKeyspace} message SrvKeyspace message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        SrvKeyspace.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.partitions != null && message.partitions.length)
                for (var i = 0; i < message.partitions.length; ++i)
                    $root.topodata.SrvKeyspace.KeyspacePartition.encode(
                        message.partitions[i],
                        writer.uint32(/* id 1, wireType 2 =*/ 10).fork()
                    ).ldelim();
            if (message.sharding_column_name != null && Object.hasOwnProperty.call(message, 'sharding_column_name'))
                writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.sharding_column_name);
            if (message.sharding_column_type != null && Object.hasOwnProperty.call(message, 'sharding_column_type'))
                writer.uint32(/* id 3, wireType 0 =*/ 24).int32(message.sharding_column_type);
            if (message.served_from != null && message.served_from.length)
                for (var i = 0; i < message.served_from.length; ++i)
                    $root.topodata.SrvKeyspace.ServedFrom.encode(
                        message.served_from[i],
                        writer.uint32(/* id 4, wireType 2 =*/ 34).fork()
                    ).ldelim();
            return writer;
        };

        /**
         * Encodes the specified SrvKeyspace message, length delimited. Does not implicitly {@link topodata.SrvKeyspace.verify|verify} messages.
         * @function encodeDelimited
         * @memberof topodata.SrvKeyspace
         * @static
         * @param {topodata.ISrvKeyspace} message SrvKeyspace message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        SrvKeyspace.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a SrvKeyspace message from the specified reader or buffer.
         * @function decode
         * @memberof topodata.SrvKeyspace
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {topodata.SrvKeyspace} SrvKeyspace
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        SrvKeyspace.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.topodata.SrvKeyspace();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        if (!(message.partitions && message.partitions.length)) message.partitions = [];
                        message.partitions.push(
                            $root.topodata.SrvKeyspace.KeyspacePartition.decode(reader, reader.uint32())
                        );
                        break;
                    case 2:
                        message.sharding_column_name = reader.string();
                        break;
                    case 3:
                        message.sharding_column_type = reader.int32();
                        break;
                    case 4:
                        if (!(message.served_from && message.served_from.length)) message.served_from = [];
                        message.served_from.push($root.topodata.SrvKeyspace.ServedFrom.decode(reader, reader.uint32()));
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a SrvKeyspace message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof topodata.SrvKeyspace
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {topodata.SrvKeyspace} SrvKeyspace
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        SrvKeyspace.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a SrvKeyspace message.
         * @function verify
         * @memberof topodata.SrvKeyspace
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        SrvKeyspace.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.partitions != null && message.hasOwnProperty('partitions')) {
                if (!Array.isArray(message.partitions)) return 'partitions: array expected';
                for (var i = 0; i < message.partitions.length; ++i) {
                    var error = $root.topodata.SrvKeyspace.KeyspacePartition.verify(message.partitions[i]);
                    if (error) return 'partitions.' + error;
                }
            }
            if (message.sharding_column_name != null && message.hasOwnProperty('sharding_column_name'))
                if (!$util.isString(message.sharding_column_name)) return 'sharding_column_name: string expected';
            if (message.sharding_column_type != null && message.hasOwnProperty('sharding_column_type'))
                switch (message.sharding_column_type) {
                    default:
                        return 'sharding_column_type: enum value expected';
                    case 0:
                    case 1:
                    case 2:
                        break;
                }
            if (message.served_from != null && message.hasOwnProperty('served_from')) {
                if (!Array.isArray(message.served_from)) return 'served_from: array expected';
                for (var i = 0; i < message.served_from.length; ++i) {
                    var error = $root.topodata.SrvKeyspace.ServedFrom.verify(message.served_from[i]);
                    if (error) return 'served_from.' + error;
                }
            }
            return null;
        };

        /**
         * Creates a SrvKeyspace message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof topodata.SrvKeyspace
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {topodata.SrvKeyspace} SrvKeyspace
         */
        SrvKeyspace.fromObject = function fromObject(object) {
            if (object instanceof $root.topodata.SrvKeyspace) return object;
            var message = new $root.topodata.SrvKeyspace();
            if (object.partitions) {
                if (!Array.isArray(object.partitions))
                    throw TypeError('.topodata.SrvKeyspace.partitions: array expected');
                message.partitions = [];
                for (var i = 0; i < object.partitions.length; ++i) {
                    if (typeof object.partitions[i] !== 'object')
                        throw TypeError('.topodata.SrvKeyspace.partitions: object expected');
                    message.partitions[i] = $root.topodata.SrvKeyspace.KeyspacePartition.fromObject(
                        object.partitions[i]
                    );
                }
            }
            if (object.sharding_column_name != null) message.sharding_column_name = String(object.sharding_column_name);
            switch (object.sharding_column_type) {
                case 'UNSET':
                case 0:
                    message.sharding_column_type = 0;
                    break;
                case 'UINT64':
                case 1:
                    message.sharding_column_type = 1;
                    break;
                case 'BYTES':
                case 2:
                    message.sharding_column_type = 2;
                    break;
            }
            if (object.served_from) {
                if (!Array.isArray(object.served_from))
                    throw TypeError('.topodata.SrvKeyspace.served_from: array expected');
                message.served_from = [];
                for (var i = 0; i < object.served_from.length; ++i) {
                    if (typeof object.served_from[i] !== 'object')
                        throw TypeError('.topodata.SrvKeyspace.served_from: object expected');
                    message.served_from[i] = $root.topodata.SrvKeyspace.ServedFrom.fromObject(object.served_from[i]);
                }
            }
            return message;
        };

        /**
         * Creates a plain object from a SrvKeyspace message. Also converts values to other types if specified.
         * @function toObject
         * @memberof topodata.SrvKeyspace
         * @static
         * @param {topodata.SrvKeyspace} message SrvKeyspace
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        SrvKeyspace.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.arrays || options.defaults) {
                object.partitions = [];
                object.served_from = [];
            }
            if (options.defaults) {
                object.sharding_column_name = '';
                object.sharding_column_type = options.enums === String ? 'UNSET' : 0;
            }
            if (message.partitions && message.partitions.length) {
                object.partitions = [];
                for (var j = 0; j < message.partitions.length; ++j)
                    object.partitions[j] = $root.topodata.SrvKeyspace.KeyspacePartition.toObject(
                        message.partitions[j],
                        options
                    );
            }
            if (message.sharding_column_name != null && message.hasOwnProperty('sharding_column_name'))
                object.sharding_column_name = message.sharding_column_name;
            if (message.sharding_column_type != null && message.hasOwnProperty('sharding_column_type'))
                object.sharding_column_type =
                    options.enums === String
                        ? $root.topodata.KeyspaceIdType[message.sharding_column_type]
                        : message.sharding_column_type;
            if (message.served_from && message.served_from.length) {
                object.served_from = [];
                for (var j = 0; j < message.served_from.length; ++j)
                    object.served_from[j] = $root.topodata.SrvKeyspace.ServedFrom.toObject(
                        message.served_from[j],
                        options
                    );
            }
            return object;
        };

        /**
         * Converts this SrvKeyspace to JSON.
         * @function toJSON
         * @memberof topodata.SrvKeyspace
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        SrvKeyspace.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        SrvKeyspace.KeyspacePartition = (function () {
            /**
             * Properties of a KeyspacePartition.
             * @memberof topodata.SrvKeyspace
             * @interface IKeyspacePartition
             * @property {topodata.TabletType|null} [served_type] KeyspacePartition served_type
             * @property {Array.<topodata.IShardReference>|null} [shard_references] KeyspacePartition shard_references
             * @property {Array.<topodata.IShardTabletControl>|null} [shard_tablet_controls] KeyspacePartition shard_tablet_controls
             */

            /**
             * Constructs a new KeyspacePartition.
             * @memberof topodata.SrvKeyspace
             * @classdesc Represents a KeyspacePartition.
             * @implements IKeyspacePartition
             * @constructor
             * @param {topodata.SrvKeyspace.IKeyspacePartition=} [properties] Properties to set
             */
            function KeyspacePartition(properties) {
                this.shard_references = [];
                this.shard_tablet_controls = [];
                if (properties)
                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                        if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
            }

            /**
             * KeyspacePartition served_type.
             * @member {topodata.TabletType} served_type
             * @memberof topodata.SrvKeyspace.KeyspacePartition
             * @instance
             */
            KeyspacePartition.prototype.served_type = 0;

            /**
             * KeyspacePartition shard_references.
             * @member {Array.<topodata.IShardReference>} shard_references
             * @memberof topodata.SrvKeyspace.KeyspacePartition
             * @instance
             */
            KeyspacePartition.prototype.shard_references = $util.emptyArray;

            /**
             * KeyspacePartition shard_tablet_controls.
             * @member {Array.<topodata.IShardTabletControl>} shard_tablet_controls
             * @memberof topodata.SrvKeyspace.KeyspacePartition
             * @instance
             */
            KeyspacePartition.prototype.shard_tablet_controls = $util.emptyArray;

            /**
             * Creates a new KeyspacePartition instance using the specified properties.
             * @function create
             * @memberof topodata.SrvKeyspace.KeyspacePartition
             * @static
             * @param {topodata.SrvKeyspace.IKeyspacePartition=} [properties] Properties to set
             * @returns {topodata.SrvKeyspace.KeyspacePartition} KeyspacePartition instance
             */
            KeyspacePartition.create = function create(properties) {
                return new KeyspacePartition(properties);
            };

            /**
             * Encodes the specified KeyspacePartition message. Does not implicitly {@link topodata.SrvKeyspace.KeyspacePartition.verify|verify} messages.
             * @function encode
             * @memberof topodata.SrvKeyspace.KeyspacePartition
             * @static
             * @param {topodata.SrvKeyspace.IKeyspacePartition} message KeyspacePartition message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            KeyspacePartition.encode = function encode(message, writer) {
                if (!writer) writer = $Writer.create();
                if (message.served_type != null && Object.hasOwnProperty.call(message, 'served_type'))
                    writer.uint32(/* id 1, wireType 0 =*/ 8).int32(message.served_type);
                if (message.shard_references != null && message.shard_references.length)
                    for (var i = 0; i < message.shard_references.length; ++i)
                        $root.topodata.ShardReference.encode(
                            message.shard_references[i],
                            writer.uint32(/* id 2, wireType 2 =*/ 18).fork()
                        ).ldelim();
                if (message.shard_tablet_controls != null && message.shard_tablet_controls.length)
                    for (var i = 0; i < message.shard_tablet_controls.length; ++i)
                        $root.topodata.ShardTabletControl.encode(
                            message.shard_tablet_controls[i],
                            writer.uint32(/* id 3, wireType 2 =*/ 26).fork()
                        ).ldelim();
                return writer;
            };

            /**
             * Encodes the specified KeyspacePartition message, length delimited. Does not implicitly {@link topodata.SrvKeyspace.KeyspacePartition.verify|verify} messages.
             * @function encodeDelimited
             * @memberof topodata.SrvKeyspace.KeyspacePartition
             * @static
             * @param {topodata.SrvKeyspace.IKeyspacePartition} message KeyspacePartition message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            KeyspacePartition.encodeDelimited = function encodeDelimited(message, writer) {
                return this.encode(message, writer).ldelim();
            };

            /**
             * Decodes a KeyspacePartition message from the specified reader or buffer.
             * @function decode
             * @memberof topodata.SrvKeyspace.KeyspacePartition
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {topodata.SrvKeyspace.KeyspacePartition} KeyspacePartition
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            KeyspacePartition.decode = function decode(reader, length) {
                if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
                var end = length === undefined ? reader.len : reader.pos + length,
                    message = new $root.topodata.SrvKeyspace.KeyspacePartition();
                while (reader.pos < end) {
                    var tag = reader.uint32();
                    switch (tag >>> 3) {
                        case 1:
                            message.served_type = reader.int32();
                            break;
                        case 2:
                            if (!(message.shard_references && message.shard_references.length))
                                message.shard_references = [];
                            message.shard_references.push(
                                $root.topodata.ShardReference.decode(reader, reader.uint32())
                            );
                            break;
                        case 3:
                            if (!(message.shard_tablet_controls && message.shard_tablet_controls.length))
                                message.shard_tablet_controls = [];
                            message.shard_tablet_controls.push(
                                $root.topodata.ShardTabletControl.decode(reader, reader.uint32())
                            );
                            break;
                        default:
                            reader.skipType(tag & 7);
                            break;
                    }
                }
                return message;
            };

            /**
             * Decodes a KeyspacePartition message from the specified reader or buffer, length delimited.
             * @function decodeDelimited
             * @memberof topodata.SrvKeyspace.KeyspacePartition
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {topodata.SrvKeyspace.KeyspacePartition} KeyspacePartition
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            KeyspacePartition.decodeDelimited = function decodeDelimited(reader) {
                if (!(reader instanceof $Reader)) reader = new $Reader(reader);
                return this.decode(reader, reader.uint32());
            };

            /**
             * Verifies a KeyspacePartition message.
             * @function verify
             * @memberof topodata.SrvKeyspace.KeyspacePartition
             * @static
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {string|null} `null` if valid, otherwise the reason why it is not
             */
            KeyspacePartition.verify = function verify(message) {
                if (typeof message !== 'object' || message === null) return 'object expected';
                if (message.served_type != null && message.hasOwnProperty('served_type'))
                    switch (message.served_type) {
                        default:
                            return 'served_type: enum value expected';
                        case 0:
                        case 1:
                        case 2:
                        case 3:
                        case 3:
                        case 4:
                        case 5:
                        case 6:
                        case 7:
                        case 8:
                            break;
                    }
                if (message.shard_references != null && message.hasOwnProperty('shard_references')) {
                    if (!Array.isArray(message.shard_references)) return 'shard_references: array expected';
                    for (var i = 0; i < message.shard_references.length; ++i) {
                        var error = $root.topodata.ShardReference.verify(message.shard_references[i]);
                        if (error) return 'shard_references.' + error;
                    }
                }
                if (message.shard_tablet_controls != null && message.hasOwnProperty('shard_tablet_controls')) {
                    if (!Array.isArray(message.shard_tablet_controls)) return 'shard_tablet_controls: array expected';
                    for (var i = 0; i < message.shard_tablet_controls.length; ++i) {
                        var error = $root.topodata.ShardTabletControl.verify(message.shard_tablet_controls[i]);
                        if (error) return 'shard_tablet_controls.' + error;
                    }
                }
                return null;
            };

            /**
             * Creates a KeyspacePartition message from a plain object. Also converts values to their respective internal types.
             * @function fromObject
             * @memberof topodata.SrvKeyspace.KeyspacePartition
             * @static
             * @param {Object.<string,*>} object Plain object
             * @returns {topodata.SrvKeyspace.KeyspacePartition} KeyspacePartition
             */
            KeyspacePartition.fromObject = function fromObject(object) {
                if (object instanceof $root.topodata.SrvKeyspace.KeyspacePartition) return object;
                var message = new $root.topodata.SrvKeyspace.KeyspacePartition();
                switch (object.served_type) {
                    case 'UNKNOWN':
                    case 0:
                        message.served_type = 0;
                        break;
                    case 'MASTER':
                    case 1:
                        message.served_type = 1;
                        break;
                    case 'REPLICA':
                    case 2:
                        message.served_type = 2;
                        break;
                    case 'RDONLY':
                    case 3:
                        message.served_type = 3;
                        break;
                    case 'BATCH':
                    case 3:
                        message.served_type = 3;
                        break;
                    case 'SPARE':
                    case 4:
                        message.served_type = 4;
                        break;
                    case 'EXPERIMENTAL':
                    case 5:
                        message.served_type = 5;
                        break;
                    case 'BACKUP':
                    case 6:
                        message.served_type = 6;
                        break;
                    case 'RESTORE':
                    case 7:
                        message.served_type = 7;
                        break;
                    case 'DRAINED':
                    case 8:
                        message.served_type = 8;
                        break;
                }
                if (object.shard_references) {
                    if (!Array.isArray(object.shard_references))
                        throw TypeError('.topodata.SrvKeyspace.KeyspacePartition.shard_references: array expected');
                    message.shard_references = [];
                    for (var i = 0; i < object.shard_references.length; ++i) {
                        if (typeof object.shard_references[i] !== 'object')
                            throw TypeError(
                                '.topodata.SrvKeyspace.KeyspacePartition.shard_references: object expected'
                            );
                        message.shard_references[i] = $root.topodata.ShardReference.fromObject(
                            object.shard_references[i]
                        );
                    }
                }
                if (object.shard_tablet_controls) {
                    if (!Array.isArray(object.shard_tablet_controls))
                        throw TypeError(
                            '.topodata.SrvKeyspace.KeyspacePartition.shard_tablet_controls: array expected'
                        );
                    message.shard_tablet_controls = [];
                    for (var i = 0; i < object.shard_tablet_controls.length; ++i) {
                        if (typeof object.shard_tablet_controls[i] !== 'object')
                            throw TypeError(
                                '.topodata.SrvKeyspace.KeyspacePartition.shard_tablet_controls: object expected'
                            );
                        message.shard_tablet_controls[i] = $root.topodata.ShardTabletControl.fromObject(
                            object.shard_tablet_controls[i]
                        );
                    }
                }
                return message;
            };

            /**
             * Creates a plain object from a KeyspacePartition message. Also converts values to other types if specified.
             * @function toObject
             * @memberof topodata.SrvKeyspace.KeyspacePartition
             * @static
             * @param {topodata.SrvKeyspace.KeyspacePartition} message KeyspacePartition
             * @param {$protobuf.IConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            KeyspacePartition.toObject = function toObject(message, options) {
                if (!options) options = {};
                var object = {};
                if (options.arrays || options.defaults) {
                    object.shard_references = [];
                    object.shard_tablet_controls = [];
                }
                if (options.defaults) object.served_type = options.enums === String ? 'UNKNOWN' : 0;
                if (message.served_type != null && message.hasOwnProperty('served_type'))
                    object.served_type =
                        options.enums === String ? $root.topodata.TabletType[message.served_type] : message.served_type;
                if (message.shard_references && message.shard_references.length) {
                    object.shard_references = [];
                    for (var j = 0; j < message.shard_references.length; ++j)
                        object.shard_references[j] = $root.topodata.ShardReference.toObject(
                            message.shard_references[j],
                            options
                        );
                }
                if (message.shard_tablet_controls && message.shard_tablet_controls.length) {
                    object.shard_tablet_controls = [];
                    for (var j = 0; j < message.shard_tablet_controls.length; ++j)
                        object.shard_tablet_controls[j] = $root.topodata.ShardTabletControl.toObject(
                            message.shard_tablet_controls[j],
                            options
                        );
                }
                return object;
            };

            /**
             * Converts this KeyspacePartition to JSON.
             * @function toJSON
             * @memberof topodata.SrvKeyspace.KeyspacePartition
             * @instance
             * @returns {Object.<string,*>} JSON object
             */
            KeyspacePartition.prototype.toJSON = function toJSON() {
                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
            };

            return KeyspacePartition;
        })();

        SrvKeyspace.ServedFrom = (function () {
            /**
             * Properties of a ServedFrom.
             * @memberof topodata.SrvKeyspace
             * @interface IServedFrom
             * @property {topodata.TabletType|null} [tablet_type] ServedFrom tablet_type
             * @property {string|null} [keyspace] ServedFrom keyspace
             */

            /**
             * Constructs a new ServedFrom.
             * @memberof topodata.SrvKeyspace
             * @classdesc Represents a ServedFrom.
             * @implements IServedFrom
             * @constructor
             * @param {topodata.SrvKeyspace.IServedFrom=} [properties] Properties to set
             */
            function ServedFrom(properties) {
                if (properties)
                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                        if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
            }

            /**
             * ServedFrom tablet_type.
             * @member {topodata.TabletType} tablet_type
             * @memberof topodata.SrvKeyspace.ServedFrom
             * @instance
             */
            ServedFrom.prototype.tablet_type = 0;

            /**
             * ServedFrom keyspace.
             * @member {string} keyspace
             * @memberof topodata.SrvKeyspace.ServedFrom
             * @instance
             */
            ServedFrom.prototype.keyspace = '';

            /**
             * Creates a new ServedFrom instance using the specified properties.
             * @function create
             * @memberof topodata.SrvKeyspace.ServedFrom
             * @static
             * @param {topodata.SrvKeyspace.IServedFrom=} [properties] Properties to set
             * @returns {topodata.SrvKeyspace.ServedFrom} ServedFrom instance
             */
            ServedFrom.create = function create(properties) {
                return new ServedFrom(properties);
            };

            /**
             * Encodes the specified ServedFrom message. Does not implicitly {@link topodata.SrvKeyspace.ServedFrom.verify|verify} messages.
             * @function encode
             * @memberof topodata.SrvKeyspace.ServedFrom
             * @static
             * @param {topodata.SrvKeyspace.IServedFrom} message ServedFrom message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            ServedFrom.encode = function encode(message, writer) {
                if (!writer) writer = $Writer.create();
                if (message.tablet_type != null && Object.hasOwnProperty.call(message, 'tablet_type'))
                    writer.uint32(/* id 1, wireType 0 =*/ 8).int32(message.tablet_type);
                if (message.keyspace != null && Object.hasOwnProperty.call(message, 'keyspace'))
                    writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.keyspace);
                return writer;
            };

            /**
             * Encodes the specified ServedFrom message, length delimited. Does not implicitly {@link topodata.SrvKeyspace.ServedFrom.verify|verify} messages.
             * @function encodeDelimited
             * @memberof topodata.SrvKeyspace.ServedFrom
             * @static
             * @param {topodata.SrvKeyspace.IServedFrom} message ServedFrom message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            ServedFrom.encodeDelimited = function encodeDelimited(message, writer) {
                return this.encode(message, writer).ldelim();
            };

            /**
             * Decodes a ServedFrom message from the specified reader or buffer.
             * @function decode
             * @memberof topodata.SrvKeyspace.ServedFrom
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {topodata.SrvKeyspace.ServedFrom} ServedFrom
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            ServedFrom.decode = function decode(reader, length) {
                if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
                var end = length === undefined ? reader.len : reader.pos + length,
                    message = new $root.topodata.SrvKeyspace.ServedFrom();
                while (reader.pos < end) {
                    var tag = reader.uint32();
                    switch (tag >>> 3) {
                        case 1:
                            message.tablet_type = reader.int32();
                            break;
                        case 2:
                            message.keyspace = reader.string();
                            break;
                        default:
                            reader.skipType(tag & 7);
                            break;
                    }
                }
                return message;
            };

            /**
             * Decodes a ServedFrom message from the specified reader or buffer, length delimited.
             * @function decodeDelimited
             * @memberof topodata.SrvKeyspace.ServedFrom
             * @static
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {topodata.SrvKeyspace.ServedFrom} ServedFrom
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            ServedFrom.decodeDelimited = function decodeDelimited(reader) {
                if (!(reader instanceof $Reader)) reader = new $Reader(reader);
                return this.decode(reader, reader.uint32());
            };

            /**
             * Verifies a ServedFrom message.
             * @function verify
             * @memberof topodata.SrvKeyspace.ServedFrom
             * @static
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {string|null} `null` if valid, otherwise the reason why it is not
             */
            ServedFrom.verify = function verify(message) {
                if (typeof message !== 'object' || message === null) return 'object expected';
                if (message.tablet_type != null && message.hasOwnProperty('tablet_type'))
                    switch (message.tablet_type) {
                        default:
                            return 'tablet_type: enum value expected';
                        case 0:
                        case 1:
                        case 2:
                        case 3:
                        case 3:
                        case 4:
                        case 5:
                        case 6:
                        case 7:
                        case 8:
                            break;
                    }
                if (message.keyspace != null && message.hasOwnProperty('keyspace'))
                    if (!$util.isString(message.keyspace)) return 'keyspace: string expected';
                return null;
            };

            /**
             * Creates a ServedFrom message from a plain object. Also converts values to their respective internal types.
             * @function fromObject
             * @memberof topodata.SrvKeyspace.ServedFrom
             * @static
             * @param {Object.<string,*>} object Plain object
             * @returns {topodata.SrvKeyspace.ServedFrom} ServedFrom
             */
            ServedFrom.fromObject = function fromObject(object) {
                if (object instanceof $root.topodata.SrvKeyspace.ServedFrom) return object;
                var message = new $root.topodata.SrvKeyspace.ServedFrom();
                switch (object.tablet_type) {
                    case 'UNKNOWN':
                    case 0:
                        message.tablet_type = 0;
                        break;
                    case 'MASTER':
                    case 1:
                        message.tablet_type = 1;
                        break;
                    case 'REPLICA':
                    case 2:
                        message.tablet_type = 2;
                        break;
                    case 'RDONLY':
                    case 3:
                        message.tablet_type = 3;
                        break;
                    case 'BATCH':
                    case 3:
                        message.tablet_type = 3;
                        break;
                    case 'SPARE':
                    case 4:
                        message.tablet_type = 4;
                        break;
                    case 'EXPERIMENTAL':
                    case 5:
                        message.tablet_type = 5;
                        break;
                    case 'BACKUP':
                    case 6:
                        message.tablet_type = 6;
                        break;
                    case 'RESTORE':
                    case 7:
                        message.tablet_type = 7;
                        break;
                    case 'DRAINED':
                    case 8:
                        message.tablet_type = 8;
                        break;
                }
                if (object.keyspace != null) message.keyspace = String(object.keyspace);
                return message;
            };

            /**
             * Creates a plain object from a ServedFrom message. Also converts values to other types if specified.
             * @function toObject
             * @memberof topodata.SrvKeyspace.ServedFrom
             * @static
             * @param {topodata.SrvKeyspace.ServedFrom} message ServedFrom
             * @param {$protobuf.IConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            ServedFrom.toObject = function toObject(message, options) {
                if (!options) options = {};
                var object = {};
                if (options.defaults) {
                    object.tablet_type = options.enums === String ? 'UNKNOWN' : 0;
                    object.keyspace = '';
                }
                if (message.tablet_type != null && message.hasOwnProperty('tablet_type'))
                    object.tablet_type =
                        options.enums === String ? $root.topodata.TabletType[message.tablet_type] : message.tablet_type;
                if (message.keyspace != null && message.hasOwnProperty('keyspace')) object.keyspace = message.keyspace;
                return object;
            };

            /**
             * Converts this ServedFrom to JSON.
             * @function toJSON
             * @memberof topodata.SrvKeyspace.ServedFrom
             * @instance
             * @returns {Object.<string,*>} JSON object
             */
            ServedFrom.prototype.toJSON = function toJSON() {
                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
            };

            return ServedFrom;
        })();

        return SrvKeyspace;
    })();

    topodata.CellInfo = (function () {
        /**
         * Properties of a CellInfo.
         * @memberof topodata
         * @interface ICellInfo
         * @property {string|null} [server_address] CellInfo server_address
         * @property {string|null} [root] CellInfo root
         */

        /**
         * Constructs a new CellInfo.
         * @memberof topodata
         * @classdesc Represents a CellInfo.
         * @implements ICellInfo
         * @constructor
         * @param {topodata.ICellInfo=} [properties] Properties to set
         */
        function CellInfo(properties) {
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * CellInfo server_address.
         * @member {string} server_address
         * @memberof topodata.CellInfo
         * @instance
         */
        CellInfo.prototype.server_address = '';

        /**
         * CellInfo root.
         * @member {string} root
         * @memberof topodata.CellInfo
         * @instance
         */
        CellInfo.prototype.root = '';

        /**
         * Creates a new CellInfo instance using the specified properties.
         * @function create
         * @memberof topodata.CellInfo
         * @static
         * @param {topodata.ICellInfo=} [properties] Properties to set
         * @returns {topodata.CellInfo} CellInfo instance
         */
        CellInfo.create = function create(properties) {
            return new CellInfo(properties);
        };

        /**
         * Encodes the specified CellInfo message. Does not implicitly {@link topodata.CellInfo.verify|verify} messages.
         * @function encode
         * @memberof topodata.CellInfo
         * @static
         * @param {topodata.ICellInfo} message CellInfo message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        CellInfo.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.server_address != null && Object.hasOwnProperty.call(message, 'server_address'))
                writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.server_address);
            if (message.root != null && Object.hasOwnProperty.call(message, 'root'))
                writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.root);
            return writer;
        };

        /**
         * Encodes the specified CellInfo message, length delimited. Does not implicitly {@link topodata.CellInfo.verify|verify} messages.
         * @function encodeDelimited
         * @memberof topodata.CellInfo
         * @static
         * @param {topodata.ICellInfo} message CellInfo message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        CellInfo.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a CellInfo message from the specified reader or buffer.
         * @function decode
         * @memberof topodata.CellInfo
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {topodata.CellInfo} CellInfo
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        CellInfo.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.topodata.CellInfo();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        message.server_address = reader.string();
                        break;
                    case 2:
                        message.root = reader.string();
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a CellInfo message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof topodata.CellInfo
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {topodata.CellInfo} CellInfo
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        CellInfo.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a CellInfo message.
         * @function verify
         * @memberof topodata.CellInfo
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        CellInfo.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.server_address != null && message.hasOwnProperty('server_address'))
                if (!$util.isString(message.server_address)) return 'server_address: string expected';
            if (message.root != null && message.hasOwnProperty('root'))
                if (!$util.isString(message.root)) return 'root: string expected';
            return null;
        };

        /**
         * Creates a CellInfo message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof topodata.CellInfo
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {topodata.CellInfo} CellInfo
         */
        CellInfo.fromObject = function fromObject(object) {
            if (object instanceof $root.topodata.CellInfo) return object;
            var message = new $root.topodata.CellInfo();
            if (object.server_address != null) message.server_address = String(object.server_address);
            if (object.root != null) message.root = String(object.root);
            return message;
        };

        /**
         * Creates a plain object from a CellInfo message. Also converts values to other types if specified.
         * @function toObject
         * @memberof topodata.CellInfo
         * @static
         * @param {topodata.CellInfo} message CellInfo
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        CellInfo.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.defaults) {
                object.server_address = '';
                object.root = '';
            }
            if (message.server_address != null && message.hasOwnProperty('server_address'))
                object.server_address = message.server_address;
            if (message.root != null && message.hasOwnProperty('root')) object.root = message.root;
            return object;
        };

        /**
         * Converts this CellInfo to JSON.
         * @function toJSON
         * @memberof topodata.CellInfo
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        CellInfo.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return CellInfo;
    })();

    topodata.CellsAlias = (function () {
        /**
         * Properties of a CellsAlias.
         * @memberof topodata
         * @interface ICellsAlias
         * @property {Array.<string>|null} [cells] CellsAlias cells
         */

        /**
         * Constructs a new CellsAlias.
         * @memberof topodata
         * @classdesc Represents a CellsAlias.
         * @implements ICellsAlias
         * @constructor
         * @param {topodata.ICellsAlias=} [properties] Properties to set
         */
        function CellsAlias(properties) {
            this.cells = [];
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * CellsAlias cells.
         * @member {Array.<string>} cells
         * @memberof topodata.CellsAlias
         * @instance
         */
        CellsAlias.prototype.cells = $util.emptyArray;

        /**
         * Creates a new CellsAlias instance using the specified properties.
         * @function create
         * @memberof topodata.CellsAlias
         * @static
         * @param {topodata.ICellsAlias=} [properties] Properties to set
         * @returns {topodata.CellsAlias} CellsAlias instance
         */
        CellsAlias.create = function create(properties) {
            return new CellsAlias(properties);
        };

        /**
         * Encodes the specified CellsAlias message. Does not implicitly {@link topodata.CellsAlias.verify|verify} messages.
         * @function encode
         * @memberof topodata.CellsAlias
         * @static
         * @param {topodata.ICellsAlias} message CellsAlias message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        CellsAlias.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.cells != null && message.cells.length)
                for (var i = 0; i < message.cells.length; ++i)
                    writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.cells[i]);
            return writer;
        };

        /**
         * Encodes the specified CellsAlias message, length delimited. Does not implicitly {@link topodata.CellsAlias.verify|verify} messages.
         * @function encodeDelimited
         * @memberof topodata.CellsAlias
         * @static
         * @param {topodata.ICellsAlias} message CellsAlias message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        CellsAlias.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a CellsAlias message from the specified reader or buffer.
         * @function decode
         * @memberof topodata.CellsAlias
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {topodata.CellsAlias} CellsAlias
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        CellsAlias.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.topodata.CellsAlias();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 2:
                        if (!(message.cells && message.cells.length)) message.cells = [];
                        message.cells.push(reader.string());
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a CellsAlias message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof topodata.CellsAlias
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {topodata.CellsAlias} CellsAlias
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        CellsAlias.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a CellsAlias message.
         * @function verify
         * @memberof topodata.CellsAlias
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        CellsAlias.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.cells != null && message.hasOwnProperty('cells')) {
                if (!Array.isArray(message.cells)) return 'cells: array expected';
                for (var i = 0; i < message.cells.length; ++i)
                    if (!$util.isString(message.cells[i])) return 'cells: string[] expected';
            }
            return null;
        };

        /**
         * Creates a CellsAlias message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof topodata.CellsAlias
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {topodata.CellsAlias} CellsAlias
         */
        CellsAlias.fromObject = function fromObject(object) {
            if (object instanceof $root.topodata.CellsAlias) return object;
            var message = new $root.topodata.CellsAlias();
            if (object.cells) {
                if (!Array.isArray(object.cells)) throw TypeError('.topodata.CellsAlias.cells: array expected');
                message.cells = [];
                for (var i = 0; i < object.cells.length; ++i) message.cells[i] = String(object.cells[i]);
            }
            return message;
        };

        /**
         * Creates a plain object from a CellsAlias message. Also converts values to other types if specified.
         * @function toObject
         * @memberof topodata.CellsAlias
         * @static
         * @param {topodata.CellsAlias} message CellsAlias
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        CellsAlias.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.arrays || options.defaults) object.cells = [];
            if (message.cells && message.cells.length) {
                object.cells = [];
                for (var j = 0; j < message.cells.length; ++j) object.cells[j] = message.cells[j];
            }
            return object;
        };

        /**
         * Converts this CellsAlias to JSON.
         * @function toJSON
         * @memberof topodata.CellsAlias
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        CellsAlias.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return CellsAlias;
    })();

    return topodata;
})();

$root.vttime = (function () {
    /**
     * Namespace vttime.
     * @exports vttime
     * @namespace
     */
    var vttime = {};

    vttime.Time = (function () {
        /**
         * Properties of a Time.
         * @memberof vttime
         * @interface ITime
         * @property {number|Long|null} [seconds] Time seconds
         * @property {number|null} [nanoseconds] Time nanoseconds
         */

        /**
         * Constructs a new Time.
         * @memberof vttime
         * @classdesc Represents a Time.
         * @implements ITime
         * @constructor
         * @param {vttime.ITime=} [properties] Properties to set
         */
        function Time(properties) {
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
        }

        /**
         * Time seconds.
         * @member {number|Long} seconds
         * @memberof vttime.Time
         * @instance
         */
        Time.prototype.seconds = $util.Long ? $util.Long.fromBits(0, 0, false) : 0;

        /**
         * Time nanoseconds.
         * @member {number} nanoseconds
         * @memberof vttime.Time
         * @instance
         */
        Time.prototype.nanoseconds = 0;

        /**
         * Creates a new Time instance using the specified properties.
         * @function create
         * @memberof vttime.Time
         * @static
         * @param {vttime.ITime=} [properties] Properties to set
         * @returns {vttime.Time} Time instance
         */
        Time.create = function create(properties) {
            return new Time(properties);
        };

        /**
         * Encodes the specified Time message. Does not implicitly {@link vttime.Time.verify|verify} messages.
         * @function encode
         * @memberof vttime.Time
         * @static
         * @param {vttime.ITime} message Time message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        Time.encode = function encode(message, writer) {
            if (!writer) writer = $Writer.create();
            if (message.seconds != null && Object.hasOwnProperty.call(message, 'seconds'))
                writer.uint32(/* id 1, wireType 0 =*/ 8).int64(message.seconds);
            if (message.nanoseconds != null && Object.hasOwnProperty.call(message, 'nanoseconds'))
                writer.uint32(/* id 2, wireType 0 =*/ 16).int32(message.nanoseconds);
            return writer;
        };

        /**
         * Encodes the specified Time message, length delimited. Does not implicitly {@link vttime.Time.verify|verify} messages.
         * @function encodeDelimited
         * @memberof vttime.Time
         * @static
         * @param {vttime.ITime} message Time message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        Time.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a Time message from the specified reader or buffer.
         * @function decode
         * @memberof vttime.Time
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {vttime.Time} Time
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        Time.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length,
                message = new $root.vttime.Time();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                    case 1:
                        message.seconds = reader.int64();
                        break;
                    case 2:
                        message.nanoseconds = reader.int32();
                        break;
                    default:
                        reader.skipType(tag & 7);
                        break;
                }
            }
            return message;
        };

        /**
         * Decodes a Time message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof vttime.Time
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {vttime.Time} Time
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        Time.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader)) reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a Time message.
         * @function verify
         * @memberof vttime.Time
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        Time.verify = function verify(message) {
            if (typeof message !== 'object' || message === null) return 'object expected';
            if (message.seconds != null && message.hasOwnProperty('seconds'))
                if (
                    !$util.isInteger(message.seconds) &&
                    !(message.seconds && $util.isInteger(message.seconds.low) && $util.isInteger(message.seconds.high))
                )
                    return 'seconds: integer|Long expected';
            if (message.nanoseconds != null && message.hasOwnProperty('nanoseconds'))
                if (!$util.isInteger(message.nanoseconds)) return 'nanoseconds: integer expected';
            return null;
        };

        /**
         * Creates a Time message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof vttime.Time
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {vttime.Time} Time
         */
        Time.fromObject = function fromObject(object) {
            if (object instanceof $root.vttime.Time) return object;
            var message = new $root.vttime.Time();
            if (object.seconds != null)
                if ($util.Long) (message.seconds = $util.Long.fromValue(object.seconds)).unsigned = false;
                else if (typeof object.seconds === 'string') message.seconds = parseInt(object.seconds, 10);
                else if (typeof object.seconds === 'number') message.seconds = object.seconds;
                else if (typeof object.seconds === 'object')
                    message.seconds = new $util.LongBits(
                        object.seconds.low >>> 0,
                        object.seconds.high >>> 0
                    ).toNumber();
            if (object.nanoseconds != null) message.nanoseconds = object.nanoseconds | 0;
            return message;
        };

        /**
         * Creates a plain object from a Time message. Also converts values to other types if specified.
         * @function toObject
         * @memberof vttime.Time
         * @static
         * @param {vttime.Time} message Time
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        Time.toObject = function toObject(message, options) {
            if (!options) options = {};
            var object = {};
            if (options.defaults) {
                if ($util.Long) {
                    var long = new $util.Long(0, 0, false);
                    object.seconds =
                        options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
                } else object.seconds = options.longs === String ? '0' : 0;
                object.nanoseconds = 0;
            }
            if (message.seconds != null && message.hasOwnProperty('seconds'))
                if (typeof message.seconds === 'number')
                    object.seconds = options.longs === String ? String(message.seconds) : message.seconds;
                else
                    object.seconds =
                        options.longs === String
                            ? $util.Long.prototype.toString.call(message.seconds)
                            : options.longs === Number
                            ? new $util.LongBits(message.seconds.low >>> 0, message.seconds.high >>> 0).toNumber()
                            : message.seconds;
            if (message.nanoseconds != null && message.hasOwnProperty('nanoseconds'))
                object.nanoseconds = message.nanoseconds;
            return object;
        };

        /**
         * Converts this Time to JSON.
         * @function toJSON
         * @memberof vttime.Time
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        Time.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return Time;
    })();

    return vttime;
})();

module.exports = $root;
