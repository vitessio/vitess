<?php

/*
 * This module is used for interacting with associative arrays that represent
 * the proto3 structures for Vitess. Currently there's no protobuf compiler
 * plugin for PHP that supports maps (a protobuf feature we use), so we can't
 * generate the code needed to connect through gRPC. An official PHP plugin for
 * proto3 is in the works, and the plan is to replace this with generated code
 * when that's ready.
 *
 * In the meantime, we produce associative arrays that encode into a BSON
 * representation that's compatible with our proto3 structures. We then send
 * them over our legacy BSON-based GoRPC+ protocol.
 */

/**
 * VTUnsignedInt is a wrapper used to tell the Vitess RPC layer that it should
 * encode as unsigned int.
 *
 * This is necessary because PHP doesn't have a real unsigned int type.
 */
class VTUnsignedInt {
	public $bsonValue;

	public function __construct($value) {
		if (is_int($value)) {
			$this->bsonValue = $value;
		} else if (is_string($value)) {
			$this->bsonValue = new MongoInt64($value);
		} else {
			throw new Exception('Unsupported type for VTUnsignedInt');
		}
	}
}

class VTTabletType {
	const IDLE = 1;
	const MASTER = 2;
	const REPLICA = 3;
	const RDONLY = 4;
	const SPARE = 5;
	const EXPERIMENTAL = 6;
	const SCHEMA_UPGRADE = 7;
	const BACKUP = 8;
	const RESTORE = 9;
	const WORKER = 10;
	const SCRAP = 11;
}

class VTBindVariable {
	const TYPE_NULL = 0;
	const TYPE_BYTES = 1;
	const TYPE_INT = 2;
	const TYPE_UINT = 3;
	const TYPE_FLOAT = 4;
	const TYPE_BYTES_LIST = 5;
	const TYPE_INT_LIST = 6;
	const TYPE_UINT_LIST = 7;
	const TYPE_FLOAT_LIST = 8;

	/**
	 * buildBsonP3 creates a BindVariable bsonp3 object.
	 */
	public static function buildBsonP3($value) {
		if (is_null($value)) {
			return array(
					'Type' => self::TYPE_NULL 
			);
		} else if (is_string($value)) {
			return array(
					'Type' => self::TYPE_BYTES,
					'ValueBytes' => new MongoBinData($value) 
			);
		} else if (is_int($value)) {
			return array(
					'Type' => self::TYPE_INT,
					'ValueInt' => $value 
			);
		} else if (is_float($value)) {
			return array(
					'Type' => self::TYPE_FLOAT,
					'ValueFloat' => $value 
			);
		} else if (is_object($value)) {
			switch (get_class($value)) {
				case 'VTUnsignedInt':
					return array(
							'Type' => self::TYPE_UINT,
							'ValueUint' => $value->bsonValue 
					);
			}
		}
		// TODO(enisoc): Implement list bind vars.
		
		throw new Exception('Unknown bind variable type.');
	}

	/**
	 * fromBsonP3 returns a PHP object from a bsonp3 BindVariable object.
	 */
	public static function fromBsonP3($bson) {
		if (! array_key_exists('Type', $bson)) {
			return NULL;
		}
		switch ($bson['Type']) {
			case self::TYPE_NULL:
				return NULL;
			case self::TYPE_BYTES:
				return $bson['ValueBytes'];
			case self::TYPE_INT:
				return $bson['ValueInt'];
			case self::TYPE_UINT:
				return new VTUnsignedInt($bson['ValueUint']);
			case self::TYPE_FLOAT:
				return $bson['ValueFloat'];
			case self::TYPE_BYTES_LIST:
				return $bson['ValueBytesList'];
			case self::TYPE_INT_LIST:
				return $bson['ValueIntList'];
			case self::TYPE_UINT_LIST:
				$result = array();
				foreach ($bson['ValueUintList'] as $val) {
					$result[] = new VTUnsignedInt($val);
				}
				return $result;
			case self::TYPE_FLOAT_LIST:
				return $bson['ValueFloatList'];
		}
		
		throw new Exception('Unknown bind variable type.');
	}
}

class VTBoundQuery {
	public $query;
	public $bindVars;

	public function __construct($query = '', $bindVars = array()) {
		$this->query = $query;
		$this->bindVars = $bindVars;
	}

	public static function fromBsonP3($bson) {
		$result = new VTBoundQuery();
		if (array_key_exists('Sql', $bson)) {
			$result->query = $bson['Sql'];
		}
		if (array_key_exists('BindVariables', $bson)) {
			foreach ($bson['BindVariables'] as $key => $val) {
				$result->bindVars[$key] = VTBindVariable::fromBsonP3($val);
			}
		}
		return $result;
	}

	/**
	 * buildBsonP3 creates a BoundQuery bsonp3 object.
	 */
	public static function buildBsonP3($query, $vars) {
		$bindVars = array();
		if ($vars) {
			foreach ($vars as $key => $value) {
				$bindVars[$key] = VTBindVariable::buildBsonP3($value);
			}
		}
		return array(
				'Sql' => $query,
				'BindVariables' => $bindVars 
		);
	}
}

class VTBoundShardQuery {
	public $query;
	public $vars;
	public $keyspace;
	public $shards;

	public function __construct($query, $bind_vars, $keyspace, $shards) {
		$this->query = $query;
		$this->vars = $bind_vars;
		$this->keyspace = $keyspace;
		$this->shards = $shards;
	}

	public function toBsonP3() {
		return array(
				'Query' => VTBoundQuery::buildBsonP3($this->query, $this->vars),
				'Keyspace' => $this->keyspace,
				'Shards' => $this->shards 
		);
	}

	public static function buildBsonP3Array($bound_queries) {
		$result = array();
		foreach ($bound_queries as $bound_query) {
			$result[] = $bound_query->toBsonP3();
		}
		return $result;
	}
}

class VTBoundKeyspaceIdQuery {
	public $query;
	public $vars;
	public $keyspace;
	public $keyspaceIds;

	public function __construct($query, $bind_vars, $keyspace, $keyspace_ids) {
		$this->query = $query;
		$this->vars = $bind_vars;
		$this->keyspace = $keyspace;
		$this->keyspaceIds = $keyspace_ids;
	}

	public function toBsonP3() {
		return array(
				'Query' => VTBoundQuery::buildBsonP3($this->query, $this->vars),
				'Keyspace' => $this->keyspace,
				'KeyspaceIds' => VTKeyspaceId::buildBsonP3Array($this->keyspaceIds) 
		);
	}

	public static function buildBsonP3Array($bound_queries) {
		$result = array();
		foreach ($bound_queries as $bound_query) {
			$result[] = $bound_query->toBsonP3();
		}
		return $result;
	}
}

class VTField {
	const TYPE_DECIMAL = 0;
	const TYPE_TINY = 1;
	const TYPE_SHORT = 2;
	const TYPE_LONG = 3;
	const TYPE_FLOAT = 4;
	const TYPE_DOUBLE = 5;
	const TYPE_NULL = 6;
	const TYPE_TIMESTAMP = 7;
	const TYPE_LONGLONG = 8;
	const TYPE_INT24 = 9;
	const TYPE_DATE = 10;
	const TYPE_TIME = 11;
	const TYPE_DATETIME = 12;
	const TYPE_YEAR = 13;
	const TYPE_NEWDATE = 14;
	const TYPE_VARCHAR = 15;
	const TYPE_BIT = 16;
	const TYPE_NEWDECIMAL = 246;
	const TYPE_ENUM = 247;
	const TYPE_SET = 248;
	const TYPE_TINY_BLOB = 249;
	const TYPE_MEDIUM_BLOB = 250;
	const TYPE_LONG_BLOB = 251;
	const TYPE_BLOB = 252;
	const TYPE_VAR_STRING = 253;
	const TYPE_STRING = 254;
	const TYPE_GEOMETRY = 255;
	const VT_ZEROVALUE_FLAG = 0;
	const VT_NOT_NULL_FLAG = 1;
	const VT_PRI_KEY_FLAG = 2;
	const VT_UNIQUE_KEY_FLAG = 4;
	const VT_MULTIPLE_KEY_FLAG = 8;
	const VT_BLOB_FLAG = 16;
	const VT_UNSIGNED_FLAG = 32;
	const VT_ZEROFILL_FLAG = 64;
	const VT_BINARY_FLAG = 128;
	const VT_ENUM_FLAG = 256;
	const VT_AUTO_INCREMENT_FLAG = 512;
	const VT_TIMESTAMP_FLAG = 1024;
	const VT_SET_FLAG = 2048;
	const VT_NO_DEFAULT_VALUE_FLAG = 4096;
	const VT_ON_UPDATE_NOW_FLAG = 8192;
	const VT_NUM_FLAG = 32768;
	public $name = '';
	public $type = 0;
	public $flags = 0;

	public static function fromBsonP3($bson) {
		$result = new VTField();
		if (array_key_exists('Name', $bson)) {
			$result->name = $bson['Name'];
		}
		if (array_key_exists('Type', $bson)) {
			$result->type = $bson['Type'];
		}
		if (array_key_exists('Flags', $bson)) {
			$result->flags = $bson['Flags'];
		}
		return $result;
	}
}

class VTQueryResult {
	public $fields = array();
	public $rowsAffected = 0;
	public $insertId = 0;
	public $rows = array();

	public static function fromBsonP3($bson) {
		$result = new VTQueryResult();
		if (array_key_exists('Fields', $bson)) {
			foreach ($bson['Fields'] as $field) {
				$result->fields[] = VTField::fromBsonP3($field);
			}
		}
		if (array_key_exists('RowsAffected', $bson)) {
			$result->rowsAffected = $bson['RowsAffected'];
		}
		if (array_key_exists('InsertId', $bson)) {
			$result->insertId = $bson['InsertId'];
		}
		if (array_key_exists('Rows', $bson)) {
			foreach ($bson['Rows'] as $row) {
				$result->rows[] = $row['Values'];
			}
		}
		return $result;
	}
}

class VTProto {

	public static function checkError($resp) {
		// TODO(enisoc): Implement app-level error checking.
	}
}

class VTCallerId {
	public $principal;
	public $component;
	public $subcomponent;

	public function __construct($principal, $component, $subcomponent) {
		$this->principal = $principal;
		$this->component = $component;
		$this->subcomponent = $subcomponent;
	}

	public function toBsonP3() {
		return array(
				'Principal' => $this->principal,
				'Component' => $this->component,
				'Subcomponent' => $this->subcomponent 
		);
	}
}

class VTKeyRange {

	public static function buildBsonP3Array($key_ranges) {
		$result = array();
		foreach ($key_ranges as $key_range) {
			$result[] = array(
					'Start' => new MongoBinData($key_range[0]),
					'End' => new MongoBinData($key_range[1]) 
			);
		}
		return $result;
	}

	public static function fromBsonP3($bson) {
		$result = array(
				'',
				'' 
		);
		if (array_key_exists('Start', $bson)) {
			$result[0] = $bson['Start'];
		}
		if (array_key_exists('End', $bson)) {
			$result[1] = $bson['End'];
		}
		return $result;
	}
}

class VTKeyspaceId {

	public static function fromHex($hex) {
		return pack('H*', $hex);
	}

	public static function buildBsonP3Array($keyspace_ids) {
		$result = array();
		foreach ($keyspace_ids as $kid) {
			$result[] = new MongoBinData($kid);
		}
		return $result;
	}
}

class VTEntityId {
	const TYPE_BYTES = 1;
	const TYPE_INT = 2;
	const TYPE_UINT = 3;
	const TYPE_FLOAT = 4;

	public static function buildBsonP3($entity_id) {
		if (is_string($entity_id)) {
			return array(
					'XidType' => self::TYPE_BYTES,
					'XidBytes' => new MongoBinData($entity_id) 
			);
		} else if (is_int($entity_id)) {
			return array(
					'XidType' => self::TYPE_INT,
					'XidInt' => $entity_id 
			);
		} else if (is_float($entity_id)) {
			return array(
					'XidType' => self::TYPE_FLOAT,
					'XidFloat' => $entity_id 
			);
		} else if (is_object($entity_id)) {
			switch (get_class($entity_id)) {
				case 'VTUnsignedInt':
					return array(
							'XidType' => self::TYPE_UINT,
							'XidUint' => $entity_id->bsonValue 
					);
			}
		}
		
		throw new Exception('Unknown entity ID type.');
	}

	public static function buildBsonP3Array($entity_keyspace_ids) {
		$result = array();
		foreach ($entity_keyspace_ids as $keyspace_id => $entity_id) {
			$eid = self::buildBsonP3($entity_id);
			$eid['KeyspaceId'] = new MongoBinData($keyspace_id);
			$result[] = $eid;
		}
		return $result;
	}
}

class VTSplitQueryKeyRangePart {
	public $keyspace = '';
	public $keyRanges = array();

	public static function fromBsonP3($bson) {
		$result = new VTSplitQueryKeyRangePart();
		if (array_key_exists('Keyspace', $bson)) {
			$result->keyspace = $bson['Keyspace'];
		}
		if (array_key_exists('KeyRanges', $bson) && $bson['KeyRanges']) {
			foreach ($bson['KeyRanges'] as $val) {
				$result->keyRanges[] = VTKeyRange::fromBsonP3($val);
			}
		}
		return $result;
	}
}

class VTSplitQueryShardPart {
	public $keyspace = '';
	public $shards = array();

	public static function fromBsonP3($bson) {
		$result = new VTSplitQueryShardPart();
		if (array_key_exists('Keyspace', $bson)) {
			$result->keyspace = $bson['Keyspace'];
		}
		if (array_key_exists('Shards', $bson)) {
			$result->shards = $bson['Shards'];
		}
		return $result;
	}
}

class VTSplitQueryPart {
	public $query = '';
	public $keyRangePart;
	public $shardPart;
	public $size = 0;

	public static function fromBsonP3($bson) {
		$result = new VTSplitQueryPart();
		if (array_key_exists('Query', $bson)) {
			$result->query = VTBoundQuery::fromBsonP3($bson['Query']);
		}
		if (array_key_exists('KeyRangePart', $bson) && $bson['KeyRangePart']) {
			$result->keyRangePart = VTSplitQueryKeyRangePart::fromBsonP3($bson['KeyRangePart']);
		}
		if (array_key_exists('ShardPart', $bson) && $bson['ShardPart']) {
			$result->shardPart = VTSplitQueryShardPart::fromBsonP3($bson['ShardPart']);
		}
		if (array_key_exists('Size', $bson)) {
			$result->size = $bson['Size'];
		}
		return $result;
	}
}
