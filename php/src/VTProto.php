<?php
require_once (dirname(__FILE__) . '/../vendor/autoload.php');
require_once (dirname(__FILE__) . '/proto/query.php');
require_once (dirname(__FILE__) . '/proto/vtgate.php');
require_once (dirname(__FILE__) . '/proto/topodata.php');
require_once (dirname(__FILE__) . '/proto/vtrpc.php');

/*
 * This module contains helper functions and classes for interacting with the
 * Vitess protobufs.
 */

/**
 * VTUnsignedInt is a wrapper used to tell the Vitess RPC layer that it should
 * encode as unsigned int.
 *
 * This is necessary because PHP doesn't have a real unsigned int type.
 */
class VTUnsignedInt {
	public $value;

	public function __construct($value) {
		if (is_int($value)) {
			$this->value = $value;
		} else {
			throw new VTException('Unsupported type for VTUnsignedInt');
		}
	}
}

class VTProto {

	public static function checkError($response) {
		$error = $response->getError();
		if ($error) {
			switch ($error->getCode()) {
				case \vtrpc\ErrorCode::SUCCESS:
					break;
				case \vtrpc\ErrorCode::BAD_INPUT:
					throw new VTBadInputError($error->getMessage());
				case \vtrpc\ErrorCode::DEADLINE_EXCEEDED:
					throw new VTDeadlineExceededError($error->getMessage());
				case \vtrpc\ErrorCode::INTEGRITY_ERROR:
					throw new VTIntegrityError($error->getMessage());
				case \vtrpc\ErrorCode::TRANSIENT_ERROR:
					throw new VTTransientError($error->getMessage());
				case \vtrpc\ErrorCode::UNAUTHENTICATED:
					throw new VTUnauthenticatedError($error->getMessage());
				default:
					throw new VTException($error->getCode() . ': ' . $error->getMessage());
			}
		}
	}

	public static function BoundQuery($query, $vars) {
		$bound_query = new \query\BoundQuery();
		$bound_query->setSql($query);
		if ($vars) {
			foreach ($vars as $key => $value) {
				$entry = new \query\BoundQuery\BindVariablesEntry();
				$entry->setKey($key);
				$entry->setValue(self::BindVariable($value));
				$bound_query->addBindVariables($entry);
			}
		}
		return $bound_query;
	}

	public static function BindVariable($value) {
		$bind_var = new \query\BindVariable();
		
		if (is_null($value)) {
			$bind_var->setType(\query\BindVariable\Type::TYPE_NULL);
		} else if (is_string($value)) {
			$bind_var->setType(\query\BindVariable\Type::TYPE_BYTES);
			$bind_var->setValueBytes($value);
		} else if (is_int($value)) {
			$bind_var->setType(\query\BindVariable\Type::TYPE_INT);
			$bind_var->setValueInt($value);
		} else if (is_float($value)) {
			$bind_var->setType(\query\BindVariable\Type::TYPE_FLOAT);
			$bind_var->setValueFloat($value);
		} else if (is_object($value)) {
			switch (get_class($value)) {
				case 'VTUnsignedInt':
					$bind_var->setType(\query\BindVariable\Type::TYPE_UINT);
					$bind_var->setValueUint($value->value);
					break;
				default:
					throw new VTException('Unknown bind variable class: ' . get_class($value));
			}
		} else if (is_array($value)) {
			self::ListBindVariable($bind_var, $value);
		} else {
			throw new VTException('Unknown bind variable type.');
		}
		
		return $bind_var;
	}

	protected static function ListBindVariable(&$bind_var, array $list) {
		if (count($list) == 0) {
			// The list is empty, so it has no type. VTTablet will reject an empty
			// list anyway, so we'll just pretend it was a list of bytes.
			$bind_var->setType(\query\BindVariable\Type::TYPE_BYTES_LIST);
			return;
		}
		
		// Check type of first item to determine type of list.
		// We only support lists whose elements have uniform types.
		if (is_string($list[0])) {
			$bind_var->setType(\query\BindVariable\Type::TYPE_BYTES_LIST);
			$bind_var->setValueBytesList($list);
		} else if (is_int($list[0])) {
			$bind_var->setType(\query\BindVariable\Type::TYPE_INT_LIST);
			$bind_var->setValueIntList($list);
		} else if (is_float($list[0])) {
			$bind_var->setType(\query\BindVariable\Type::TYPE_FLOAT_LIST);
			$bind_var->setValueFloatList($list);
		} else if (is_object($list[0])) {
			switch (get_class($list[0])) {
				case 'VTUnsignedInt':
					$bind_var->setType(\query\BindVariable\Type::TYPE_UINT_LIST);
					$value = array();
					foreach ($list as $val) {
						$value[] = $val->value;
					}
					$bind_var->setValueUintList($value);
					break;
				default:
					throw new VTException('Unknown list bind variable class: ' . get_class($list[0]));
			}
		} else {
			throw new VTException('Unknown list bind variable type.');
		}
	}

	public static function addQueries($proto, $queries) {
		foreach ($queries as $query) {
			$proto->addQueries($query);
		}
	}

	public static function KeyspaceIdFromHex($hex) {
		return pack('H*', $hex);
	}

	public static function KeyRangeFromHex($start, $end) {
		$value = new \topodata\KeyRange();
		$value->setStart(self::KeyspaceIdFromHex($start));
		$value->setEnd(self::KeyspaceIdFromHex($end));
		return $value;
	}

	public static function addKeyRanges($proto, $key_ranges) {
		foreach ($key_ranges as $key_range) {
			$proto->addKeyRanges($key_range);
		}
	}

	public static function EntityId($keyspace_id, $value) {
		$eid = new \vtgate\ExecuteEntityIdsRequest\EntityId();
		$eid->setKeyspaceId($keyspace_id);
		
		if (is_string($value)) {
			$eid->setXidType(\vtgate\ExecuteEntityIdsRequest\EntityId\Type::TYPE_BYTES);
			$eid->setXidBytes($value);
		} else if (is_int($value)) {
			$eid->setXidType(\vtgate\ExecuteEntityIdsRequest\EntityId\Type::TYPE_INT);
			$eid->setXidInt($value);
		} else if (is_float($value)) {
			$eid->setXidType(\vtgate\ExecuteEntityIdsRequest\EntityId\Type::TYPE_FLOAT);
			$eid->setXidFloat($value);
		} else if (is_object($value)) {
			switch (get_class($value)) {
				case 'VTUnsignedInt':
					$eid->setXidType(\vtgate\ExecuteEntityIdsRequest\EntityId\Type::TYPE_UINT);
					$eid->setXidUint($value->value);
					break;
				default:
					throw new VTException('Unknown entity ID class: ' . get_class($value));
			}
		} else {
			throw new VTException('Unknown entity ID type.');
		}
		
		return $eid;
	}

	public static function addEntityKeyspaceIds($proto, $entity_keyspace_ids) {
		foreach ($entity_keyspace_ids as $keyspace_id => $entity_id) {
			$proto->addEntityKeyspaceIds(self::EntityId($keyspace_id, $entity_id));
		}
	}

	public function BoundShardQuery($query, $bind_vars, $keyspace, $shards) {
		$value = new \vtgate\BoundShardQuery();
		$value->setQuery(self::BoundQuery($query, $bind_vars));
		$value->setKeyspace($keyspace);
		$value->setShards($shards);
		return $value;
	}

	public function BoundKeyspaceIdQuery($query, $bind_vars, $keyspace, $keyspace_ids) {
		$value = new \vtgate\BoundKeyspaceIdQuery();
		$value->setQuery(self::BoundQuery($query, $bind_vars));
		$value->setKeyspace($keyspace);
		$value->setKeyspaceIds($keyspace_ids);
		return $value;
	}
}

