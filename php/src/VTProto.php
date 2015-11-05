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
 *
 * You can pass in a signed int value, in which case the re-interpreted
 * 64-bit 2's complement value will be sent as an unsigned int.
 * For example:
 * new VTUnsignedInt(42) // will send 42
 * new VTUnsignedInt(-1) // will send 0xFFFFFFFFFFFFFFFF
 *
 * You can also pass in a string consisting of only decimal digits.
 * For example:
 * new VTUnsignedInt('12345') // will send 12345
 */
class VTUnsignedInt {
	private $value;

	public function __construct($value) {
		if (is_int($value)) {
			$this->value = $value;
		} else if (is_string($value)) {
			if (! ctype_digit($value)) {
				throw new VTBadInputException('Invalid string value given for VTUnsignedInt: ' . $value);
			}
			$this->value = $value;
		} else {
			throw new VTBadInputException('Unsupported type for VTUnsignedInt');
		}
	}

	public function __toString() {
		if (is_int($this->value)) {
			return sprintf('%u', $this->value);
		} else {
			return strval($this->value);
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
		
		if (is_array($value)) {
			if (count($value) == 0) {
				throw new VTBadInputException('Empty list not allowed for list bind variable');
			}
			
			$bind_var->setType(\query\Type::TUPLE);
			
			foreach ($value as $elem) {
				list ( $type, $tval ) = self::TypedValue($elem);
				$bind_var->addValues((new \query\Value())->setType($type)->setValue($tval));
			}
		} else {
			list ( $type, $tval ) = self::TypedValue($value);
			$bind_var->setType($type);
			$bind_var->setValue($tval);
		}
		
		return $bind_var;
	}

	/**
	 * Returns a tuple of detected \query\Type and string value compatible with \query\Value.
	 */
	protected static function TypedValue($value) {
		if (is_null($value)) {
			return array(
					\query\Type::NULL_TYPE,
					'' 
			);
		} else if (is_string($value)) {
			return array(
					\query\Type::VARBINARY,
					$value 
			);
		} else if (is_int($value)) {
			return array(
					\query\Type::INT64,
					strval($value) 
			);
		} else if (is_float($value)) {
			return array(
					\query\Type::FLOAT64,
					strval($value) 
			);
		} else if (is_object($value)) {
			switch (get_class($value)) {
				case 'VTUnsignedInt':
					return array(
							\query\Type::UINT64,
							strval($value) 
					);
				default:
					throw new VTBadInputException('Unknown \query\Value variable class: ' . get_class($value));
			}
		} else {
			throw new VTBadInputException('Unknown type for \query\Value proto:' . gettype($value));
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
		
		list ( $type, $tval ) = self::TypedValue($value);
		$eid->setXidType($type);
		$eid->setXidValue($tval);
		
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

	public function RowValues($row) {
		$values = array();
		
		// Row values are packed into a single buffer.
		// See the docs for the Row message in query.proto.
		$start = 0;
		$buf = $row->getValues();
		$lengths = $row->getLengths();
		foreach ($lengths as $len) {
			if ($len < 0) {
				// This indicates a MySQL NULL value,
				// to distinguish it from a zero-length string.
				$values[] = NULL;
			} else {
				$val = substr($buf, $start, $len);
				if ($val === FALSE || strlen($val) != $len) {
					throw new VTException('Index out of bounds while decoding Row values');
				}
				$values[] = $val;
				$start += $len;
			}
		}
		
		return $values;
	}
}

