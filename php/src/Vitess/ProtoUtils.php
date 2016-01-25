<?php
namespace Vitess;

/*
 * This module contains helper functions and classes for interacting with the
 * Vitess protobufs.
 */
class ProtoUtils
{

    public static function checkError($response)
    {
        $error = $response->getError();
        if ($error) {
            switch ($error->getCode()) {
                case Proto\Vtrpc\ErrorCode::SUCCESS:
                    break;
                case Proto\Vtrpc\ErrorCode::BAD_INPUT:
                    throw new Error\BadInput($error->getMessage());
                case Proto\Vtrpc\ErrorCode::DEADLINE_EXCEEDED:
                    throw new Error\DeadlineExceeded($error->getMessage());
                case Proto\Vtrpc\ErrorCode::INTEGRITY_ERROR:
                    throw new Error\Integrity($error->getMessage());
                case Proto\Vtrpc\ErrorCode::TRANSIENT_ERROR:
                    throw new Error\Transient($error->getMessage());
                case Proto\Vtrpc\ErrorCode::UNAUTHENTICATED:
                    throw new Error\Unauthenticated($error->getMessage());
                default:
                    throw new \Vitess\Exception($error->getCode() . ': ' . $error->getMessage());
            }
        }
    }

    public static function BoundQuery($query, $vars)
    {
        $bound_query = new Proto\Query\BoundQuery();
        $bound_query->setSql($query);
        if ($vars) {
            foreach ($vars as $key => $value) {
                $entry = new Proto\Query\BoundQuery\BindVariablesEntry();
                $entry->setKey($key);
                $entry->setValue(self::BindVariable($value));
                $bound_query->addBindVariables($entry);
            }
        }
        return $bound_query;
    }

    public static function BindVariable($value)
    {
        $bind_var = new Proto\Query\BindVariable();

        if (is_array($value)) {
            if (count($value) == 0) {
                throw new Error\BadInput('Empty list not allowed for list bind variable');
            }

            $bind_var->setType(Proto\Query\Type::TUPLE);

            foreach ($value as $elem) {
                list ($type, $tval) = self::TypedValue($elem);
                $bind_var->addValues((new Proto\Query\Value())->setType($type)
                    ->setValue($tval));
            }
        } else {
            list ($type, $tval) = self::TypedValue($value);
            $bind_var->setType($type);
            $bind_var->setValue($tval);
        }

        return $bind_var;
    }

    /**
     * Returns a tuple of detected Proto\Query\Type and string value compatible with Proto\Query\Value.
     */
    protected static function TypedValue($value)
    {
        if (is_null($value)) {
            return array(
                Proto\Query\Type::NULL_TYPE,
                ''
            );
        } else
            if (is_string($value)) {
                return array(
                    Proto\Query\Type::VARBINARY,
                    $value
                );
            } else
                if (is_int($value)) {
                    return array(
                        Proto\Query\Type::INT64,
                        strval($value)
                    );
                } else
                    if (is_float($value)) {
                        return array(
                            Proto\Query\Type::FLOAT64,
                            strval($value)
                        );
                    } else
                        if (is_object($value)) {
                            switch (get_class($value)) {
                                case 'Vitess\UnsignedInt':
                                    return array(
                                        Proto\Query\Type::UINT64,
                                        strval($value)
                                    );
                                default:
                                    throw new Error\BadInput('Unknown Proto\Query\Value variable class: ' . get_class($value));
                            }
                        } else {
                            throw new Error\BadInput('Unknown type for Proto\Query\Value proto:' . gettype($value));
                        }
    }

    public static function addQueries($proto, $queries)
    {
        foreach ($queries as $query) {
            $proto->addQueries($query);
        }
    }

    public static function KeyspaceIdFromHex($hex)
    {
        return pack('H*', $hex);
    }

    public static function KeyRangeFromHex($start, $end)
    {
        $value = new Proto\Topodata\KeyRange();
        $value->setStart(self::KeyspaceIdFromHex($start));
        $value->setEnd(self::KeyspaceIdFromHex($end));
        return $value;
    }

    public static function addKeyRanges($proto, $key_ranges)
    {
        foreach ($key_ranges as $key_range) {
            $proto->addKeyRanges($key_range);
        }
    }

    public static function EntityId($keyspace_id, $value)
    {
        $eid = new Proto\Vtgate\ExecuteEntityIdsRequest\EntityId();
        $eid->setKeyspaceId($keyspace_id);

        list ($type, $tval) = self::TypedValue($value);
        $eid->setXidType($type);
        $eid->setXidValue($tval);

        return $eid;
    }

    public static function addEntityKeyspaceIds($proto, $entity_keyspace_ids)
    {
        foreach ($entity_keyspace_ids as $keyspace_id => $entity_id) {
            $proto->addEntityKeyspaceIds(self::EntityId($keyspace_id, $entity_id));
        }
    }

    public function BoundShardQuery($query, $bind_vars, $keyspace, $shards)
    {
        $value = new Proto\Vtgate\BoundShardQuery();
        $value->setQuery(self::BoundQuery($query, $bind_vars));
        $value->setKeyspace($keyspace);
        $value->setShards($shards);
        return $value;
    }

    public function BoundKeyspaceIdQuery($query, $bind_vars, $keyspace, $keyspace_ids)
    {
        $value = new Proto\Vtgate\BoundKeyspaceIdQuery();
        $value->setQuery(self::BoundQuery($query, $bind_vars));
        $value->setKeyspace($keyspace);
        $value->setKeyspaceIds($keyspace_ids);
        return $value;
    }

    public function RowValues($row)
    {
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
                    throw new \Vitess\Exception('Index out of bounds while decoding Row values');
                }
                $values[] = $val;
                $start += $len;
            }
        }

        return $values;
    }
}

