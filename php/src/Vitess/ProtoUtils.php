<?php
namespace Vitess;

use Vitess\Proto\Vtrpc\Code;
use Vitess\Proto\Vtrpc\LegacyErrorCode;
use Vitess\Proto\Query;
use Vitess\Proto\Query\Type;
use Vitess\Proto\Query\BoundQuery;
use Vitess\Proto\Topodata\KeyRange;
use Vitess\Proto\Vtgate\ExecuteEntityIdsRequest\EntityId;
use Vitess\Proto\Vtgate\BoundShardQuery;
use Vitess\Proto\Vtgate\BoundKeyspaceIdQuery;

/**
 * This module contains helper functions and classes for interacting with the
 * Vitess protobufs.
 */
class ProtoUtils
{

    /**
     * Throws the appropriate exception for the "partial error" in a response.
     *
     * @param mixed $response
     *            any protobuf response message that may have a "partial error"
     *
     * @throws Error\BadInput
     * @throws Error\DeadlineExceeded
     * @throws Error\Integrity
     * @throws Error\Transient
     * @throws Error\Unauthenticated
     * @throws Error\Aborted
     * @throws Exception
     */
    public static function checkError($response)
    {
        $error = $response->getError();
        if ($error) {
            switch ($error->getCode()) {
                case Code::OK:
                    break;
                case Code::INVALID_ARGUMENT:
                    throw new Error\BadInput($error->getMessage());
                case Code::DEADLINE_EXCEEDED:
                    throw new Error\DeadlineExceeded($error->getMessage());
                case Code::ALREADY_EXISTS:
                    throw new Error\Integrity($error->getMessage());
                case Code::UNAVAILABLE:
                    throw new Error\Transient($error->getMessage());
                case Code::UNAUTHENTICATED:
                    throw new Error\Unauthenticated($error->getMessage());
                case Code::ABORTED:
                    throw new Error\Aborted($error->getMessage());
                default:
                    throw new Exception($error->getCode() . ': ' . $error->getMessage());
            }
            switch ($error->getLegacyCode()) {
                case LegacyErrorCode::SUCCESS_LEGACY:
                    break;
                case LegacyErrorCode::BAD_INPUT_LEGACY:
                    throw new Error\BadInput($error->getMessage());
                case LegacyErrorCode::DEADLINE_EXCEEDED_LEGACY:
                    throw new Error\DeadlineExceeded($error->getMessage());
                case LegacyErrorCode::INTEGRITY_ERROR_LEGACY:
                    throw new Error\Integrity($error->getMessage());
                case LegacyErrorCode::TRANSIENT_ERROR_LEGACY:
                    throw new Error\Transient($error->getMessage());
                case LegacyErrorCode::UNAUTHENTICATED_LEGACY:
                    throw new Error\Unauthenticated($error->getMessage());
                case LegacyErrorCode::NOT_IN_TX_LEGACY:
                    throw new Error\Aborted($error->getMessage());
                default:
                    throw new Exception($error->getCode() . ': ' . $error->getMessage());
            }
        }
    }

    /**
     *
     * @param string $query
     * @param array $vars
     *
     * @return BoundQuery
     * @throws Error\BadInput
     */
    public static function BoundQuery($query, array $vars)
    {
        $bound_query = new BoundQuery();
        $bound_query->setSql($query);
        if ($vars) {
            foreach ($vars as $key => $value) {
                $entry = new BoundQuery\BindVariablesEntry();
                $entry->setKey($key);
                $entry->setValue(self::BindVariable($value));
                $bound_query->addBindVariables($entry);
            }
        }
        return $bound_query;
    }

    /**
     *
     * @param mixed $value
     *
     * @return Query\BindVariable
     * @throws Error\BadInput
     */
    public static function BindVariable($value)
    {
        $bind_var = new Query\BindVariable();

        if (is_array($value)) {
            if (count($value) == 0) {
                throw new Error\BadInput('Empty list not allowed for list bind variable');
            }

            $bind_var->setType(Query\Type::TUPLE);

            foreach ($value as $elem) {
                list ($type, $tval) = self::TypedValue($elem);
                $bind_var->addValues((new Query\Value())->setType($type)
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
     *
     * @param mixed $value
     *
     * @return array
     * @throws Error\BadInput
     */
    protected static function TypedValue($value)
    {
        if (is_null($value)) {
            return array(
                Type::NULL_TYPE,
                ''
            );
        } else
            if (is_string($value)) {
                return array(
                    Type::VARBINARY,
                    $value
                );
            } else
                if (is_int($value)) {
                    return array(
                        Type::INT64,
                        strval($value)
                    );
                } else
                    if (is_float($value)) {
                        return array(
                            Type::FLOAT64,
                            strval($value)
                        );
                    } else
                        if (is_object($value)) {
                            switch (get_class($value)) {
                                case 'Vitess\UnsignedInt':
                                    return array(
                                        Type::UINT64,
                                        strval($value)
                                    );
                                default:
                                    throw new Error\BadInput('Unknown Proto\Query\Value variable class: ' . get_class($value));
                            }
                        } else {
                            throw new Error\BadInput('Unknown type for Proto\Query\Value proto:' . gettype($value));
                        }
    }

    /**
     *
     * @param mixed $proto
     * @param array $queries
     */
    public static function addQueries($proto, array $queries)
    {
        foreach ($queries as $query) {
            $proto->addQueries($query);
        }
    }

    /**
     *
     * @param string $hex
     *
     * @return string
     */
    public static function KeyspaceIdFromHex($hex)
    {
        return pack('H*', $hex);
    }

    /**
     *
     * @param string $start
     * @param string $end
     *
     * @return KeyRange
     */
    public static function KeyRangeFromHex($start, $end)
    {
        $value = new KeyRange();
        $value->setStart(self::KeyspaceIdFromHex($start));
        $value->setEnd(self::KeyspaceIdFromHex($end));
        return $value;
    }

    /**
     *
     * @param mixed $proto
     * @param array $key_ranges
     */
    public static function addKeyRanges($proto, array $key_ranges)
    {
        foreach ($key_ranges as $key_range) {
            $proto->addKeyRanges($key_range);
        }
    }

    /**
     *
     * @param string $keyspace_id
     * @param mixed $value
     *
     * @return EntityId
     * @throws Error\BadInput
     */
    public static function EntityId($keyspace_id, $value)
    {
        $eid = new EntityId();
        $eid->setKeyspaceId($keyspace_id);

        list ($type, $tval) = self::TypedValue($value);
        $eid->setType($type);
        $eid->setValue($tval);

        return $eid;
    }

    /**
     *
     * @param mixed $proto
     * @param array $entity_keyspace_ids
     */
    public static function addEntityKeyspaceIds($proto, array $entity_keyspace_ids)
    {
        foreach ($entity_keyspace_ids as $keyspace_id => $entity_id) {
            $proto->addEntityKeyspaceIds(self::EntityId($keyspace_id, $entity_id));
        }
    }

    /**
     *
     * @param string $query
     * @param mixed $bind_vars
     * @param string $keyspace
     * @param mixed $shards
     *
     * @return BoundShardQuery
     */
    public function BoundShardQuery($query, $bind_vars, $keyspace, $shards)
    {
        $value = new BoundShardQuery();
        $value->setQuery(self::BoundQuery($query, $bind_vars));
        $value->setKeyspace($keyspace);
        $value->setShards($shards);
        return $value;
    }

    /**
     *
     * @param string $query
     * @param mixed $bind_vars
     * @param string $keyspace
     * @param mixed $keyspace_ids
     *
     * @return BoundKeyspaceIdQuery
     */
    public function BoundKeyspaceIdQuery($query, $bind_vars, $keyspace, $keyspace_ids)
    {
        $value = new BoundKeyspaceIdQuery();
        $value->setQuery(self::BoundQuery($query, $bind_vars));
        $value->setKeyspace($keyspace);
        $value->setKeyspaceIds($keyspace_ids);
        return $value;
    }

    /**
     *
     * @param Query\Row $row
     * @param Query\Field[] $fields
     *
     * @return array
     * @throws Exception
     */
    public static function RowValues($row, array $fields)
    {
        $values = array();

        // Row values are packed into a single buffer.
        // See the docs for the Row message in query.proto.
        $start = 0;
        $buf = $row->getValues();
        $buflen = strlen($buf);
        $lengths = $row->getLengths();

        foreach ($lengths as $key => $len) {
            $fieldKey = $fields[$key]->getName();

            // $len < 0 indicates a MySQL NULL value,
            // to distinguish it from a zero-length string.
            $val = null;
            if ($len >= 0) {
                if ($start == $buflen) {
                    // Different PHP versions treat this case differently in
                    // substr(), so we handle it manually.
                    $val = '';
                } else {
                    $val = substr($buf, $start, $len);
                    if ($val === FALSE || strlen($val) !== $len) {
                        throw new Exception("Index out of bounds while decoding Row values (start=$start, len=$len). Raw protobuf: " . var_export($row, TRUE));
                    }
                }

                $start += $len;
            }

            $values[$fieldKey] = $val;
            $values[$key] = $val;
        }

        return $values;
    }
}

