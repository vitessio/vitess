<?php

namespace Vitess;

/**
 * Class Cursor
 *
 * @package Vitess
 */
class Cursor
{

    /**
     * @var Proto\Query\QueryResult
     */
    private $queryResult;

    /**
     * @var int
     */
    private $pos = - 1;

    /**
     * @var Proto\Query\Field[]
     */
    private $fields;

    /**
     * @var Proto\Query\Row[]
     */
    private $rows;

    /**
     * Cursor constructor.
     *
     * @param Proto\Query\QueryResult $query_result
     */
    public function __construct(Proto\Query\QueryResult $query_result)
    {
        $this->queryResult = $query_result;
        if ($query_result->hasRows()) {
            $this->rows = $query_result->getRowsList();
        }
    }

    /**
     * @return int
     */
    public function getRowsAffected()
    {
        return $this->queryResult->getRowsAffected();
    }

    /**
     * @return int
     */
    public function getInsertId()
    {
        return $this->queryResult->getInsertId();
    }

    /**
     * @return Proto\Query\Field[]
     */
    public function getFields()
    {
        if ($this->fields === null) {
            $this->fields = $this->queryResult->getFieldsList();
        }

        return $this->fields;
    }

    /**
     * @return void
     */
    public function close()
    {}

    /**
     * @return array|bool
     * @throws Exception
     */
    public function next()
    {
        if ($this->rows && ++ $this->pos < count($this->rows)) {
            return ProtoUtils::RowValues($this->rows[$this->pos], $this->getFields());
        } else {
            return FALSE;
        }
    }
}
