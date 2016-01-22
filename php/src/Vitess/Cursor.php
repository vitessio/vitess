<?php
namespace Vitess;

class Cursor
{

    private $queryResult;

    private $pos = - 1;

    private $rows;

    public function __construct(Proto\Query\QueryResult $query_result)
    {
        $this->queryResult = $query_result;
        if ($query_result->hasRows()) {
            $this->rows = $query_result->getRowsList();
        }
    }

    public function getRowsAffected()
    {
        return $this->queryResult->getRowsAffected();
    }

    public function getInsertId()
    {
        return $this->queryResult->getInsertId();
    }

    public function getFields()
    {
        return $this->queryResult->getFieldsList();
    }

    public function close()
    {}

    public function next()
    {
        if ($this->rows && ++ $this->pos < count($this->rows)) {
            return ProtoUtils::RowValues($this->rows[$this->pos]);
        } else {
            return FALSE;
        }
    }
}
