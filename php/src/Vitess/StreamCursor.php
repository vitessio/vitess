<?php

namespace Vitess;

use Vitess\Grpc\StreamResponse;

/**
 * Class StreamCursor
 *
 * @package Vitess
 */
class StreamCursor
{

    /**
     * @var int
     */
    private $pos = - 1;

    /**
     * @var array
     */
    private $rows;

    /**
     * @var Proto\Query\Field[]
     */
    private $fields;

    /**
     * @var StreamResponse
     */
    private $stream;

    public function __construct(StreamResponse $stream)
    {
        $this->stream = $stream;
    }

    /**
     * @return Proto\Query\Field[]
     * @throws Exception
     */
    public function getFields()
    {
        // The first QueryResult should have the fields.
        if ($this->fields === null && !$this->nextQueryResult()) {
            throw new Exception('Stream ended before fields were received');
        }

        return $this->fields;
    }

    /**
     * @return void
     */
    public function close()
    {
        if ($this->stream) {
            $this->stream->close();
            $this->stream = null;
        }
    }

    /**
     * @return array|bool
     * @throws Exception
     */
    public function next()
    {
        // Get the next row from the current QueryResult.
        if ($this->rows && ++ $this->pos < count($this->rows)) {
            return ProtoUtils::RowValues($this->rows[$this->pos], $this->getFields());
        }

        // Get the next QueryResult. Loop in case we get a QueryResult with no rows (e.g. only fields).
        while ($this->nextQueryResult()) {
            // Get the first row from the new QueryResult.
            if ($this->rows && ++ $this->pos < count($this->rows)) {
                return ProtoUtils::RowValues($this->rows[$this->pos], $this->getFields());
            }
        }

        // No more rows and no more QueryResults.
        return false;
    }

    /**
     * @return bool
     */
    private function nextQueryResult()
    {
        if (($response = $this->stream->next()) !== false) {
            $queryResult = $response->getResult();
            $this->rows = $queryResult->getRowsList();
            $this->pos = - 1;

            if ($this->fields === null) {
                // The first QueryResult should have the fields.
                $this->fields = $queryResult->getFieldsList();
            }
            return true;
        } else {
            // No more QueryResults.
            $this->rows = null;
            return false;
        }
    }
}