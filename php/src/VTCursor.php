<?php
require_once (dirname(__FILE__) . '/VTProto.php');
require_once (dirname(__FILE__) . '/VTException.php');

class VTCursor {
	private $queryResult;
	private $pos = - 1;
	private $rows;

	public function __construct(\query\QueryResult $query_result) {
		$this->queryResult = $query_result;
		if ($query_result->hasRows()) {
			$this->rows = $query_result->getRowsList();
		}
	}

	public function getRowsAffected() {
		return $this->queryResult->getRowsAffected();
	}

	public function getInsertId() {
		return $this->queryResult->getInsertId();
	}

	public function getFields() {
		return $this->queryResult->getFieldsList();
	}

	public function close() {
	}

	public function next() {
		if ($this->rows && ++ $this->pos < count($this->rows)) {
			return VTProto::RowValues($this->rows[$this->pos]);
		} else {
			return FALSE;
		}
	}
}

class VTStreamCursor {
	private $pos = - 1;
	private $rows;
	private $fields;
	private $stream;

	public function __construct($stream) {
		$this->stream = $stream;
	}

	public function getFields() {
		if ($this->fields === NULL) {
			// The first QueryResult should have the fields.
			if (! $this->nextQueryResult()) {
				throw new VTException("stream ended before fields were received");
			}
		}
		return $this->fields;
	}

	public function close() {
		if ($this->stream) {
			$this->stream->close();
			$this->stream = NULL;
		}
	}

	public function next() {
		// Get the next row from the current QueryResult.
		if ($this->rows && ++ $this->pos < count($this->rows)) {
			return VTProto::RowValues($this->rows[$this->pos]);
		}
		
		// Get the next QueryResult. Loop in case we get a QueryResult with no rows (e.g. only fields).
		while ($this->nextQueryResult()) {
			// Get the first row from the new QueryResult.
			if ($this->rows && ++ $this->pos < count($this->rows)) {
				return VTProto::RowValues($this->rows[$this->pos]);
			}
		}
		
		// No more rows and no more QueryResults.
		return FALSE;
	}

	private function nextQueryResult() {
		if (($response = $this->stream->next()) !== FALSE) {
			$query_result = $response->getResult();
			$this->rows = $query_result->getRowsList();
			$this->pos = - 1;
			
			if ($this->fields === NULL) {
				// The first QueryResult should have the fields.
				$this->fields = $query_result->getFieldsList();
			}
			return TRUE;
		} else {
			// No more QueryResults.
			$this->rows = NULL;
			return FALSE;
		}
	}
}