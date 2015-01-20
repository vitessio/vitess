<?php

/*
This is a naive implementation of a Vitess-compatible Go-style RPC layer
for PHP. It should be considered a proof-of-concept, as it has not been
tested, since we don't use PHP ourselves. It was loosely modeled after
the Python implementation in the Vitess tree, but without consideration
for timeouts, authentication, or SSL.
*/

class GoRpcException extends Exception {
}

class GoRpcRemoteError extends GoRpcException {
}

class GoRpcRequest {
	public $header;
	public $body;

	public function __construct($seq, $method, $body) {
		$this->header = array('ServiceMethod' => $method, 'Seq' => $seq);
		$this->body = $body;
	}

	public function seq() {
		return $this->header['Seq'];
	}
}

class GoRpcResponse {
	public $header;
	public $reply;

	public function __construct($header, $body) {
		$this->header = $header;
		$this->reply = $body;
	}

	public function error() {
		return $this->header['Error'];
	}

	public function seq() {
		return $this->header['Seq'];
	}

	public function is_eos() {
		return $this->error() == self::END_OF_STREAM;
	}

	const END_OF_STREAM = 'EOS';
}

abstract class GoRpcClient {
	protected $seq = 0;
	protected $stream = NULL;

	abstract protected function send_request(GoRpcRequest $req);
	abstract protected function read_response();

	public function dial($addr, $path) {
		// Connect to $addr.
		$fp = stream_socket_client($addr, $errno, $errstr);
		if ($fp === FALSE)
			throw new GoRpcException("can't connect to $addr: $errstr ($errno)");

		// Initiate request for $path.
		$this->write("CONNECT $path HTTP/1.0\n\n");

		// Read until handshake is completed.
		$data = '';
		while (strpos($data, "\n\n") === FALSE)
			$data .= $this->read(1024);

		$this->stream = $fp;
	}

	public function close() {
		if ($this->stream !== NULL) {
			fclose($this->stream);
			$this->stream = NULL;
		}
	}

	public function call($method, $request) {
		$req = new GoRpcRequest($this->next_seq(), $method, $request);
		$this->send_request($req);

		$resp = $this->read_response();
		if ($resp->seq() != $req->seq())
			throw new GoRpcException("$method: request sequence mismatch");
		if ($resp->error())
			throw new GoRpcRemoteError("$method: " . $resp->error());

		return $resp;
	}

	public function stream_call($method, $request) {
		$req = new GoRpcRequest($this->next_seq(), $method, $request);
		$this->send_request($req);
	}

	public function stream_next() {
		$resp = $this->read_response();
		if ($resp->seq() != $this->seq)
			throw new GoRpcException("$method: request sequence mismatch");
		if ($resp->is_eos())
			return FALSE;
		if ($resp->error())
			throw new GoRpcRemoteError("$method: " . $resp->error());
		return $resp;
	}

	protected function read($max_len) {
		if (feof($this->stream))
			throw new GoRpcException("unexpected EOF while reading from stream");
		$packet = fread($this->stream, $max_len);
		if ($packet === FALSE)
			throw new GoRpcException("can't read from stream");
	}

	protected function read_n($target_len) {
		// Read exactly $target_len bytes or bust.
		$data = '';
		while (($len = strlen($data)) < $target_len)
			$data .= $this->read($target_len - $len);
		return $data;
	}


	protected function write($data) {
		if (fwrite($this->stream, $data) === FALSE)
			throw new GoRpcException("can't write to stream");
	}

	protected function next_seq() {
		return ++$this->seq;
	}
}
