<?php

/*
 * This is an implementation of a Vitess-compatible Go-style RPC layer for PHP.
 */

// TODO(enisoc): Implement timeouts.
class GoRpcException extends Exception {
}

class GoRpcRemoteError extends GoRpcException {
}

class GoRpcRequest {
	public $header;
	public $body;

	public function __construct($seq, $method, $body) {
		$this->header = array(
				'ServiceMethod' => $method,
				'Seq' => $seq 
		);
		$this->body = $body;
	}

	public function seq() {
		return $this->header['Seq'];
	}
}

function unwrap_bin_data($value) {
	if (is_object($value) && get_class($value) == 'MongoBinData')
		return $value->bin;
	if (is_array($value)) {
		foreach ($value as $key => $child)
			$value[$key] = unwrap_bin_data($child);
	}
	return $value;
}

class GoRpcResponse {
	public $header;
	public $reply;

	public function __construct($header, $body) {
		$this->header = unwrap_bin_data($header);
		$this->reply = unwrap_bin_data($body);
	}

	public function error() {
		return $this->header['Error'];
	}

	public function seq() {
		return $this->header['Seq'];
	}

	public function isEOS() {
		return $this->error() == self::END_OF_STREAM;
	}
	const END_OF_STREAM = 'EOS';
}

abstract class GoRpcClient {
	protected $seq = 0;
	protected $stream = NULL;

	abstract protected function sendRequest(VTContext $ctx, GoRpcRequest $req);

	abstract protected function readResponse(VTContext $ctx);

	public function dial(VTContext $ctx, $addr, $path) {
		// Connect to $addr.
		$fp = stream_socket_client($addr, $errno, $errstr);
		if ($fp === FALSE)
			throw new GoRpcException("can't connect to $addr: $errstr ($errno)");
		$this->stream = $fp;
		
		// Initiate request for $path.
		$this->write($ctx, "CONNECT $path HTTP/1.0\n\n");
		
		// Read until handshake is completed.
		$data = '';
		while (strpos($data, "\n\n") === FALSE)
			$data .= $this->read($ctx, 1024);
	}

	public function close() {
		if ($this->stream !== NULL) {
			fclose($this->stream);
			$this->stream = NULL;
		}
	}

	public function call(VTContext $ctx, $method, $request) {
		$req = new GoRpcRequest($this->nextSeq(), $method, $request);
		$this->sendRequest($ctx, $req);
		
		$resp = $this->readResponse($ctx);
		if ($resp->seq() != $req->seq())
			throw new GoRpcException("$method: request sequence mismatch");
		if ($resp->error())
			throw new GoRpcRemoteError("$method: " . $resp->error());
		
		return $resp;
	}

	public function streamCall(VTContext $ctx, $method, $request) {
		$req = new GoRpcRequest($this->nextSeq(), $method, $request);
		$this->sendRequest($ctx, $req);
	}

	public function streamNext(VTContext $ctx) {
		$resp = $this->readResponse($ctx);
		if ($resp->seq() != $this->seq)
			throw new GoRpcException("$method: request sequence mismatch");
		if ($resp->isEOS())
			return FALSE;
		if ($resp->error())
			throw new GoRpcRemoteError("$method: " . $resp->error());
		return $resp;
	}

	protected function read(VTContext $ctx, $max_len) {
		if (feof($this->stream))
			throw new GoRpcException("unexpected EOF while reading from stream");
		$packet = fread($this->stream, $max_len);
		if ($packet === FALSE)
			throw new GoRpcException("can't read from stream");
		return $packet;
	}

	protected function readN(VTContext $ctx, $target_len) {
		// Read exactly $target_len bytes or bust.
		$data = '';
		while (($len = strlen($data)) < $target_len)
			$data .= $this->read($ctx, $target_len - $len);
		return $data;
	}

	protected function write(VTContext $ctx, $data) {
		if (fwrite($this->stream, $data) === FALSE)
			throw new GoRpcException("can't write to stream");
	}

	protected function nextSeq() {
		return ++ $this->seq;
	}
}
