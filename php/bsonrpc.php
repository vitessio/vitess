<?php

/*
NOTE: This module requires bson_encode() and bson_decode(),
      which can be obtained by installing the MongoDB driver.
      For example, on Debian/Ubuntu:
          $ sudo apt-get install php5-mongo
*/

require_once('gorpc.php');

class BsonRpcClient extends GoRpcClient {
	const LEN_PACK_FORMAT = 'V';
	const LEN_PACK_SIZE = 4;

	protected function send_request(GoRpcRequest $req) {
		$this->write(bson_encode($req->header));
		$this->write(bson_encode($req->body));
	}

	protected function read_response() {
		// Read the header.
		$data = $this->read_n(LEN_PACK_SIZE);
		$len = unpack(LEN_PACK_FORMAT, $data)[0];
		$header = $data . $this->read_n($len - LEN_PACK_SIZE);

		// Read the body.
		$data = $this->read_n(LEN_PACK_SIZE);
		$len = unpack(LEN_PACK_FORMAT, $data)[0];
		$body = $data . $this->read_n($len - LEN_PACK_SIZE);

		// Decode and return.
		return new GoRpcResponse(bson_decode($header), bson_decode($body));
	}
}
