<?php

/*
 * NOTE: This module requires bson_encode() and bson_decode(),
 * which can be obtained by installing the MongoDB driver.
 * For example, on Debian/Ubuntu:
 * $ sudo apt-get install php5-dev php5-cli php-pear
 * $ sudo pecl install mongo
 */
ini_set('mongo.native_long', 1);

require_once (dirname(__FILE__) . '/GoRpcClient.php');

class BsonRpcClient extends GoRpcClient {
	const LEN_PACK_FORMAT = 'V';
	const LEN_PACK_SIZE = 4;

	public function dial(VTContext $ctx, $addr) {
		parent::dial($ctx, "tcp://$addr", '/_bson_rpc_');
	}

	protected function sendRequest(VTContext $ctx, GoRpcRequest $req) {
		$this->write($ctx, bson_encode($req->header));
		if ($req->body === NULL)
			$this->write($ctx, bson_encode(array()));
		else
			$this->write($ctx, bson_encode($req->body));
	}

	protected function readResponse(VTContext $ctx) {
		// Read the header.
		$data = $this->readN($ctx, self::LEN_PACK_SIZE);
		$unpack = unpack(self::LEN_PACK_FORMAT, $data);
		$len = $unpack[1];
		$header = $data . $this->readN($ctx, $len - self::LEN_PACK_SIZE);
		
		// Read the body.
		$data = $this->readN($ctx, self::LEN_PACK_SIZE);
		$unpack = unpack(self::LEN_PACK_FORMAT, $data);
		$len = $unpack[1];
		$body = $data . $this->readN($ctx, $len - self::LEN_PACK_SIZE);
		
		// Decode and return.
		return new GoRpcResponse(bson_decode($header), bson_decode($body));
	}
}
