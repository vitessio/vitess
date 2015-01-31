<?php

/*
This module contains a connection class for the VTGate V3 API over BSON RPC,
in a vaguely PDO-like interface. This is a minimal implementation to serve
as a proof-of-concept in PHP.
*/

require_once('bsonrpc.php');

class VTGateStatement {
	protected $res = NULL;
	protected $i = 0;

	public function __construct($res) {
		$this->res = $res;
	}

	public function rowCount() {
		return $this->res['RowsAffected'];
	}

	public function fetch() {
		if ($this->i >= count($this->res['Rows']))
			return FALSE;
		return $this->res['Rows'][$this->i++];
	}

	public function fetchAll() {
		return $this->res['Rows'];
	}

	public function closeCursor() {
		$this->res = NULL;
	}
}

class VTGateConnection {
	protected $rpc = NULL;
	protected $session = NULL;

	public function __construct($addr) {
		$this->rpc = new BsonRpcClient();
		$this->rpc->dial($addr);
	}

	public function beginTransaction() {
		$resp = $this->rpc->call('VTGate.Begin', NULL);
		$this->session = $resp->reply;
	}

	public function commit() {
		$session = $this->session;
		$this->session = NULL;
		$this->rpc->call('VTGate.Commit', $session);
	}

	public function rollBack() {
		$session = $this->session;
		$this->session = NULL;
		$this->rpc->call('VTGate.Rollback', $session);
	}

	public function execute($sql, $bind_vars, $tablet_type) {
		$req = array(
			'Sql' => $sql,
			'BindVariables' => $bind_vars,
			'TabletType' => $tablet_type,
		);
		if ($this->session)
			$req['Session'] = $this->session;

		$resp = $this->rpc->call('VTGate.Execute', $req);

		$reply = $resp->reply;
		if (array_key_exists('Session', $reply) && $reply['Session'])
			$this->session = $reply['Session'];
		if (array_key_exists('Error', $reply) && $reply['Error'])
			throw new GoRpcRemoteError('exec: ' . $reply['Error']);

		return $reply['Result'];
	}

	public function exec($sql, $bind_vars, $tablet_type) {
		$res = $this->execute($sql, $bind_vars, $tablet_type);
		return $res['RowsAffected'];
	}

	public function query($sql, $bind_vars, $tablet_type) {
		$res = $this->execute($sql, $bind_vars, $tablet_type);
		return new VTGateStatement($res);
	}
}
