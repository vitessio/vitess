<?php
require_once (dirname(__FILE__) . '/VTTestUtils.php');
require_once (dirname(__FILE__) . '/../src/VTProto.php');
require_once (dirname(__FILE__) . '/../src/VTGateConn.php');
require_once (dirname(__FILE__) . '/../src/VTGrpcClient.php');

class VTGateConnTest extends PHPUnit_Framework_TestCase {
	private static $proc;
	private static $client;
	private static $ECHO_QUERY = 'echo://test query';
	private static $ERROR_PREFIX = 'error://';
	private static $PARTIAL_ERROR_PREFIX = 'partialerror://';
	private static $EXECUTE_ERRORS = array(
			'bad input' => 'VTBadInputError',
			'deadline exceeded' => 'VTDeadlineExceededError',
			'integrity error' => 'VTIntegrityError',
			'transient error' => 'VTTransientError',
			'unauthenticated' => 'VTUnauthenticatedError',
			'unknown error' => 'VTException' 
	);
	private static $BIND_VARS; // initialized in setUpBeforeClass()
	private static $BIND_VARS_ECHO = 'map[bytes:[104 101 108 108 111] float:1.5 int:123 uint_from_int:18446744073709551493 uint_from_string:456]'; // 18446744073709551493 = uint64(-123)
	private static $BIND_VARS_ECHO_P3 = 'map[bytes:type:VARBINARY value:"hello"  float:type:FLOAT64 value:"1.5"  int:type:INT64 value:"123"  uint_from_int:type:UINT64 value:"18446744073709551493"  uint_from_string:type:UINT64 value:"456" ]'; // 18446744073709551493 = uint64(-123)
	private static $CALLER_ID; // initialized in setUpBeforeClass()
	private static $CALLER_ID_ECHO = 'principal:"test_principal" component:"test_component" subcomponent:"test_subcomponent" ';
	private static $TABLET_TYPE = \topodata\TabletType::REPLICA;
	private static $TABLET_TYPE_ECHO = 'REPLICA';
	private static $KEYSPACE = 'test_keyspace';
	private static $SHARDS = array(
			'-80',
			'80-' 
	);
	private static $SHARDS_ECHO = '[-80 80-]';
	private static $KEYSPACE_IDS; // initialized in setUpBeforeClass()
	private static $KEYSPACE_IDS_ECHO = '[[128 0 0 0 0 0 0 0] [255 0 0 0 0 0 0 239]]';
	private static $KEY_RANGES; // initialized in setUpBeforeClass()
	private static $KEY_RANGES_ECHO = '[end:"\200\000\000\000\000\000\000\000"  start:"\200\000\000\000\000\000\000\000" ]';
	private static $ENTITY_COLUMN_NAME = 'test_column';
	private static $ENTITY_KEYSPACE_IDS; // initialized in setUpBeforeClass()
	private static $ENTITY_KEYSPACE_IDS_ECHO = '[xid_type:FLOAT64 xid_value:"1.5" keyspace_id:"\0224Vx\000\000\000\002"  xid_type:INT64 xid_value:"123" keyspace_id:"\0224Vx\000\000\000\000"  xid_type:UINT64 xid_value:"456" keyspace_id:"\0224Vx\000\000\000\001" ]';
	private static $SESSION_ECHO = 'in_transaction:true ';

	public static function setUpBeforeClass() {
		$VTROOT = getenv('VTROOT');
		if (! $VTROOT) {
			throw new Exception('VTROOT env var not set; make sure to source dev.env');
		}
		
		// Pick an unused port.
		$sock = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
		socket_bind($sock, 'localhost');
		if (! socket_getsockname($sock, $addr, $port)) {
			throw new Exception('Failed to find unused port for mock vtgate server.');
		}
		socket_close($sock);
		
		$cmd = "$VTROOT/bin/vtgateclienttest -logtostderr -lameduck-period 0 -grpc_port $port -service_map grpc-vtgateservice";
		
		$proc = proc_open($cmd, array(), $pipes);
		if (! $proc) {
			throw new Exception("Failed to start mock vtgate server with command: $cmd");
		}
		self::$proc = $proc;
		
		// Wait for connection to be accepted.
		$ctx = VTContext::getDefault()->withDeadlineAfter(5.0);
		$level = error_reporting(error_reporting() & ~ E_WARNING);
		while (! $ctx->isCancelled()) {
			try {
				$client = new VTGrpcClient("$addr:$port");
			} catch (Exception $e) {
				usleep(100000);
				continue;
			}
			break;
		}
		error_reporting($level);
		self::$client = $client;
		
		// Test fixtures that can't be statically initialized.
		self::$BIND_VARS = array(
				'bytes' => 'hello',
				'int' => 123,
				'uint_from_int' => new VTUnsignedInt(- 123),
				'uint_from_string' => new VTUnsignedInt('456'),
				'float' => 1.5 
		);
		self::$CALLER_ID = new \vtrpc\CallerID();
		self::$CALLER_ID->setPrincipal('test_principal');
		self::$CALLER_ID->setComponent('test_component');
		self::$CALLER_ID->setSubcomponent('test_subcomponent');
		self::$KEYSPACE_IDS = array(
				VTProto::KeyspaceIdFromHex('8000000000000000'),
				VTProto::KeyspaceIdFromHex('ff000000000000ef') 
		);
		self::$KEY_RANGES = array(
				VTProto::KeyRangeFromHex('', '8000000000000000'),
				VTProto::KeyRangeFromHex('8000000000000000', '') 
		);
		self::$ENTITY_KEYSPACE_IDS = array(
				VTProto::KeyspaceIdFromHex('1234567800000002') => 'hello',
				VTProto::KeyspaceIdFromHex('1234567800000000') => 123,
				VTProto::KeyspaceIdFromHex('1234567800000001') => new VTUnsignedInt(456),
				VTProto::KeyspaceIdFromHex('1234567800000002') => 1.5 
		);
	}

	public static function tearDownAfterClass() {
		if (self::$client) {
			try {
				$ctx = VTContext::getDefault()->withDeadlineAfter(5.0);
				$conn = new VTGateconn(self::$client);
				$conn->execute($ctx, 'quit://', array(), 0);
				self::$client->close();
			} catch (Exception $e) {
			}
		}
		if (self::$proc) {
			proc_terminate(self::$proc, 9);
			proc_close(self::$proc);
		}
	}
	private $conn;

	public function setUp() {
		$this->ctx = VTContext::getDefault()->withDeadlineAfter(5.0)->withCallerId(self::$CALLER_ID);
		$this->conn = new VTGateconn(self::$client);
	}

	private static function getEcho($result) {
		$echo = array();
		$row = $result->next();
		foreach ($result->getFields() as $i => $field) {
			$echo[$field->getName()] = $row[$i];
		}
		return $echo;
	}

	public function testEchoExecute() {
		$ctx = $this->ctx;
		$conn = $this->conn;
		
		$echo = self::getEcho($conn->execute($ctx, self::$ECHO_QUERY, self::$BIND_VARS, self::$TABLET_TYPE));
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		
		// Check NULL vs. empty string.
		$this->assertEquals(true, is_null($echo['null']));
		$this->assertEquals(true, is_string($echo['emptyString']));
		$this->assertEquals('', $echo['emptyString']);
		
		$echo = self::getEcho($conn->executeShards($ctx, self::$ECHO_QUERY, self::$KEYSPACE, self::$SHARDS, self::$BIND_VARS, self::$TABLET_TYPE));
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$SHARDS_ECHO, $echo['shards']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		
		$echo = self::getEcho($conn->executeKeyspaceIds($ctx, self::$ECHO_QUERY, self::$KEYSPACE, self::$KEYSPACE_IDS, self::$BIND_VARS, self::$TABLET_TYPE));
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$KEYSPACE_IDS_ECHO, $echo['keyspaceIds']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		
		$echo = self::getEcho($conn->executeKeyRanges($ctx, self::$ECHO_QUERY, self::$KEYSPACE, self::$KEY_RANGES, self::$BIND_VARS, self::$TABLET_TYPE));
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$KEY_RANGES_ECHO, $echo['keyRanges']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		
		$echo = self::getEcho($conn->executeEntityIds($ctx, self::$ECHO_QUERY, self::$KEYSPACE, self::$ENTITY_COLUMN_NAME, self::$ENTITY_KEYSPACE_IDS, self::$BIND_VARS, self::$TABLET_TYPE));
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$ENTITY_COLUMN_NAME, $echo['entityColumnName']);
		$this->assertEquals(self::$ENTITY_KEYSPACE_IDS_ECHO, $echo['entityIds']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		
		$results = $conn->executeBatchShards($ctx, array(
				VTProto::BoundShardQuery(self::$ECHO_QUERY, self::$BIND_VARS, self::$KEYSPACE, self::$SHARDS) 
		), self::$TABLET_TYPE, TRUE);
		$echo = self::getEcho($results[0]);
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$SHARDS_ECHO, $echo['shards']);
		$this->assertEquals(self::$BIND_VARS_ECHO_P3, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		$this->assertEquals('true', $echo['asTransaction']);
		
		$results = $conn->executeBatchKeyspaceIds($ctx, array(
				VTProto::BoundKeyspaceIdQuery(self::$ECHO_QUERY, self::$BIND_VARS, self::$KEYSPACE, self::$KEYSPACE_IDS) 
		), self::$TABLET_TYPE, TRUE);
		$echo = self::getEcho($results[0]);
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$KEYSPACE_IDS_ECHO, $echo['keyspaceIds']);
		$this->assertEquals(self::$BIND_VARS_ECHO_P3, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		$this->assertEquals('true', $echo['asTransaction']);
	}

	public function testEchoStreamExecute() {
		$ctx = $this->ctx;
		$conn = $this->conn;
		
		$echo = self::getEcho($conn->streamExecute($ctx, self::$ECHO_QUERY, self::$BIND_VARS, self::$TABLET_TYPE));
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		
		$echo = self::getEcho($conn->streamExecuteShards($ctx, self::$ECHO_QUERY, self::$KEYSPACE, self::$SHARDS, self::$BIND_VARS, self::$TABLET_TYPE));
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$SHARDS_ECHO, $echo['shards']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		
		$echo = self::getEcho($conn->streamExecuteKeyspaceIds($ctx, self::$ECHO_QUERY, self::$KEYSPACE, self::$KEYSPACE_IDS, self::$BIND_VARS, self::$TABLET_TYPE));
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$KEYSPACE_IDS_ECHO, $echo['keyspaceIds']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		
		$echo = self::getEcho($conn->streamExecuteKeyRanges($ctx, self::$ECHO_QUERY, self::$KEYSPACE, self::$KEY_RANGES, self::$BIND_VARS, self::$TABLET_TYPE));
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$KEY_RANGES_ECHO, $echo['keyRanges']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
	}

	public function testEchoTransactionExecute() {
		$ctx = $this->ctx;
		$conn = $this->conn;
		
		$tx = $conn->begin($ctx);
		
		$echo = self::getEcho($tx->execute($ctx, self::$ECHO_QUERY, self::$BIND_VARS, self::$TABLET_TYPE, TRUE));
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		$this->assertEquals(self::$SESSION_ECHO, $echo['session']);
		$this->assertEquals('true', $echo['notInTransaction']);
		
		$echo = self::getEcho($tx->executeShards($ctx, self::$ECHO_QUERY, self::$KEYSPACE, self::$SHARDS, self::$BIND_VARS, self::$TABLET_TYPE, TRUE));
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$SHARDS_ECHO, $echo['shards']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		$this->assertEquals(self::$SESSION_ECHO, $echo['session']);
		$this->assertEquals('true', $echo['notInTransaction']);
		
		$echo = self::getEcho($tx->executeKeyspaceIds($ctx, self::$ECHO_QUERY, self::$KEYSPACE, self::$KEYSPACE_IDS, self::$BIND_VARS, self::$TABLET_TYPE, TRUE));
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$KEYSPACE_IDS_ECHO, $echo['keyspaceIds']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		$this->assertEquals(self::$SESSION_ECHO, $echo['session']);
		$this->assertEquals('true', $echo['notInTransaction']);
		
		$echo = self::getEcho($tx->executeKeyRanges($ctx, self::$ECHO_QUERY, self::$KEYSPACE, self::$KEY_RANGES, self::$BIND_VARS, self::$TABLET_TYPE, TRUE));
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$KEY_RANGES_ECHO, $echo['keyRanges']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		$this->assertEquals(self::$SESSION_ECHO, $echo['session']);
		$this->assertEquals('true', $echo['notInTransaction']);
		
		$echo = self::getEcho($tx->executeEntityIds($ctx, self::$ECHO_QUERY, self::$KEYSPACE, self::$ENTITY_COLUMN_NAME, self::$ENTITY_KEYSPACE_IDS, self::$BIND_VARS, self::$TABLET_TYPE, TRUE));
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$ENTITY_COLUMN_NAME, $echo['entityColumnName']);
		$this->assertEquals(self::$ENTITY_KEYSPACE_IDS_ECHO, $echo['entityIds']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		$this->assertEquals(self::$SESSION_ECHO, $echo['session']);
		$this->assertEquals('true', $echo['notInTransaction']);
		
		$tx->rollback($ctx);
		$tx = $conn->begin($ctx);
		
		$results = $tx->executeBatchShards($ctx, array(
				VTProto::BoundShardQuery(self::$ECHO_QUERY, self::$BIND_VARS, self::$KEYSPACE, self::$SHARDS) 
		), self::$TABLET_TYPE, TRUE);
		$echo = self::getEcho($results[0]);
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$SHARDS_ECHO, $echo['shards']);
		$this->assertEquals(self::$BIND_VARS_ECHO_P3, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		$this->assertEquals(self::$SESSION_ECHO, $echo['session']);
		$this->assertEquals('true', $echo['asTransaction']);
		
		$results = $tx->executeBatchKeyspaceIds($ctx, array(
				VTProto::BoundKeyspaceIdQuery(self::$ECHO_QUERY, self::$BIND_VARS, self::$KEYSPACE, self::$KEYSPACE_IDS) 
		), self::$TABLET_TYPE, TRUE);
		$echo = self::getEcho($results[0]);
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$KEYSPACE_IDS_ECHO, $echo['keyspaceIds']);
		$this->assertEquals(self::$BIND_VARS_ECHO_P3, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		$this->assertEquals(self::$SESSION_ECHO, $echo['session']);
		$this->assertEquals('true', $echo['asTransaction']);
		
		$tx->commit($ctx);
	}

	public function testEchoSplitQuery() {
		$ctx = $this->ctx;
		$conn = $this->conn;
		
		$input_bind_vars = array(
				'bytes' => 'hello',
				'float' => 1.5,
				'int' => 123,
				'uint_from_int' => new VTUnsignedInt(345),
				'uint_from_string' => new VTUnsignedInt('678') 
		);
		
		$splits = $conn->splitQuery($ctx, self::$KEYSPACE, self::$ECHO_QUERY, $input_bind_vars, 'split_column', 123);
		$actual = $splits[0];
		$bound_query = $actual->getQuery();
		
		$this->assertEquals(self::$KEYSPACE, $actual->getKeyRangePart()->getKeyspace());
		$this->assertEquals(self::$ECHO_QUERY . ':split_column:123', $bound_query->getSql());
		
		// The map of bind vars is implemented as a repeated field, and the order of
		// the entries is arbitrary. First load them into a map.
		$actual_bind_vars = array();
		foreach ($bound_query->getBindVariablesList() as $bind_var_entry) {
			$actual_bind_vars[$bind_var_entry->getKey()] = $bind_var_entry->getValue();
		}
		// Then check that all the expected values exist and are correct.
		foreach ($input_bind_vars as $name => $value) {
			$this->assertEquals(VTProto::BindVariable($value), $actual_bind_vars[$name]);
		}
	}

	public function testGetSrvKeyspace() {
		$ctx = $this->ctx;
		$conn = $this->conn;
		
		$expected = new \topodata\SrvKeyspace();
		$partition = new \topodata\SrvKeyspace\KeyspacePartition();
		$partition->setServedType(\topodata\TabletType::REPLICA);
		$shard_ref = new \topodata\ShardReference();
		$shard_ref->setName("shard0");
		$shard_ref->setKeyRange(VTProto::KeyRangeFromHex('4000000000000000', '8000000000000000'));
		$partition->addShardReferences($shard_ref);
		$expected->addPartitions($partition);
		$expected->setShardingColumnName('sharding_column_name');
		$expected->setShardingColumnType(\topodata\KeyspaceIdType::UINT64);
		$served_from = new \topodata\SrvKeyspace\ServedFrom();
		$served_from->setTabletType(\topodata\TabletType::MASTER);
		$served_from->setKeyspace('other_keyspace');
		$expected->addServedFrom($served_from);
		$expected->setSplitShardCount(128);
		
		$actual = $conn->getSrvKeyspace($ctx, "big");
		$this->assertEquals($expected, $actual);
	}

	private function checkExecuteErrors($execute, $partial = TRUE) {
		foreach (self::$EXECUTE_ERRORS as $error => $class) {
			try {
				$query = self::$ERROR_PREFIX . $error;
				$execute($this->ctx, $this->conn, $query);
				$this->fail("no exception thrown for $query");
			} catch (Exception $e) {
				$this->assertEquals($class, get_class($e), $e->getMessage());
			}
			
			if ($partial) {
				try {
					$query = self::$PARTIAL_ERROR_PREFIX . $error;
					$execute($this->ctx, $this->conn, $query);
					$this->fail("no exception thrown for $query");
				} catch (Exception $e) {
					$this->assertEquals($class, get_class($e), $e->getMessage());
				}
			}
		}
	}

	private function checkTransactionExecuteErrors($execute) {
		foreach (self::$EXECUTE_ERRORS as $error => $class) {
			try {
				$tx = $this->conn->begin($this->ctx);
				$query = self::$ERROR_PREFIX . $error;
				$execute($this->ctx, $tx, $query);
				$this->fail("no exception thrown for $query");
			} catch (Exception $e) {
				$this->assertEquals($class, get_class($e), $e->getMessage());
			}
			
			// Don't close the transaction on partial error.
			$tx = $this->conn->begin($this->ctx);
			try {
				$query = self::$PARTIAL_ERROR_PREFIX . $error;
				$execute($this->ctx, $tx, $query);
				$this->fail("no exception thrown for $query");
			} catch (Exception $e) {
				$this->assertEquals($class, get_class($e), $e->getMessage());
			}
			// The transaction should still be usable now.
			$tx->rollback($this->ctx);
			
			// Close the transaction on partial error.
			$tx = $this->conn->begin($this->ctx);
			try {
				$query = self::$PARTIAL_ERROR_PREFIX . $error . '/close transaction';
				$execute($this->ctx, $tx, $query);
				$this->fail("no exception thrown for $query");
			} catch (Exception $e) {
				$this->assertEquals($class, get_class($e), $e->getMessage());
			}
			// The transaction should be unusable now.
			try {
				$tx->rollback($this->ctx);
				$this->fail("no exception thrown for rollback() after closed transaction");
			} catch (Exception $e) {
				$this->assertEquals('VTException', get_class($e), $e->getMessage());
				$this->assertEquals(TRUE, strpos($e->getMessage(), 'not in transaction') !== FALSE);
			}
		}
	}

	private function checkStreamExecuteErrors($execute) {
		$this->checkExecuteErrors($execute, FALSE);
	}

	public function testExecuteErrors() {
		$this->checkExecuteErrors(function ($ctx, $conn, $query) {
			$conn->execute($ctx, $query, self::$BIND_VARS, self::$TABLET_TYPE);
		});
		
		$this->checkExecuteErrors(function ($ctx, $conn, $query) {
			$conn->executeShards($ctx, $query, self::$KEYSPACE, self::$SHARDS, self::$BIND_VARS, self::$TABLET_TYPE);
		});
		
		$this->checkExecuteErrors(function ($ctx, $conn, $query) {
			$conn->executeKeyspaceIds($ctx, $query, self::$KEYSPACE, self::$KEYSPACE_IDS, self::$BIND_VARS, self::$TABLET_TYPE);
		});
		
		$this->checkExecuteErrors(function ($ctx, $conn, $query) {
			$conn->executeKeyRanges($ctx, $query, self::$KEYSPACE, self::$KEY_RANGES, self::$BIND_VARS, self::$TABLET_TYPE);
		});
		
		$this->checkExecuteErrors(function ($ctx, $conn, $query) {
			$conn->executeEntityIds($ctx, $query, self::$KEYSPACE, self::$ENTITY_COLUMN_NAME, self::$ENTITY_KEYSPACE_IDS, self::$BIND_VARS, self::$TABLET_TYPE);
		});
		
		$this->checkExecuteErrors(function ($ctx, $conn, $query) {
			$conn->executeBatchShards($ctx, array(
					VTProto::BoundShardQuery($query, self::$BIND_VARS, self::$KEYSPACE, self::$SHARDS) 
			), self::$TABLET_TYPE, TRUE);
		});
		
		$this->checkExecuteErrors(function ($ctx, $conn, $query) {
			$conn->executeBatchKeyspaceIds($ctx, array(
					VTProto::BoundKeyspaceIdQuery($query, self::$BIND_VARS, self::$KEYSPACE, self::$KEYSPACE_IDS) 
			), self::$TABLET_TYPE, TRUE);
		});
	}

	public function testTransactionExecuteErrors() {
		$this->checkTransactionExecuteErrors(function ($ctx, $tx, $query) {
			$tx->execute($ctx, $query, self::$BIND_VARS, self::$TABLET_TYPE, TRUE);
		});
		
		$this->checkTransactionExecuteErrors(function ($ctx, $tx, $query) {
			$tx->executeShards($ctx, $query, self::$KEYSPACE, self::$SHARDS, self::$BIND_VARS, self::$TABLET_TYPE, TRUE);
		});
		
		$this->checkTransactionExecuteErrors(function ($ctx, $tx, $query) {
			$tx->executeKeyspaceIds($ctx, $query, self::$KEYSPACE, self::$KEYSPACE_IDS, self::$BIND_VARS, self::$TABLET_TYPE, TRUE);
		});
		
		$this->checkTransactionExecuteErrors(function ($ctx, $tx, $query) {
			$tx->executeKeyRanges($ctx, $query, self::$KEYSPACE, self::$KEY_RANGES, self::$BIND_VARS, self::$TABLET_TYPE, TRUE);
		});
		
		$this->checkTransactionExecuteErrors(function ($ctx, $tx, $query) {
			$tx->executeEntityIds($ctx, $query, self::$KEYSPACE, self::$ENTITY_COLUMN_NAME, self::$ENTITY_KEYSPACE_IDS, self::$BIND_VARS, self::$TABLET_TYPE, TRUE);
		});
		
		$this->checkTransactionExecuteErrors(function ($ctx, $tx, $query) {
			$tx->executeBatchShards($ctx, array(
					VTProto::BoundShardQuery($query, self::$BIND_VARS, self::$KEYSPACE, self::$SHARDS) 
			), self::$TABLET_TYPE, TRUE);
		});
		
		$this->checkTransactionExecuteErrors(function ($ctx, $tx, $query) {
			$tx->executeBatchKeyspaceIds($ctx, array(
					VTProto::BoundKeyspaceIdQuery($query, self::$BIND_VARS, self::$KEYSPACE, self::$KEYSPACE_IDS) 
			), self::$TABLET_TYPE, TRUE);
		});
	}

	public function testStreamExecuteErrors() {
		$this->checkStreamExecuteErrors(function ($ctx, $conn, $query) {
			$conn->streamExecute($ctx, $query, self::$BIND_VARS, self::$TABLET_TYPE)->next();
		});
		
		$this->checkStreamExecuteErrors(function ($ctx, $conn, $query) {
			$conn->streamExecuteShards($ctx, $query, self::$KEYSPACE, self::$SHARDS, self::$BIND_VARS, self::$TABLET_TYPE)->next();
		});
		
		$this->checkStreamExecuteErrors(function ($ctx, $conn, $query) {
			$conn->streamExecuteKeyspaceIds($ctx, $query, self::$KEYSPACE, self::$KEYSPACE_IDS, self::$BIND_VARS, self::$TABLET_TYPE)->next();
		});
		
		$this->checkStreamExecuteErrors(function ($ctx, $conn, $query) {
			$conn->streamExecuteKeyRanges($ctx, $query, self::$KEYSPACE, self::$KEY_RANGES, self::$BIND_VARS, self::$TABLET_TYPE)->next();
		});
	}
}
