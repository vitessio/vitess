<?php
require_once (dirname(__FILE__) . '/../src/VTProto.php');
require_once (dirname(__FILE__) . '/../src/VTGateConn.php');
require_once (dirname(__FILE__) . '/../src/BsonRpcClient.php');

class VTGateConnTest extends PHPUnit_Framework_TestCase {
	private static $proc;
	private static $client;
	private static $ECHO_QUERY = 'echo://test query';
	private static $BIND_VARS; // initialized in setUpBeforeClass()
	private static $BIND_VARS_ECHO = 'map[bytes:[104 101 108 108 111] float:1.5 int:123 uint_from_int:345 uint_from_string:678]';
	private static $CALLER_ID; // initialized in setUpBeforeClass()
	private static $CALLER_ID_ECHO = 'principal:"test_principal" component:"test_component" subcomponent:"test_subcomponent" ';
	private static $TABLET_TYPE = VTTabletType::REPLICA;
	private static $TABLET_TYPE_ECHO = 'REPLICA';
	private static $KEYSPACE = 'test_keyspace';
	private static $SHARDS = array(
			'-80',
			'80-' 
	);
	private static $SHARDS_ECHO = '[-80 80-]';
	private static $KEYSPACE_IDS; // initialized in setUpBeforeClass()
	private static $KEYSPACE_IDS_ECHO = '[[128 0 0 0 0 0 0 0] [255 0 0 0 0 0 0 239]]';
	private static $KEYSPACE_IDS_ECHO_OLD = '[8000000000000000 ff000000000000ef]';
	private static $KEY_RANGES; // initialized in setUpBeforeClass()
	private static $KEY_RANGES_ECHO = '[end:"\200\000\000\000\000\000\000\000"  start:"\200\000\000\000\000\000\000\000" ]';
	private static $ENTITY_COLUMN_NAME = 'test_column';
	private static $ENTITY_KEYSPACE_IDS; // initialized in setUpBeforeClass()
	private static $ENTITY_KEYSPACE_IDS_ECHO = '[xid_type:TYPE_FLOAT xid_float:1.5 keyspace_id:"\0224Vx\000\000\000\002"  xid_type:TYPE_INT xid_int:123 keyspace_id:"\0224Vx\000\000\000\000"  xid_type:TYPE_UINT xid_uint:456 keyspace_id:"\0224Vx\000\000\000\001" ]';
	private static $SESSION_ECHO = 'InTransaction: true, ShardSession: []';

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
		
		$cmd = "$VTROOT/bin/vtgateclienttest -logtostderr -lameduck-period 0 -port $port -service_map bsonrpc-vt-vtgateservice";
		
		$proc = proc_open($cmd, array(), $pipes);
		if (! $proc) {
			throw new Exception("Failed to start mock vtgate server with command: $cmd");
		}
		self::$proc = $proc;
		
		// Wait for connection to be accepted.
		$ctx = VTContext::getDefault()->withDeadlineAfter(5.0);
		$client = new BsonRpcClient();
		$level = error_reporting(error_reporting() & ~ E_WARNING);
		while (! $ctx->isCancelled()) {
			try {
				$client->dial($ctx, "$addr:$port");
			} catch (GoRpcException $e) {
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
				'uint_from_int' => new VTUnsignedInt(345),
				'uint_from_string' => new VTUnsignedInt('678'),
				'float' => 1.5 
		);
		self::$CALLER_ID = new VTCallerId('test_principal', 'test_component', 'test_subcomponent');
		self::$KEYSPACE_IDS = array(
				VTKeyspaceID::fromHex('8000000000000000'),
				VTKeyspaceID::fromHex('ff000000000000ef') 
		);
		self::$KEY_RANGES = array(
				array(
						'',
						VTKeyspaceID::fromHex('8000000000000000') 
				),
				array(
						VTKeyspaceID::fromHex('8000000000000000'),
						'' 
				) 
		);
		self::$ENTITY_KEYSPACE_IDS = array(
				VTKeyspaceID::fromHex('1234567800000002') => 'hello',
				VTKeyspaceID::fromHex('1234567800000000') => 123,
				VTKeyspaceID::fromHex('1234567800000001') => new VTUnsignedInt('456'),
				VTKeyspaceID::fromHex('1234567800000002') => 1.5 
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

	private static function getEcho(VTQueryResult $result) {
		$echo = array();
		foreach ($result->fields as $i => $field) {
			$echo[$field->name] = $result->rows[0][$i];
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
				new VTBoundShardQuery(self::$ECHO_QUERY, self::$BIND_VARS, self::$KEYSPACE, self::$SHARDS) 
		), self::$TABLET_TYPE, TRUE);
		$echo = self::getEcho($results[0]);
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$SHARDS_ECHO, $echo['shards']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		$this->assertEquals('true', $echo['asTransaction']);
		
		$results = $conn->executeBatchKeyspaceIds($ctx, array(
				new VTBoundKeyspaceIdQuery(self::$ECHO_QUERY, self::$BIND_VARS, self::$KEYSPACE, self::$KEYSPACE_IDS) 
		), self::$TABLET_TYPE, TRUE);
		$echo = self::getEcho($results[0]);
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$KEYSPACE_IDS_ECHO_OLD, $echo['keyspaceIds']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		$this->assertEquals('true', $echo['asTransaction']);
	}

	public function testEchoStreamExecute() {
		$ctx = $this->ctx;
		$conn = $this->conn;
		
		$sr = $conn->streamExecute($ctx, self::$ECHO_QUERY, self::$BIND_VARS, self::$TABLET_TYPE);
		$results = $sr->fetchAll();
		$echo = self::getEcho($results[0]);
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		
		$sr = $conn->streamExecuteShards($ctx, self::$ECHO_QUERY, self::$KEYSPACE, self::$SHARDS, self::$BIND_VARS, self::$TABLET_TYPE);
		$results = $sr->fetchAll();
		$echo = self::getEcho($results[0]);
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$SHARDS_ECHO, $echo['shards']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		
		$sr = $conn->streamExecuteKeyspaceIds($ctx, self::$ECHO_QUERY, self::$KEYSPACE, self::$KEYSPACE_IDS, self::$BIND_VARS, self::$TABLET_TYPE);
		$results = $sr->fetchAll();
		$echo = self::getEcho($results[0]);
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$KEYSPACE_IDS_ECHO, $echo['keyspaceIds']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		
		$sr = $conn->streamExecuteKeyRanges($ctx, self::$ECHO_QUERY, self::$KEYSPACE, self::$KEY_RANGES, self::$BIND_VARS, self::$TABLET_TYPE);
		$results = $sr->fetchAll();
		$echo = self::getEcho($results[0]);
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
				new VTBoundShardQuery(self::$ECHO_QUERY, self::$BIND_VARS, self::$KEYSPACE, self::$SHARDS) 
		), self::$TABLET_TYPE, TRUE);
		$echo = self::getEcho($results[0]);
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$SHARDS_ECHO, $echo['shards']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
		$this->assertEquals(self::$TABLET_TYPE_ECHO, $echo['tabletType']);
		$this->assertEquals(self::$SESSION_ECHO, $echo['session']);
		$this->assertEquals('true', $echo['asTransaction']);
		
		$results = $tx->executeBatchKeyspaceIds($ctx, array(
				new VTBoundKeyspaceIdQuery(self::$ECHO_QUERY, self::$BIND_VARS, self::$KEYSPACE, self::$KEYSPACE_IDS) 
		), self::$TABLET_TYPE, TRUE);
		$echo = self::getEcho($results[0]);
		$this->assertEquals(self::$CALLER_ID_ECHO, $echo['callerId']);
		$this->assertEquals(self::$ECHO_QUERY, $echo['query']);
		$this->assertEquals(self::$KEYSPACE, $echo['keyspace']);
		$this->assertEquals(self::$KEYSPACE_IDS_ECHO_OLD, $echo['keyspaceIds']);
		$this->assertEquals(self::$BIND_VARS_ECHO, $echo['bindVars']);
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
				'int' => 123,
				'uint_from_int' => new VTUnsignedInt(345),
				'uint_from_string' => new VTUnsignedInt('678'),
				'float' => 1.5 
		);
		$expected_bind_vars = array(
				'bytes' => 'hello',
				'int' => 123,
				'uint_from_int' => new VTUnsignedInt(345),
				// uint_from_string will come back to us as an int.
				'uint_from_string' => new VTUnsignedInt(678),
				'float' => 1.5 
		);
		
		$expected = new VTSplitQueryPart();
		$expected->query = new VTBoundQuery(self::$ECHO_QUERY . ':split_column:123', $expected_bind_vars);
		$expected->keyRangePart = new VTSplitQueryKeyRangePart();
		$expected->keyRangePart->keyspace = self::$KEYSPACE;
		
		$actual = $conn->splitQuery($ctx, self::$KEYSPACE, self::$ECHO_QUERY, $input_bind_vars, 'split_column', 123);
		$this->assertEquals($expected, $actual[0]);
	}

	public function testGetSrvKeyspace() {
		$ctx = $this->ctx;
		$conn = $this->conn;
		
		$expected = new VTSrvKeyspace();
		$expected->partitions[] = new VTSrvKeyspacePartition(VTTabletType::REPLICA, array(
				new VTShardReference("shard0", array(
						VTKeyspaceID::fromHex('4000000000000000'),
						VTKeyspaceID::fromHex('8000000000000000') 
				)) 
		));
		$expected->shardingColumnName = 'sharding_column_name';
		$expected->shardingColumnType = VTKeyspaceIDType::UINT64;
		$expected->servedFrom[] = new VTSrvKeyspaceServedFrom(VTTabletType::MASTER, 'other_keyspace');
		$expected->splitShardCount = 128;
		
		$actual = $conn->getSrvKeyspace($ctx, "big");
		$this->assertEquals($expected, $actual);
	}
}
