<?php

/*
 * This is a sample for using the PHP Vitess client with an unsharded keyspace.
 *
 * Before running this, start up a local example cluster as described in the
 * README.md file.
 *
 * Then run:
 * vitess/examples/local$ php client.php --server=localhost:15991
 */
require_once __DIR__ . '/../../php/vendor/autoload.php';

use Vitess\Context;
use Vitess\VTGateConn;
use Vitess\Proto\Topodata\TabletType;

$opts = getopt('', array(
    'server:'
));

$keyspace = 'test_keyspace';

// An unsharded keyspace is the same as custom sharding (0, 1, 2, ...),
// but with only a single shard (0).
$shards = array(
    '0'
);

// Create a connection.
$ctx = Context::getDefault();
$conn = new VTGateConn(new \Vitess\Grpc\Client($opts['server'], [
    'credentials' => Grpc\ChannelCredentials::createInsecure()
]));

// Insert something.
echo "Inserting into master...\n";
$tx = $conn->begin($ctx);
$tx->executeShards($ctx, 'INSERT INTO test_table (msg) VALUES (:msg)', $keyspace, $shards, array(
    'msg' => 'V is for speed'
));
$tx->commit($ctx);

// Read it back from the master.
echo "Reading from master...\n";
$cursor = $conn->executeShards($ctx, 'SELECT * FROM test_table', $keyspace, $shards, array(), TabletType::MASTER);
while (($row = $cursor->next()) !== FALSE) {
    printf("(%s)\n", implode(', ', $row));
}

// Read from a replica.
// Note that this may be behind master due to replication lag.
echo "Reading from replica...\n";
$cursor = $conn->executeShards($ctx, 'SELECT * FROM test_table', $keyspace, $shards, array(), TabletType::REPLICA);
while (($row = $cursor->next()) !== FALSE) {
    printf("(%s)\n", implode(', ', $row));
}

$conn->close();
