<?php

/*
 * This is a sample for using the PHP Vitess client.
 *
 * Before running this, start up a local demo cluster by running:
 * vitess$ test/demo.py
 *
 * The demo.py script will print the vtgate port, which you pass in like this:
 * vitess/php$ php demo.php --server localhost:port
 */
require_once ('./src/VTGateConn.php');
require_once ('./src/BsonRpcClient.php');

$opts = getopt('', array(
		'server:' 
));

// Create a connection.
$ctx = VTContext::getDefault();
$client = new BsonRpcClient();
$client->dial($ctx, $opts['server']);
$conn = new VTGateConn($client);

// Insert something.
$tx = $conn->begin($ctx);
$tx->execute($ctx, 'INSERT INTO user (name) VALUES (:name)', array(
		'name' => 'sugu' 
));
$tx->commit($ctx);

// Read it back.
$result = $conn->execute($ctx, 'SELECT * FROM user', array(), VTTabletType::MASTER);
print_r($result->rows);
