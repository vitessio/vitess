<?php

/*
 * This is a sample for using the PHP Vitess client.
 *
 * Before running this, start up a local demo cluster by running:
 * vitess$ examples/demo/run.py
 *
 * Then in another terminal:
 * vitess/php$ php demo.php --server=localhost:12346
 */
require_once ('./src/VTGateConn.php');
require_once ('./src/VTGrpcClient.php');

$opts = getopt('', array(
		'server:' 
));

// Create a connection.
$ctx = VTContext::getDefault();
$conn = new VTGateConn(new VTGrpcClient($opts['server']));

// Insert something.
$tx = $conn->begin($ctx);
$tx->execute($ctx, 'INSERT INTO user (name) VALUES (:name)', array(
		'name' => 'sugu' 
));
$tx->commit($ctx);

// Read it back.
$cursor = $conn->execute($ctx, 'SELECT * FROM user', array(), \topodata\TabletType::MASTER);
while (($row = $cursor->next()) !== FALSE) {
	print_r($row);
}

$conn->close();
