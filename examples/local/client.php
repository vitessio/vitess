<?php
/*
 * Copyright 2017 Google Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This is a sample for using the low-level PHP Vitess client.
 * For a sample of using the PDO wrapper, see client_pdo.php.
 *
 * Before running this, start up a local example cluster as described in the
 * README.md file.
 *
 * You will also need to install the gRPC PHP extension as described in
 * vitess/php/README.md, and download dependencies with:
 * vitess$ composer install
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

// Create a connection.
$ctx = Context::getDefault();
$conn = new VTGateConn(new \Vitess\Grpc\Client($opts['server'], [
    'credentials' => Grpc\ChannelCredentials::createInsecure()
]));

// Insert some messages on random pages.
echo "Inserting into master...\n";
for ($i = 0; $i < 3; $i ++) {
    $page = rand(1, 100);
    $time_created = sprintf('%.0f', microtime(true) * 1000000000);;

    $tx = $conn->begin($ctx);
    $tx->execute($ctx, 'INSERT INTO messages (page,time_created_ns,message) VALUES (:page,:time_created_ns,:message)', array(
        'page' => $page,
        'time_created_ns' => $time_created,
        'message' => 'V is for speed'
    ));
    $tx->commit($ctx);
}

// Read it back from the master.
echo "Reading from master...\n";
$cursor = $conn->execute($ctx, 'SELECT page, time_created_ns, message FROM messages', array(), TabletType::MASTER);
while (($row = $cursor->next()) !== FALSE) {
    printf("(%d, %d, %s)\n", $row[0], $row[1], $row[2]);
}

// Read from a replica.
// Note that this may be behind master due to replication lag.
echo "Reading from replica...\n";
$cursor = $conn->execute($ctx, 'SELECT page, time_created_ns, message FROM messages', array(), TabletType::REPLICA);
while (($row = $cursor->next()) !== FALSE) {
    printf("(%d, %d, %s)\n", $row[0], $row[1], $row[2]);
}

$conn->close();
