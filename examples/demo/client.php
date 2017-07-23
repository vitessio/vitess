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
 * This is a sample for using the PHP Vitess client with VTGateV3.
 *
 * Before running this, start up a local V3 demo cluster by running:
 * vitess/examples/demo$ ./run.py
 *
 * Then in another terminal:
 * vitess/examples/demo$ php client.php --server=localhost:12346
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

// Insert something.
$tx = $conn->begin($ctx);
$tx->execute($ctx, 'INSERT INTO user (name) VALUES (:name)', array(
    'name' => 'sugu'
));
$tx->commit($ctx);

// Read it back.
$cursor = $conn->execute($ctx, 'SELECT * FROM user', array(), TabletType::MASTER);
while (($row = $cursor->next()) !== FALSE) {
    print_r($row);
}

$conn->close();
