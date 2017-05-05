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
 * This is a sample for using the PDO Vitess client.
 *
 * Before running this, start up a local example cluster as described in the
 * README.md file.
 *
 * You'll also need to install the gRPC PHP extension as described in
 * php/README.md, and then download dependencies for the PDO wrapper:
 *   vitess/php/pdo$ composer install
 *
 * Then run:
 *   vitess/examples/local$ php client.php --server=localhost:15991
 */
require_once __DIR__ . '/../../php/pdo/vendor/autoload.php';

$opts = getopt('', array(
    'server:'
));
list($host, $port) = explode(':', $opts['server']);

$keyspace = 'test_keyspace';

// Create a connection.
$pdo = new \VitessPdo\PDO("vitess:dbname={$keyspace};host={$host};port={$port}");
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

// Insert some messages on random pages.
echo "Inserting into master...\n";
for ($i = 0; $i < 3; $i ++) {
    $page = rand(1, 100);
    $time_created = sprintf('%.0f', microtime(true) * 1000000000);;

    $stmt = $pdo->prepare('INSERT INTO messages (page,time_created_ns,message) VALUES (?,?,?)');
    $stmt->execute([$page, $time_created, 'V is for speed']);
}

// Read from a replica.
// Note that this may be behind master due to replication lag.
echo "Reading from replica...\n";
$stmt = $pdo->prepare('SELECT page, time_created_ns, message FROM messages');
$stmt->execute();
while (($row = $stmt->fetch()) !== FALSE) {
    printf("(%d, %d, %s)\n", $row[0], $row[1], $row[2]);
}
