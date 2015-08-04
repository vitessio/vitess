<?php

/*
This is a sample for using the vtgatev3.php module.

Before running this, start up a local demo cluster by running:
    vitess$ test/demo.py

The demo.py script will print the vtgate port, which you pass in like this:
    vitess/php$ php test.php --server localhost:port
*/

require_once('vtgatev3.php');

$opts = getopt('', array('server:'));

$dbh = new VTGateConnection($opts['server']);

$dbh->beginTransaction();
$dbh->exec('INSERT INTO user (name) VALUES (:name)', array('name' => 'user 1'), 'master');
$dbh->exec('INSERT INTO user (name) VALUES (:name)', array('name' => 'user 2'), 'master');
$dbh->commit();

$stmt = $dbh->query('SELECT * FROM user', NULL, 'master');
print_r($stmt->fetchAll());
