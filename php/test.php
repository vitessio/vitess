<?php

/*
This is a sample for using the vtgatev3.php module.

Before running this, start up a local demo cluster by running:
    vitess$ test/demo.py
*/

require_once('vtgatev3.php');

$dbh = new VTGateConnection('localhost:15009');

$dbh->beginTransaction();
$dbh->exec('INSERT INTO user (name) VALUES (:name)', array('name' => 'user 1'), 'master');
$dbh->exec('INSERT INTO user (name) VALUES (:name)', array('name' => 'user 2'), 'master');
$dbh->commit();

$stmt = $dbh->query('SELECT * FROM user');
print_r($stmt->fetchAll());
