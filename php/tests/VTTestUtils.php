<?php

function add_bind_var($bound_query, $type, $method, $key, $value) {
	$bv = new \query\BindVariable();
	$bv->setType($type);
	$bv->$method($value);
	$entry = new \query\BoundQuery\BindVariablesEntry();
	$entry->setKey($key);
	$entry->setValue($bv);
	$bound_query->addBindVariables($entry);
}