<?php

function add_bind_var($bound_query, $type, $key, $value) {
	$bv = new \query\BindVariable();
	$bv->setType($type);
	$bv->setValue($value);
	$entry = new \query\BoundQuery\BindVariablesEntry();
	$entry->setKey($key);
	$entry->setValue($bv);
	$bound_query->addBindVariables($entry);
}

function add_list_bind_var($bound_query, $type, $key, $values) {
	$bv = new \query\BindVariable();
	$bv->setType(\query\Type::TUPLE);
	foreach ($values as $value) {
		$val = new \query\Value();
		$val->setType($type);
		$val->setValue($value);
		$bv->addValues($val);
	}
	$entry = new \query\BoundQuery\BindVariablesEntry();
	$entry->setKey($key);
	$entry->setValue($bv);
	$bound_query->addBindVariables($entry);
}
