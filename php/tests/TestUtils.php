<?php
namespace Vitess;

function add_bind_var($bound_query, $type, $key, $value)
{
    $bv = new Proto\Query\BindVariable();
    $bv->setType($type);
    $bv->setValue($value);
    $entry = new Proto\Query\BoundQuery\BindVariablesEntry();
    $entry->setKey($key);
    $entry->setValue($bv);
    $bound_query->addBindVariables($entry);
}

function add_list_bind_var($bound_query, $type, $key, $values)
{
    $bv = new Proto\Query\BindVariable();
    $bv->setType(Proto\Query\Type::TUPLE);
    foreach ($values as $value) {
        $val = new Proto\Query\Value();
        $val->setType($type);
        $val->setValue($value);
        $bv->addValues($val);
    }
    $entry = new Proto\Query\BoundQuery\BindVariablesEntry();
    $entry->setKey($key);
    $entry->setValue($bv);
    $bound_query->addBindVariables($entry);
}

function make_field($name)
{
    $field = new Proto\Query\Field();
    $field->setName($name);
    return $field;
}
