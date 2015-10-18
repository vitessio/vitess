<?php
require_once (dirname(__FILE__) . '/VTTestUtils.php');
require_once (dirname(__FILE__) . '/../src/VTProto.php');

class VTProtoTest extends PHPUnit_Framework_TestCase {

	public function testBoundQuery() {
		$expected = new \query\BoundQuery();
		$expected->setSql('test query');
		add_bind_var($expected, \query\BindVariable\Type::TYPE_BYTES, 'setValueBytes', 'bytes', 'hello');
		add_bind_var($expected, \query\BindVariable\Type::TYPE_INT, 'setValueInt', 'int', 123);
		add_bind_var($expected, \query\BindVariable\Type::TYPE_UINT, 'setValueUint', 'uint_from_int', - 123);
		add_bind_var($expected, \query\BindVariable\Type::TYPE_FLOAT, 'setValueFloat', 'float', 1.5);
		add_bind_var($expected, \query\BindVariable\Type::TYPE_BYTES_LIST, 'setValueBytesList', 'bytes_list', array(
				'one',
				'two' 
		));
		add_bind_var($expected, \query\BindVariable\Type::TYPE_INT_LIST, 'setValueIntList', 'int_list', array(
				1,
				2,
				3 
		));
		add_bind_var($expected, \query\BindVariable\Type::TYPE_UINT_LIST, 'setValueUintList', 'uint_list', array(
				123,
				456 
		));
		add_bind_var($expected, \query\BindVariable\Type::TYPE_FLOAT_LIST, 'setValueFloatList', 'float_list', array(
				2.0,
				4.0 
		));
		
		$actual = VTProto::BoundQuery('test query', array(
				'bytes' => 'hello',
				'int' => 123,
				'uint_from_int' => new VTUnsignedInt(- 123),
				'float' => 1.5,
				'bytes_list' => array(
						'one',
						'two' 
				),
				'int_list' => array(
						1,
						2,
						3 
				),
				'uint_list' => array(
						new VTUnsignedInt(123),
						new VTUnsignedInt(456) 
				),
				'float_list' => array(
						2.0,
						4.0 
				) 
		));
		
		$this->assertEquals($expected, $actual);
	}
}