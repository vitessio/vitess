<?php
require_once (dirname(__FILE__) . '/VTTestUtils.php');
require_once (dirname(__FILE__) . '/../src/VTProto.php');

class VTProtoTest extends PHPUnit_Framework_TestCase {

	public function testBoundQuery() {
		$expected = new \query\BoundQuery();
		$expected->setSql('test query');
		add_bind_var($expected, \query\Type::VARBINARY, 'bytes', 'hello');
		add_bind_var($expected, \query\Type::INT64, 'int', '123');
		add_bind_var($expected, \query\Type::UINT64, 'uint_from_int', '18446744073709551493'); // 18446744073709551493 = uint64(-123)
		add_bind_var($expected, \query\Type::UINT64, 'uint_from_string', '456');
		add_bind_var($expected, \query\Type::FLOAT64, 'float', '1.5');
		add_list_bind_var($expected, \query\Type::VARBINARY, 'bytes_list', array(
				'one',
				'two' 
		));
		add_list_bind_var($expected, \query\Type::INT64, 'int_list', array(
				'1',
				'2',
				'3' 
		));
		add_list_bind_var($expected, \query\Type::UINT64, 'uint_list', array(
				'123',
				'456' 
		));
		add_list_bind_var($expected, \query\Type::FLOAT64, 'float_list', array(
				'2.0',
				'4.0' 
		));
		
		$actual = VTProto::BoundQuery('test query', array(
				'bytes' => 'hello',
				'int' => 123,
				'uint_from_int' => new VTUnsignedInt(- 123),
				'uint_from_string' => new VTUnsignedInt('456'),
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
						new VTUnsignedInt('456') 
				),
				'float_list' => array(
						2.0,
						4.0 
				) 
		));
		
		$this->assertEquals($expected, $actual);
	}
}