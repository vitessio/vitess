<?php
require_once (dirname(__FILE__) . '/../src/VTProto.php');

class VTProtoTest extends PHPUnit_Framework_TestCase {

	public function testBoundQuery() {
		$expected = array(
				'Sql' => 'test query',
				'BindVariables' => array(
						'bytes' => array(
								'Type' => VTBindVariable::TYPE_BYTES,
								'ValueBytes' => new MongoBinData('hello') 
						),
						'int' => array(
								'Type' => VTBindVariable::TYPE_INT,
								'ValueInt' => 123 
						),
						'uint_from_int' => array(
								'Type' => VTBindVariable::TYPE_UINT,
								'ValueUint' => 345 
						),
						'uint_from_string' => array(
								'Type' => VTBindVariable::TYPE_UINT,
								'ValueUint' => new MongoInt64('678') 
						),
						'float' => array(
								'Type' => VTBindVariable::TYPE_FLOAT,
								'ValueFloat' => 1.5 
						),
						'bytes_list' => array(
								'Type' => VTBindVariable::TYPE_BYTES_LIST,
								'ValueBytesList' => array(
										new MongoBinData('one'),
										new MongoBinData('two') 
								) 
						),
						'int_list' => array(
								'Type' => VTBindVariable::TYPE_INT_LIST,
								'ValueIntList' => array(
										1,
										2,
										3 
								) 
						),
						'uint_list' => array(
								'Type' => VTBindVariable::TYPE_UINT_LIST,
								'ValueUintList' => array(
										123,
										456 
								) 
						),
						'float_list' => array(
								'Type' => VTBindVariable::TYPE_FLOAT_LIST,
								'ValueFloatList' => array(
										2.0,
										4.0 
								) 
						) 
				) 
		);
		
		$actual = VTBoundQuery::buildBsonP3('test query', array(
				'bytes' => 'hello',
				'int' => 123,
				'uint_from_int' => new VTUnsignedInt(345),
				'uint_from_string' => new VTUnsignedInt('678'),
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