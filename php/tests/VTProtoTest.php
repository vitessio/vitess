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
						) 
				) 
		);
		
		$actual = VTBoundQuery::buildBsonP3('test query', array(
				'bytes' => 'hello',
				'int' => 123,
				'uint_from_int' => new VTUnsignedInt(345),
				'uint_from_string' => new VTUnsignedInt('678'),
				'float' => 1.5 
		));
		
		$this->assertEquals($expected, $actual);
	}
}