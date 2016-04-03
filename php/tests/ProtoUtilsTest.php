<?php
namespace Vitess;

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/TestUtils.php';

class ProtoUtilsTest extends \PHPUnit_Framework_TestCase
{

    public function testBoundQuery()
    {
        $expected = new Proto\Query\BoundQuery();
        $expected->setSql('test query');
        add_bind_var($expected, Proto\Query\Type::VARBINARY, 'bytes', 'hello');
        add_bind_var($expected, Proto\Query\Type::INT64, 'int', '123');
        add_bind_var($expected, Proto\Query\Type::UINT64, 'uint_from_int', '18446744073709551493'); // 18446744073709551493 = uint64(-123)
        add_bind_var($expected, Proto\Query\Type::UINT64, 'uint_from_string', '456');
        add_bind_var($expected, Proto\Query\Type::FLOAT64, 'float', '1.5');
        add_list_bind_var($expected, Proto\Query\Type::VARBINARY, 'bytes_list', array(
            'one',
            'two'
        ));
        add_list_bind_var($expected, Proto\Query\Type::INT64, 'int_list', array(
            '1',
            '2',
            '3'
        ));
        add_list_bind_var($expected, Proto\Query\Type::UINT64, 'uint_list', array(
            '123',
            '456'
        ));
        add_list_bind_var($expected, Proto\Query\Type::FLOAT64, 'float_list', array(
            '2.0',
            '4.0'
        ));

        $actual = ProtoUtils::BoundQuery('test query', array(
            'bytes' => 'hello',
            'int' => 123,
            'uint_from_int' => new UnsignedInt(- 123),
            'uint_from_string' => new UnsignedInt('456'),
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
                new UnsignedInt(123),
                new UnsignedInt('456')
            ),
            'float_list' => array(
                2.0,
                4.0
            )
        ));

        $this->assertEquals($expected, $actual);
    }

    public function testRowValues()
    {
        $row = new Proto\Query\Row();
        $row->setValues('onethree');
        $row->setLengths([
            3,
            - 1, // MySQL NULL
            5,
            0
        ]);
        $fields = [
            make_field('c1'),
            make_field('c2'),
            make_field('c3'),
            make_field('c4')
        ];
        $expected = [
            0 => 'one',
            1 => null,
            2 => 'three',
            3 => '',
            'c1' => 'one',
            'c2' => null,
            'c3' => 'three',
            'c4' => ''
        ];
        $actual = ProtoUtils::RowValues($row, $fields);
        $this->assertEquals($expected, $actual);
    }
}
