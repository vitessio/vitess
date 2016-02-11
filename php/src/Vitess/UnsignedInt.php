<?php
namespace Vitess;

/**
 * UnsignedInt is a wrapper used to tell the Vitess RPC layer that it should
 * encode as unsigned int.
 *
 * This is necessary because PHP doesn't have a real unsigned int type.
 *
 * You can pass in a signed int value, in which case the re-interpreted
 * 64-bit 2's complement value will be sent as an unsigned int.
 * For example:
 * new UnsignedInt(42) // will send 42
 * new UnsignedInt(-1) // will send 0xFFFFFFFFFFFFFFFF
 *
 * You can also pass in a string consisting of only decimal digits.
 * For example:
 * new UnsignedInt('12345') // will send 12345
 */
class UnsignedInt
{

    private $value;

    public function __construct($value)
    {
        if (is_int($value)) {
            $this->value = $value;
        } else
            if (is_string($value)) {
                if (! ctype_digit($value)) {
                    throw new Error\BadInput('Invalid string value given for UnsignedInt: ' . $value);
                }
                $this->value = $value;
            } else {
                throw new Error\BadInput('Unsupported type for UnsignedInt');
            }
    }

    public function __toString()
    {
        if (is_int($this->value)) {
            return sprintf('%u', $this->value);
        } else {
            return strval($this->value);
        }
    }
}
