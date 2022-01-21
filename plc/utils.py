import struct


def set_uint(bytearray_: bytearray, byte_index: int, _int: int):
    """Set value in bytearray to unsigned int
    Notes:
        An datatype `uint` in the PLC consists of two `bytes`.
    Args:
        bytearray_: buffer to write on.
        byte_index: byte index to start writing from.
        _int: int value to write.
    Returns:
        Buffer with the written value.
    Examples:
        >>> data = bytearray(2)
        >>> snap7.util.set_uint(data, 0, 65535)
            bytearray(b'\\xff\\xff')
    """
    # make sure were dealing with an int
    _int = int(_int)
    _bytes = struct.unpack('2B', struct.pack('>H', _int))
    bytearray_[byte_index:byte_index + 2] = _bytes
    return bytearray_


def get_uint(bytearray_: bytearray, byte_index: int) -> int:
    """Get unsigned int value from bytearray.
    Notes:
        Datatype `uint` in the PLC is represented in two bytes
        Maximum posible value is 65535.
        Lower posible value is 0.
    Args:
        bytearray_: buffer to read from.
        byte_index: byte index to start reading from.
    Returns:
        Value read.
    Examples:
        >>> data = bytearray([255, 255])
        >>> snap7.util.get_uint(data, 0)
            65535
    """
    data = bytearray_[byte_index:byte_index + 2]
    data[1] = data[1] & 0xff
    data[0] = data[0] & 0xff
    packed = struct.pack('2B', *data)
    value = struct.unpack('>H', packed)[0]
    return value
