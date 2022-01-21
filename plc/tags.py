from abc import ABC, abstractmethod
from typing import List, Dict

from snap7.util import get_int, get_string, get_usint, get_bool, get_real, set_int, set_usint
from snap7.util import set_real, set_bool, set_string, set_dint, get_dint

from plc.plc_client import PLCClient
from plc.variables import TAG_SIZE, TAGS_COUNT_SIZE, TYPE_SIZE, TYPE_OFFSET, NAME_OFFSET, \
    NAME_SIZE, USE_SIZE, USE_OFFSET, READ_ONLY_OFFSET, DB_NUMBER_OFFSET, \
    OFFSETBYTE_OFFSET, OFFSETBIT_OFFSET
from plc.utils import set_uint, get_uint
from logs.logger import logger


class Tag(ABC):
    def __init__(self, use, read_only, db_number, offset_byte, offset_bit, size):
        self.plc_client = PLCClient('192.168.29.31')
        self.db_number = db_number
        self.use = use
        self.read_only = read_only
        self.offset_byte = offset_byte
        self.offset_bit = offset_bit
        self.size = size

    @abstractmethod
    def get(self):
        pass

    def set(self, value):
        # if self.read_only:
        #     raise PermissionError('This tag is read only!')
        pass


class BoolTag(Tag):

    def get(self):
        data = self.plc_client.read_data(self.db_number, self.offset_byte, self.size)
        return get_bool(data, 0, 0)

    def set(self, value: bool):
        super().set(value)

        data = bytearray(self.size)
        set_bool(data, byte_index=0, bool_index=0, value=value)

        return self.plc_client.write_data(self.db_number, self.offset_byte, data)


class RealTag(Tag):

    def get(self):
        data = self.plc_client.read_data(self.db_number, self.offset_byte, self.size)
        return get_real(data, 0)

    def set(self, value: float):
        super().set(value)

        data = bytearray(self.size)
        set_real(data, 0, value)

        return self.plc_client.write_data(self.db_number, self.offset_byte, data)


class IntTag(Tag):

    def get(self):
        data = self.plc_client.read_data(self.db_number, self.offset_byte, self.size)
        return get_int(data, 0)

    def set(self, value):
        super().set(value)

        data = bytearray(self.size)
        set_int(data, 0, value)

        return self.plc_client.write_data(self.db_number, self.offset_byte, data)


class UIntTag(Tag):

    def get(self):
        data = self.plc_client.read_data(self.db_number, self.offset_byte, self.size)
        return get_uint(data, 0)

    def set(self, value):
        super().set(value)

        data = bytearray(self.size)
        set_uint(data, 0, value)

        return self.plc_client.write_data(self.db_number, self.offset_byte, data)


class USIntTag(Tag):

    def get(self):
        data = self.plc_client.read_data(self.db_number, self.offset_byte, self.size)
        return get_usint(data, 0)

    def set(self, value):
        super().set(value)

        data = bytearray(self.size)
        set_usint(data, 0, value)

        return self.plc_client.write_data(self.db_number, self.offset_byte, data)


class LIntTag(Tag):
    def get(self):
        data = self.plc_client.read_data(self.db_number, self.offset_byte, self.size)
        return get_dint(data, 0)

    def set(self, value):
        super().set(value)

        data = bytearray(self.size)
        set_dint(data, 0, value)

        return self.plc_client.write_data(self.db_number, self.offset_byte, data)


class CharTag(Tag):
    def get(self):
        data = self.plc_client.read_data(self.db_number, self.offset_byte, self.size)
        return data.decode()

    def set(self, value: str):
        super().set(value)
        if len(value) > 1:
            raise ValueError("Must be only one symbol")

        data = value.encode('ascii')
        data = bytearray(data)

        return self.plc_client.write_data(self.db_number, self.offset_byte, data)


class StringTag(Tag):
    def get(self):
        data = self.plc_client.read_data(self.db_number, self.offset_byte, self.size)
        return get_string(data, 0, self.size)

    def set(self, value):
        super().set(value)

        data = bytearray(self.size)
        set_string(data, 0, value, self.size)

        return self.plc_client.write_data(self.db_number, self.offset_byte, data)


class ArrayTag(Tag):
    def get(self):
        data = self.plc_client.read_data(self.db_number, self.offset_byte, self.size * 200)  # BAD IDEA
        return [get_usint(data, i) for i in range(len(data))]

    def set(self, values: List):
        data = bytearray(self.size * 200)

        for i in range(len(values)):
            set_usint(data, i, values[i])

        return self.plc_client.write_data(self.db_number, self.offset_byte, data)


class TagsBuilder:
    def __init__(self, plc_client: PLCClient, db_tags):
        self.plc_client = plc_client
        self.db_tags = db_tags

    @property
    def tags_count(self):
        return get_int(self.plc_client.read_data(self.db_tags, 0, TAGS_COUNT_SIZE), 0)

    def get_typed_tag(self, name):
        if 'String' in name:
            return StringTag
        elif 'Array' in name:
            return ArrayTag
        else:
            plc_type_to_obj = {
                'Bool': BoolTag,
                'Real': RealTag,
                'Int': IntTag,
                'USInt': USIntTag,
                'UInt': UIntTag,
                'LInt': LIntTag,
                'Char': CharTag,
            }
            return plc_type_to_obj[name]

    def get_size(self, name):
        plc_type_to_obj = {
            'Bool': 1,
            'Real': 4,
            'Int': 2,
            'USInt': 1,
            'UInt': 2,
            'LInt': 4,
            'Char': 1,
            'Byte': 1,
        }

        if 'String' in name:
            return int(name.split('[')[1].split(']')[0])

        elif 'Array' in name:
            name = name.split()[-1]

        return plc_type_to_obj[name]

    def build_tag(self, tag_data):
        tag_type = get_string(tag_data, TYPE_OFFSET, TYPE_SIZE)
        typed_tag = self.get_typed_tag(tag_type)
        size = self.get_size(tag_type)

        tag_name = get_string(tag_data, NAME_OFFSET, NAME_SIZE)

        return {tag_name: typed_tag(
            use=get_string(tag_data, USE_OFFSET, USE_SIZE),
            read_only=bool(get_int(tag_data, READ_ONLY_OFFSET)),
            db_number=get_int(tag_data, DB_NUMBER_OFFSET),
            offset_byte=get_int(tag_data, OFFSETBYTE_OFFSET),
            offset_bit=get_usint(tag_data, OFFSETBIT_OFFSET),
            size=size
        )}

    def build_tags(self) -> Dict:
        tags = {}

        for i in range(self.tags_count):

            tag_data = self.plc_client.read_data(self.db_tags, TAGS_COUNT_SIZE + i * TAG_SIZE, TAG_SIZE)
            tag = self.build_tag(tag_data)
            tags.update(tag)

        logger.info(f'tags built successfully\n{tags.keys()}')
        return tags
