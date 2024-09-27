import datetime
import uuid

array = {
    'id': str(uuid.uuid4()),
    'txt': "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor "
           "incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud "
           "exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute "
           "irure dolor",
    'txt_empty': "",
    'ip': '127.198.0.1',
    'ip_empty': '',
    'i16': -32767,
    'i16_null': None,
    'u16': 32767,
    'u16_null': None,
    'i32': -2147483647,  # CrateDB max value for INTEGER
    'u32': 2147483647,  # INTEGER
    'i32_from_text': '-2147483647',
    'f32': -0.2147481231,
    'f64': 0.18446744073709551615,

    # > 8byte, MongoDB can only support up to 8 bytes.
    # 'i64': -18446744073709551615,
    # 'u64': 18446744073709551615,
    # 'i128': -1340282366920938463463374607431768211456,
    # 'u128': 1340282366920938463463374607431768211456,

    'bool': True,
    'null_': None,

    'datetime': datetime.datetime.now(tz=None),
    'datetime_2': datetime.datetime.now(tz=datetime.UTC),
    'obj': {
        'one': 'two',
        'three': 4,
        'five': [6,],
        'seven': {'eight': 9}
    },
    # Missing geo shape.

    # Arrays
    'empty_array': [],
    # 'empty_nested_array': [
    #     [],
    #     []
    # ],
    # 'empty_nested_array_2': [
    #     [], [1, 2, 3, 4, 5, 6]
    # ],
    # 'empty_nested_array_3': [
    #     [1, 2, 3], [1, 2, 3, 4, 5, 6]
    # ],
    # 'empty_nested_array_4': [
    #     ['Hello', 'World'], [1, 2, 3, 4, 5, 6]
    # ],
    # 'empty_nested_array_5': [
    #     [1, 2, 3], [1, 2, 3, 4, 5, 6]
    # ],
    # 'array_mixed': [12345, "678919"],
    'vector_float_simple': [0.1],
    'vector_float': [0.23423423] * 2048,
    'array_i32': [-123456, -654321],
    'array_u32': [123456, 654321],
    'array_text': ["January", "February", "March", "April", "May", "June", "July",
                   "August", "September", "October", "November", "December"]
}

dirty_structured_arrays = [
    # {"k": {"one": [1, 2]}},
    # {"k": {"one": 1}}
    {"id": 1, "name": "one", "sub_id": 1},
    {"id": 2, "name": "two", "sub_id": 2},
    {"id": 3, "name": "three", "surname": "three", "sub_id": 3},
    {'id': 4, "name": ["four"], "sub_id": 4},
    {"id": 5, "another": 1, "sub_id": "5"},
    {"id": 6, "name": {'key': 'six', 'key2': [1, 2, 3, 4]}, "sub_id": "6"},
    {"id": 7, "name": 3, "sub_id": 7},
    {"id": 8, "name": ["eight"], "sub_id": "8"},
    {"id": 9, "sub_id": 9, },
    {"id": 10, "name": {"key": [1, 2, 3, 4]}},
    {"id": 11, "name": [1, 2, 3, "4", [1, 2, 3, True]]}
]

#     {"id": 11, "name": [1, 2, 3, "4", [1, 2, 3, True]]}
# {
#   name_str: "4",
#   name_array: [1,2,3]
#   name_bool: True
# }

# memory issues
# rows/s

# STRATEGY
# INLINE_NEW_COLUMN {column_name}_{new_type} - DOESN'T ALTER TABLE
# NEW_COLUMN {column_name}_{new_type} - ALTERS TABLE