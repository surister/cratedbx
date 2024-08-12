import datetime
import uuid

array = {
    'id': str(uuid.uuid4()),
    'txt': "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor "
           "incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud "
           "exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute "
           "irure dolor",
    'ip': '127.198.0.1',
    'i16': -32767,
    'u16': 32767,
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

