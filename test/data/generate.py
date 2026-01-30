#!/usr/bin/env python3

import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
from datetime import datetime, date, timedelta

# Define explicit schema to avoid large_string (which parquet_fdw doesn't support)
example1_schema = pa.schema([
    pa.field('one', pa.int64()),
    pa.field('two', pa.list_(pa.int64())),
    pa.field('three', pa.string()),  # Use string, not large_string
    pa.field('four', pa.timestamp('us')),
    pa.field('five', pa.date32()),
    pa.field('six', pa.bool_()),
    pa.field('seven', pa.float64()),
])

# example1.parquet file
df1 = pd.DataFrame({'one': [1, 2, 3],
                    'two': [[1, 2, 3], [None, 5, 6], [7, 8, 9]],
                    'three': ['foo', 'bar', 'baz'],
                    'four': [datetime(2018, 1, 1),
                             datetime(2018, 1, 2),
                             datetime(2018, 1, 3)],
                    'five': [date(2018, 1, 1),
                             date(2018, 1, 2),
                             date(2018, 1, 3)],
                    'six': [True, False, True],
                    'seven': [0.5, None, 1.0]})
table1 = pa.Table.from_pandas(df1, schema=example1_schema, preserve_index=False)

df2 = pd.DataFrame({'one': [4, 5, 6],
                    'two': [[10, 11, 12], [13, 14, 15], [16, 17, 18]],
                    'three': ['uno', 'dos', 'tres'],
                    'four': [datetime(2018, 1, 4)+timedelta(seconds=10),
                             datetime(2018, 1, 5)+timedelta(milliseconds=10),
                             datetime(2018, 1, 6)+timedelta(microseconds=10)],
                    'five': [date(2018, 1, 4),
                             date(2018, 1, 5),
                             date(2018, 1, 6)],
                    'six': [False, False, False],
                    'seven': [1.5, None, 2.0]})
table2 = pa.Table.from_pandas(df2, schema=example1_schema, preserve_index=False)

with pq.ParquetWriter('simple/example1.parquet', example1_schema) as writer:
    writer.write_table(table1)
    writer.write_table(table2)

# example2.parquet file - schema without 'seven' column
example2_schema = pa.schema([
    pa.field('one', pa.int64()),
    pa.field('two', pa.list_(pa.int64())),
    pa.field('three', pa.string()),  # Use string, not large_string
    pa.field('four', pa.timestamp('us')),
    pa.field('five', pa.date32()),
    pa.field('six', pa.bool_()),
])

df3 = pd.DataFrame({'one': [1, 3, 5, 7, 9],
                    'two': [[19, 20], [21, 22], [23, 24], [25, 26], [27, 28]],
                    'three': ['eins', 'zwei', 'drei', 'vier', 'fünf'],
                    'four': [datetime(2018, 1, 1),
                             datetime(2018, 1, 3),
                             datetime(2018, 1, 5),
                             datetime(2018, 1, 7),
                             datetime(2018, 1, 9)],
                    'five': [date(2018, 1, 1),
                             date(2018, 1, 3),
                             date(2018, 1, 5),
                             date(2018, 1, 7),
                             date(2018, 1, 9)],
                    'six': [True, False, True, False, True]})
table3 = pa.Table.from_pandas(df3, schema=example2_schema, preserve_index=False)

# an empty data frame to test corner case
df4 = df3.drop([0, 1, 2, 3, 4])
table4 = pa.Table.from_pandas(df4, schema=example2_schema, preserve_index=False)

with pq.ParquetWriter('simple/example2.parquet', example2_schema) as writer:
    writer.write_table(table3)
    writer.write_table(table4)

# example3.parquet file
mdt1 = pa.map_(pa.int32(), pa.string())
mdt2 = pa.map_(pa.date32(), pa.int16())
df = pd.DataFrame({
        'one': pd.Series([
            [(1, 'foo'), (2, 'bar'), (3, 'baz')],
            [(4, 'test1'), (5, 'test2')],
        ]),
        'two': pd.Series([
            [(date(2018, 1, 1), 10), (date(2018, 1, 2), 15)],
            [(date(2018, 1, 3), 20), (date(2018, 1, 4), 25)],
        ]),
        'three': pd.Series([1, 2]),
    }
)

schema = pa.schema([
    pa.field('one', mdt1),
    pa.field('two', mdt2),
    pa.field('three', pa.int32())])
table = pa.Table.from_pandas(df, schema)

with pq.ParquetWriter('complex/example3.parquet', table.schema) as writer:
    writer.write_table(table)

# Parquet files for partitions
partition_schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('token', pa.int64()),
    pa.field('date', pa.timestamp('us')),
    pa.field('num', pa.int64()),
])

df_part1 = pd.DataFrame({'id': [1, 1, 2, 3],
                         'token': [1, 1, 2, 2],
                         'date': [datetime(2018, 1, 1),
                                  datetime(2018, 1, 2),
                                  datetime(2018, 1, 3),
                                  datetime(2018, 1, 4)],
                         'num': [10, 23, 9, 38]})
table_part1 = pa.Table.from_pandas(df_part1, schema=partition_schema, preserve_index=False)

with pq.ParquetWriter(
        'partition/example_part1.parquet', partition_schema) as writer:
    writer.write_table(table_part1)

df_part2 = pd.DataFrame({'id': [1, 2, 2, 3],
                         'token': [1, 2, 2, 2],
                         'date': [datetime(2018, 2, 1),
                                  datetime(2018, 2, 2),
                                  datetime(2018, 2, 3),
                                  datetime(2018, 2, 4)],
                         'num': [59, 1, 32, 96]})
table_part2 = pa.Table.from_pandas(df_part2, schema=partition_schema, preserve_index=False)

with pq.ParquetWriter(
        'partition/example_part2.parquet', partition_schema) as writer:
    writer.write_table(table_part2)

# UUID test file (using fixed-size binary of 16 bytes)
import uuid

# Generate some fixed UUIDs for predictable test results
uuid1 = uuid.UUID('550e8400-e29b-41d4-a716-446655440000')
uuid2 = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
uuid3 = uuid.UUID('6ba7b811-9dad-11d1-80b4-00c04fd430c8')
uuid4 = uuid.UUID('00000000-0000-0000-0000-000000000000')  # nil UUID
uuid5 = uuid.UUID('ffffffff-ffff-ffff-ffff-ffffffffffff')  # max UUID

# Create fixed-size binary array for UUIDs
uuid_data = [
    uuid1.bytes,
    uuid2.bytes,
    uuid3.bytes,
    uuid4.bytes,
    uuid5.bytes,
]

uuid_array = pa.FixedSizeBinaryArray.from_buffers(
    pa.binary(16),
    len(uuid_data),
    [None, pa.py_buffer(b''.join(uuid_data))]
)

# Create table with UUID and other columns
uuid_table = pa.table({
    'id': pa.array([1, 2, 3, 4, 5], type=pa.int32()),
    'uuid_col': uuid_array,
    'name': pa.array(['first', 'second', 'third', 'nil', 'max'], type=pa.string()),
})

with pq.ParquetWriter('simple/example_uuid.parquet', uuid_table.schema) as writer:
    writer.write_table(uuid_table)

# Hive-partitioned test data
# This creates a directory structure like:
#   hive/basic/year=2023/month=1/data.parquet
#   hive/basic/year=2023/month=2/data.parquet
#   hive/basic/year=2024/month=1/data.parquet
#   hive/basic/year=2024/month=2/data.parquet
import os

hive_basic_schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('amount', pa.float64()),
    pa.field('name', pa.string()),
])

# Create hive directory structure
hive_partitions = [
    ('2023', '1', [
        {'id': 1, 'amount': 100.0, 'name': 'Alice'},
        {'id': 2, 'amount': 200.0, 'name': 'Bob'},
    ]),
    ('2023', '2', [
        {'id': 3, 'amount': 150.0, 'name': 'Charlie'},
        {'id': 4, 'amount': 250.0, 'name': 'Diana'},
    ]),
    ('2024', '1', [
        {'id': 5, 'amount': 175.0, 'name': 'Eve'},
        {'id': 6, 'amount': 225.0, 'name': 'Frank'},
    ]),
    ('2024', '2', [
        {'id': 7, 'amount': 125.0, 'name': 'Grace'},
        {'id': 8, 'amount': 275.0, 'name': 'Henry'},
    ]),
]

for year, month, data in hive_partitions:
    dir_path = f'hive/basic/year={year}/month={month}'
    os.makedirs(dir_path, exist_ok=True)

    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df, schema=hive_basic_schema, preserve_index=False)

    with pq.ParquetWriter(f'{dir_path}/data.parquet', hive_basic_schema) as writer:
        writer.write_table(table)

# Hive partitioned data with region text partition
# This creates: hive/region/region=US/data.parquet, hive/region/region=EU/data.parquet
hive_region_schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('sales', pa.float64()),
])

region_partitions = [
    ('US', [
        {'id': 1, 'sales': 1000.0},
        {'id': 2, 'sales': 1500.0},
    ]),
    ('EU', [
        {'id': 3, 'sales': 2000.0},
        {'id': 4, 'sales': 2500.0},
    ]),
]

for region, data in region_partitions:
    dir_path = f'hive/region/region={region}'
    os.makedirs(dir_path, exist_ok=True)

    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df, schema=hive_region_schema, preserve_index=False)

    with pq.ParquetWriter(f'{dir_path}/data.parquet', hive_region_schema) as writer:
        writer.write_table(table)

print("Generated Hive partitioned test data in hive/")

# Time64 and Decimal test data
from datetime import time
from decimal import Decimal as PyDecimal

# Create test data for Time64 (microseconds and nanoseconds)
# and Decimal128/Decimal256 types
time_decimal_schema = pa.schema([
    pa.field('id', pa.int32()),
    pa.field('time_us', pa.time64('us')),      # Time64 in microseconds
    pa.field('time_ns', pa.time64('ns')),      # Time64 in nanoseconds
    pa.field('price', pa.decimal128(10, 2)),   # Decimal128 with precision 10, scale 2
    pa.field('amount', pa.decimal128(18, 6)),  # Decimal128 with precision 18, scale 6
    pa.field('name', pa.string()),
])

# Time values in microseconds since midnight
# 12:30:45.123456 = (12*3600 + 30*60 + 45) * 1000000 + 123456
time_us_values = [
    (12 * 3600 + 30 * 60 + 45) * 1000000 + 123456,  # 12:30:45.123456
    (8 * 3600 + 15 * 60 + 30) * 1000000 + 500000,   # 08:15:30.500000
    (23 * 3600 + 59 * 60 + 59) * 1000000 + 999999,  # 23:59:59.999999
    0,                                               # 00:00:00.000000
    (18 * 3600 + 0 * 60 + 0) * 1000000,             # 18:00:00.000000
]

# Time values in nanoseconds since midnight
time_ns_values = [
    (12 * 3600 + 30 * 60 + 45) * 1000000000 + 123456789,  # 12:30:45.123456789
    (8 * 3600 + 15 * 60 + 30) * 1000000000 + 500000000,   # 08:15:30.500000000
    (23 * 3600 + 59 * 60 + 59) * 1000000000 + 999999999,  # 23:59:59.999999999
    0,                                                     # 00:00:00.000000000
    (18 * 3600 + 0 * 60 + 0) * 1000000000 + 1000,         # 18:00:00.000001000
]

# Create the table with explicit arrays
time_decimal_table = pa.table({
    'id': pa.array([1, 2, 3, 4, 5], type=pa.int32()),
    'time_us': pa.array(time_us_values, type=pa.time64('us')),
    'time_ns': pa.array(time_ns_values, type=pa.time64('ns')),
    'price': pa.array([
        PyDecimal('123.45'),
        PyDecimal('99999999.99'),
        PyDecimal('-50.00'),
        PyDecimal('0.01'),
        PyDecimal('0.00'),
    ], type=pa.decimal128(10, 2)),
    'amount': pa.array([
        PyDecimal('123456.789012'),
        PyDecimal('999999999999.999999'),
        PyDecimal('-0.000001'),
        PyDecimal('0.000000'),
        PyDecimal('1.500000'),
    ], type=pa.decimal128(18, 6)),
    'name': pa.array(['first', 'second', 'third', 'fourth', 'fifth'], type=pa.string()),
})

with pq.ParquetWriter('simple/example_time_decimal.parquet', time_decimal_schema) as writer:
    writer.write_table(time_decimal_table)

print("Generated Time64 and Decimal test data in simple/example_time_decimal.parquet")

# Test data for IMPORT FOREIGN SCHEMA with tables_partition_map
# Creates two tables with different date columns but same partition structure:
#   hive/import_test/table1/year=YYYY/month=M/data.parquet (contains event_date)
#   hive/import_test/table2/year=YYYY/month=M/data.parquet (contains data_date)

# table1: has event_date column
table1_schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('event_date', pa.date32()),
    pa.field('value', pa.float64()),
])

table1_partitions = [
    ('2023', '1', [
        {'id': 1, 'event_date': date(2023, 1, 15), 'value': 100.0},
        {'id': 2, 'event_date': date(2023, 1, 20), 'value': 200.0},
    ]),
    ('2023', '6', [
        {'id': 3, 'event_date': date(2023, 6, 10), 'value': 150.0},
        {'id': 4, 'event_date': date(2023, 6, 25), 'value': 250.0},
    ]),
    ('2024', '3', [
        {'id': 5, 'event_date': date(2024, 3, 5), 'value': 175.0},
        {'id': 6, 'event_date': date(2024, 3, 28), 'value': 225.0},
    ]),
]

for year, month, data in table1_partitions:
    dir_path = f'hive/import_test/table1/year={year}/month={month}'
    os.makedirs(dir_path, exist_ok=True)

    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df, schema=table1_schema, preserve_index=False)

    with pq.ParquetWriter(f'{dir_path}/data.parquet', table1_schema) as writer:
        writer.write_table(table)

# table2: has data_date column (different column name)
table2_schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('data_date', pa.date32()),
    pa.field('amount', pa.float64()),
])

table2_partitions = [
    ('2023', '2', [
        {'id': 101, 'data_date': date(2023, 2, 10), 'amount': 1000.0},
        {'id': 102, 'data_date': date(2023, 2, 28), 'amount': 2000.0},
    ]),
    ('2023', '8', [
        {'id': 103, 'data_date': date(2023, 8, 15), 'amount': 1500.0},
        {'id': 104, 'data_date': date(2023, 8, 30), 'amount': 2500.0},
    ]),
    ('2024', '5', [
        {'id': 105, 'data_date': date(2024, 5, 1), 'amount': 1750.0},
        {'id': 106, 'data_date': date(2024, 5, 20), 'amount': 2250.0},
    ]),
]

for year, month, data in table2_partitions:
    dir_path = f'hive/import_test/table2/year={year}/month={month}'
    os.makedirs(dir_path, exist_ok=True)

    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df, schema=table2_schema, preserve_index=False)

    with pq.ParquetWriter(f'{dir_path}/data.parquet', table2_schema) as writer:
        writer.write_table(table)

print("Generated import_test tables with different date columns in hive/import_test/")

# =============================================================================
# Edge case test data
# =============================================================================

# Test 1: URL-encoded partition values
# Create partition with special characters that need URL encoding
# Directory: hive/url_encoded/region=North%20America/data.parquet
url_encoded_schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('sales', pa.float64()),
])

# Note: The directory name itself contains the encoded value
url_encoded_dir = 'hive/url_encoded/region=North%20America'
os.makedirs(url_encoded_dir, exist_ok=True)

url_encoded_data = pd.DataFrame({
    'id': [1, 2, 3],
    'sales': [1000.0, 2000.0, 3000.0],
})
url_encoded_table = pa.Table.from_pandas(url_encoded_data, schema=url_encoded_schema, preserve_index=False)

with pq.ParquetWriter(f'{url_encoded_dir}/data.parquet', url_encoded_schema) as writer:
    writer.write_table(url_encoded_table)

print("Generated URL-encoded partition test data in hive/url_encoded/")

# Test 2: Three-level partition (year/month/day)
# Directory: hive/three_level/year=2025/month=01/day=15/data.parquet
three_level_schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('value', pa.float64()),
])

three_level_partitions = [
    ('2025', '01', '15', [{'id': 1, 'value': 100.0}, {'id': 2, 'value': 200.0}]),
    ('2025', '01', '20', [{'id': 3, 'value': 150.0}, {'id': 4, 'value': 250.0}]),
    ('2025', '02', '10', [{'id': 5, 'value': 175.0}, {'id': 6, 'value': 225.0}]),
]

for year, month, day, data in three_level_partitions:
    dir_path = f'hive/three_level/year={year}/month={month}/day={day}'
    os.makedirs(dir_path, exist_ok=True)

    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df, schema=three_level_schema, preserve_index=False)

    with pq.ParquetWriter(f'{dir_path}/data.parquet', three_level_schema) as writer:
        writer.write_table(table)

print("Generated three-level partition test data in hive/three_level/")

# Test 3: Column conflict - column exists in both Parquet file AND partition path
# Directory: hive/conflict/year=2099/data.parquet
# The Parquet file contains a 'year' column with value 2025 (different from path's 2099)
conflict_schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('year', pa.int32()),  # This column is also in the partition path!
    pa.field('value', pa.float64()),
])

conflict_dir = 'hive/conflict/year=2099'
os.makedirs(conflict_dir, exist_ok=True)

# The file's year column has values 2025, 2026, 2027 - different from path year=2099
conflict_data = pd.DataFrame({
    'id': [1, 2, 3],
    'year': [2025, 2026, 2027],  # These values are in the Parquet file
    'value': [100.0, 200.0, 300.0],
})
conflict_table = pa.Table.from_pandas(conflict_data, schema=conflict_schema, preserve_index=False)

with pq.ParquetWriter(f'{conflict_dir}/data.parquet', conflict_schema) as writer:
    writer.write_table(conflict_table)

print("Generated column conflict test data in hive/conflict/")

# Test 4: Range spanning test - data for testing BETWEEN queries across partition boundaries
# Directory structure with event_date column that spans multiple months
# Used to verify that range queries like BETWEEN '2025-01-15' AND '2025-02-15'
# correctly include both month=01 and month=02 partitions
range_test_schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('event_date', pa.date32()),
    pa.field('value', pa.float64()),
])

range_test_partitions = [
    ('2025', '01', [
        {'id': 1, 'event_date': date(2025, 1, 10), 'value': 100.0},
        {'id': 2, 'event_date': date(2025, 1, 20), 'value': 200.0},
        {'id': 3, 'event_date': date(2025, 1, 31), 'value': 300.0},
    ]),
    ('2025', '02', [
        {'id': 4, 'event_date': date(2025, 2, 1), 'value': 400.0},
        {'id': 5, 'event_date': date(2025, 2, 15), 'value': 500.0},
        {'id': 6, 'event_date': date(2025, 2, 28), 'value': 600.0},
    ]),
    ('2025', '03', [
        {'id': 7, 'event_date': date(2025, 3, 1), 'value': 700.0},
        {'id': 8, 'event_date': date(2025, 3, 15), 'value': 800.0},
    ]),
]

for year, month, data in range_test_partitions:
    dir_path = f'hive/range_test/year={year}/month={month}'
    os.makedirs(dir_path, exist_ok=True)

    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df, schema=range_test_schema, preserve_index=False)

    with pq.ParquetWriter(f'{dir_path}/data.parquet', range_test_schema) as writer:
        writer.write_table(table)

print("Generated range spanning test data in hive/range_test/")
