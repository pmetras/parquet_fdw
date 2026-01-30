[![build](https://github.com/adjust/parquet_fdw/actions/workflows/ci.yml/badge.svg)](https://github.com/adjust/parquet_fdw/actions/workflows/ci.yml) ![experimental](https://img.shields.io/badge/status-experimental-orange)

# parquet_fdw

A read-only PostgreSQL Foreign Data Wrapper (FDW) for Apache Parquet files.

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Supported Data Types](#supported-data-types)
- [Table Options](#table-options)
- [File Patterns and Globbing](#file-patterns-and-globbing)
- [Hive Partitioning](#hive-partitioning)
- [Execution Strategies](#execution-strategies)
- [Parallel Query Execution](#parallel-query-execution)
- [Schema Import](#schema-import)
- [Security](#security)
- [Configuration](#configuration)
- [Packaging](#packaging)

## Requirements

- PostgreSQL 12-18 (versions 10-11 are end-of-life and no longer tested)
- C++17 compatible compiler
- Apache Arrow 0.15+ (`libarrow` and `libparquet` libraries)

## Installation

The extension requires `libarrow` and `libparquet` installed on your system. For Arrow versions prior to 0.15, use the [arrow-0.14](https://github.com/adjust/parquet_fdw/tree/arrow-0.14) branch.

Refer to the [Apache Arrow installation page](https://arrow.apache.org/install/) or the [building guide](https://github.com/apache/arrow/blob/master/docs/source/developers/cpp/building.rst) for installation instructions.

To build and install `parquet_fdw`:

```sh
make install
```

If PostgreSQL is installed in a custom location:

```sh
make install PG_CONFIG=/path/to/pg_config
```

You can pass additional compilation flags through the `CCFLAGS`, `PG_CFLAGS`, `PG_CXXFLAGS`, or `PG_CPPFLAGS` variables.

After successful installation, enable the extension in PostgreSQL:

```sql
CREATE EXTENSION parquet_fdw;
```

## Quick Start

First, create a foreign server and user mapping:

```sql
CREATE SERVER parquet_srv FOREIGN DATA WRAPPER parquet_fdw;
CREATE USER MAPPING FOR CURRENT_USER SERVER parquet_srv OPTIONS (user 'postgres');
```

Then create a foreign table pointing to your Parquet file:

```sql
CREATE FOREIGN TABLE users (
    id           INT,
    first_name   TEXT,
    last_name    TEXT
)
SERVER parquet_srv
OPTIONS (
    filename '/data/users.parquet'
);

-- Query the data
SELECT * FROM users WHERE id < 100;
```

## Supported Data Types

The following Arrow/Parquet types are mapped to PostgreSQL types:

| Arrow Type         | PostgreSQL Type                  |
|-------------------:|---------------------------------:|
| INT8               | INT2                             |
| INT16              | INT2                             |
| INT32              | INT4                             |
| INT64              | INT8                             |
| FLOAT              | FLOAT4                           |
| DOUBLE             | FLOAT8                           |
| TIMESTAMP          | TIMESTAMP                        |
| TIME64             | TIME                             |
| DATE32             | DATE                             |
| DECIMAL128         | NUMERIC                          |
| DECIMAL256         | NUMERIC                          |
| STRING             | TEXT                             |
| BINARY             | BYTEA                            |
| LIST               | ARRAY                            |
| MAP                | JSONB                            |
| UUID               | UUID                             |
| FIXED_SIZE_BINARY  | UUID (if 16 bytes) or BYTEA      |

**Note:** Structs and nested lists are not currently supported.

### Type Conversion Details

**TIME64:**
- Parquet TIME64 can store values in microsecond or nanosecond resolution.
- PostgreSQL TIME has microsecond resolution.
- Nanosecond values are **rounded to the nearest microsecond** (not truncated).
- Values outside the valid range (00:00:00 to 24:00:00) emit a WARNING and are clamped.

**DECIMAL128/DECIMAL256:**
- Parquet DECIMAL128 supports up to 38 digits of precision; DECIMAL256 supports up to 76 digits.
- PostgreSQL NUMERIC supports up to 1000 digits.
- If the Parquet column has higher precision than the PostgreSQL column definition, a WARNING is emitted once per column to alert about potential precision loss.
- Row group statistics filtering is not currently supported for DECIMAL types.

## Table Options

The following options can be specified when creating a foreign table:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `filename` | string | (required) | Space-separated list of paths to Parquet files. Supports [glob patterns](#file-patterns-and-globbing). |
| `sorted` | string | (none) | Space-separated list of columns the files are pre-sorted by. Enables optimizations for `ORDER BY`, `GROUP BY`, and merge joins. |
| `files_in_order` | boolean | false | When true, indicates files are ordered according to `sorted` with no overlapping ranges. Enables `Gather Merge` for parallel scans. |
| `use_mmap` | boolean | false | Use memory-mapped I/O instead of standard file reads. |
| `use_threads` | boolean | false | Enable Apache Arrow's parallel column decoding and decompression. |
| `max_open_files` | integer | (none) | Maximum number of Parquet files to keep open simultaneously. |
| `files_func` | regproc | (none) | User-defined function returning file paths. Must accept `JSONB` and return `TEXT[]`. |
| `files_func_arg` | string | (none) | JSONB argument passed to `files_func`. |
| `hive_partitioning` | boolean | false | Enable [Hive-style partitioning](#hive-partitioning). |
| `partition_columns` | string | (auto) | Space-separated list of partition column names. Overrides auto-discovery. |
| `partition_map` | string | (none) | Map table columns to partition keys. See [Column-to-Partition Mapping](#column-to-partition-mapping). |

### Example with Multiple Options

```sql
CREATE FOREIGN TABLE sales (
    id        INT,
    amount    NUMERIC(10,2),
    date      DATE,
    region    TEXT
)
SERVER parquet_srv
OPTIONS (
    filename '/data/sales/*.parquet',
    sorted 'date region',
    use_threads 'true',
    max_open_files '10'
);
```

## File Patterns and Globbing

The `filename` option supports POSIX glob patterns for matching multiple files:

| Pattern | Description | Example |
|---------|-------------|---------|
| `*` | Match any characters in a path component | `/data/*.parquet` |
| `?` | Match single character | `/data/file?.parquet` |
| `{a,b}` | Match alternatives (brace expansion) | `/data/{2023,2024}/*.parquet` |

**Note:** The recursive pattern `**` (globstar) is NOT supported by POSIX glob. Use explicit wildcards for each directory level instead (e.g., `year=*/month=*/*.parquet`).

**Examples:**

```sql
-- All Parquet files in a directory
OPTIONS (filename '/data/sales/*.parquet')

-- Match files in Hive partition directories
OPTIONS (filename '/data/year=*/month=*/*.parquet')

-- Specific years using brace expansion
OPTIONS (filename '/data/year={2023,2024}/month=*/*.parquet')

-- Multiple directories (space-separated)
OPTIONS (filename '/data/2023/*.parquet /data/2024/*.parquet')
```

## Hive Partitioning

Hive-style partitioning encodes partition values in directory paths using `key=value` segments. When enabled, these values are automatically extracted and exposed as virtual columns.

### Directory Structure Example

```
/data/sales/year=2023/month=1/data.parquet
/data/sales/year=2023/month=2/data.parquet
/data/sales/year=2024/month=1/data.parquet
```

### Basic Usage (Virtual Columns)

When partition columns don't exist in the Parquet file, they become virtual columns populated from the directory path:

```sql
CREATE FOREIGN TABLE sales (
    id      INT,
    amount  NUMERIC(10,2),
    name    TEXT,
    year    INT,    -- Virtual column from path
    month   INT     -- Virtual column from path
)
SERVER parquet_srv
OPTIONS (
    filename '/data/sales/year=*/month=*/*.parquet',
    hive_partitioning 'true'
);
```

### Partition Pruning

Filters on partition columns skip files that don't match, improving query performance:

```sql
-- Only scans year=2023 directories
SELECT * FROM sales WHERE year = 2023;

-- Only scans year=2024/month=1 directory
SELECT * FROM sales WHERE year = 2024 AND month = 1;
```

### Explicit Partition Columns

Override automatic partition column detection:

```sql
OPTIONS (
    filename '/data/sales/year=*/month=*/*.parquet',
    hive_partitioning 'true',
    partition_columns 'year month'
)
```

### Column-to-Partition Mapping

When Parquet files contain date columns but directories are partitioned by date components (year, month, day), use `partition_map` to enable pruning based on the date column:

```sql
-- Directory: /data/events/year=2024/month=10/data.parquet
-- Parquet file contains: event_date column with full dates

CREATE FOREIGN TABLE events (
    id          INT,
    event_date  DATE,    -- Exists in Parquet file
    payload     TEXT
)
SERVER parquet_srv
OPTIONS (
    filename '/data/events/year=*/month=*/*.parquet',
    hive_partitioning 'true',
    partition_map 'year={YEAR(event_date)}, month={MONTH(event_date)}'
);

-- Prunes to year=2024/month=10 based on the date filter
SELECT * FROM events WHERE event_date = '2024-10-15';

-- Scans year=2024/month=10 and month=11
SELECT * FROM events WHERE event_date BETWEEN '2024-10-01' AND '2024-11-30';
```

**Supported extraction functions:**

| Function | Description |
|----------|-------------|
| `YEAR(column)` | Extract year from DATE/TIMESTAMP |
| `MONTH(column)` | Extract month (1-12) from DATE/TIMESTAMP |
| `DAY(column)` | Extract day (1-31) from DATE/TIMESTAMP |
| `column` | Use column value directly (identity) |

### Multi-Level Partitioning

Hive partitioning supports multiple nesting levels. For example, a three-level partition structure:

```
/data/events/year=2025/month=01/day=15/data.parquet
/data/events/year=2025/month=01/day=20/data.parquet
/data/events/year=2025/month=02/day=10/data.parquet
```

```sql
CREATE FOREIGN TABLE events (
    id      INT,
    value   NUMERIC,
    year    INT,    -- from path
    month   INT,    -- from path
    day     INT     -- from path
)
SERVER parquet_srv
OPTIONS (
    filename '/data/events/year=*/month=*/day=*/*.parquet',
    hive_partitioning 'true'
);

-- Prunes at each level
SELECT * FROM events WHERE year = 2025 AND month = 1 AND day = 15;
```

### Range Queries Spanning Partitions

When using `partition_map`, range queries (BETWEEN, >=, <=) correctly include all partitions that may contain matching data:

```sql
-- With partition_map 'year={YEAR(event_date)},month={MONTH(event_date)}'

-- This query spans January and February 2025
-- Both month=01 and month=02 partitions are scanned
SELECT * FROM events
WHERE event_date BETWEEN '2025-01-15' AND '2025-02-15';
```

The FDW translates the date range to partition boundaries and includes all partitions that could contain matching rows.

### Partition Value Handling

- **Type inference:** Numeric-looking values become integers; others become text.
- **NULL values:** The special value `__HIVE_DEFAULT_PARTITION__` is converted to SQL NULL.
- **URL encoding:** Percent-encoded values are decoded automatically (e.g., `%20` → space, `%2F` → `/`).
- **Type validation:** Invalid partition values (e.g., non-numeric text for INT columns) raise proper PostgreSQL errors instead of silently converting to 0.

```sql
-- Directory: region=North%20America/data.parquet
-- The region column will contain "North America" (decoded)
SELECT * FROM sales WHERE region = 'North America';

-- Directory: category=__HIVE_DEFAULT_PARTITION__/data.parquet
-- The category column will be NULL for these rows
SELECT * FROM data WHERE category IS NULL;
```

### Column Conflict: File vs Path

When a column name exists in both the partition path and the Parquet file:

- **Column values** are read from the Parquet file (file takes precedence)
- **Partition pruning** uses the path values

```sql
-- Directory: year=2099/data.parquet
-- Parquet file contains: id, year (with values 2025, 2026, 2027), value

CREATE FOREIGN TABLE conflict_example (
    id      INT,
    year    INT,    -- exists in BOTH path and file
    value   NUMERIC
)
SERVER parquet_srv
OPTIONS (
    filename '/data/year=2099/*.parquet',
    hive_partitioning 'true'
);

-- Returns years 2025, 2026, 2027 (from Parquet file)
SELECT year FROM conflict_example;

-- Returns 0 rows! Partition pruning uses path year=2099, which doesn't match
SELECT * FROM conflict_example WHERE year = 2025;
```

**Recommendation:** Avoid column name conflicts between partition paths and Parquet file columns. If conflicts exist, filters on the conflicting column will use path values for pruning, which may unexpectedly exclude files.

## Execution Strategies

The FDW automatically selects an execution strategy based on file count and options:

| Strategy | When Used |
|----------|-----------|
| **Single File** | One file specified |
| **Multifile** | Multiple files, processed sequentially |
| **Multifile Merge** | Multiple pre-sorted files with `sorted` option; produces ordered results |
| **Caching Multifile Merge** | Same as Multifile Merge, but limits open files when count exceeds `max_open_files` |

## Parallel Query Execution

The extension supports PostgreSQL's [parallel query execution](https://www.postgresql.org/docs/current/parallel-query.html). This is separate from Apache Arrow's multi-threaded decoding (controlled by `use_threads`).

To enable parallel scans across ordered files, use:

```sql
OPTIONS (
    filename '/data/sorted/*.parquet',
    sorted 'date',
    files_in_order 'true'
)
```

## Schema Import

### Using IMPORT FOREIGN SCHEMA

Automatically create foreign tables from Parquet files in a directory:

```sql
IMPORT FOREIGN SCHEMA "/path/to/data"
FROM SERVER parquet_srv
INTO public;
```

**Note:** The schema path must be double-quoted as it represents a filesystem path.

### Import Options

| Option | Type | Description |
|--------|------|-------------|
| `sorted` | string | Apply to all imported tables |
| `hive_partitioning` | boolean | Enable for all imported tables |
| `partition_map` | string | Apply same mapping to all tables |
| `tables_map` | string | Map table names to file patterns |
| `tables_partition_map` | string | Per-table partition mappings |

### Mapping Tables to Files

Use `tables_map` to specify which files belong to which table:

```sql
IMPORT FOREIGN SCHEMA "/data"
FROM SERVER parquet_srv
INTO public
OPTIONS (
    tables_map 'orders=/data/orders/*.parquet customers=/data/customers/*.parquet'
);
```

**Syntax:** Space-separated `tablename=pattern` pairs. Use `:` to specify multiple patterns for one table:

```sql
OPTIONS (
    tables_map 'sales=/data/sales/year=2023/month=*/*.parquet:/data/sales/year=2024/month=*/*.parquet'
)
```

### Import with Hive Partitioning

Import tables with Hive partitioning enabled and a global partition map:

```sql
IMPORT FOREIGN SCHEMA "/data/events"
FROM SERVER parquet_srv
INTO public
OPTIONS (
    hive_partitioning 'true',
    partition_map 'year={YEAR(event_date)}, month={MONTH(event_date)}'
);
```

### Per-Table Partition Mappings

When different tables have different date column names, use `tables_partition_map` combined with `tables_map` to specify both the file locations and the column mappings for each table.

**Example scenario:**
- `/data/table1/year=*/month=*/*.parquet` - Parquet files contain an `event_date` column
- `/data/table2/year=*/month=*/*.parquet` - Parquet files contain a `data_date` column

Both tables are partitioned by year/month directories, but the date column has a different name in each table.

```sql
IMPORT FOREIGN SCHEMA "/data"
FROM SERVER parquet_srv
INTO public
OPTIONS (
    hive_partitioning 'true',
    tables_map 'table1=/data/table1/year=*/month=*/*.parquet table2=/data/table2/year=*/month=*/*.parquet',
    tables_partition_map 'table1:year={YEAR(event_date)},month={MONTH(event_date)} table2:year={YEAR(data_date)},month={MONTH(data_date)}'
);
```

This creates two foreign tables:
- `table1` with partition pruning based on `event_date` filters
- `table2` with partition pruning based on `data_date` filters

**Syntax details:**
- `tables_map`: Space-separated `tablename=filepattern` pairs
- `tables_partition_map`: Space-separated `tablename:mappings` pairs (note the colon `:` separator)
- Within each table's mappings, use commas to separate multiple partition keys
- Tables not listed in `tables_partition_map` use the global `partition_map` if provided, or get virtual columns (year/month as separate columns)

### Using Import Functions

For programmatic imports, use the `import_parquet` functions:

```sql
-- Import with automatic column detection
SELECT import_parquet(
    'my_table',           -- table name
    'public',             -- schema
    'parquet_srv',        -- server
    'list_files'::regproc, -- function returning file paths
    '{"dir": "/data"}',   -- function argument
    '{"sorted": "date"}'  -- table options
);

-- Import with explicit columns
SELECT import_parquet_explicit(
    'my_table',
    'public',
    'parquet_srv',
    ARRAY['id', 'name', 'value'],           -- column names
    ARRAY['int4', 'text', 'numeric']::regtype[], -- column types
    'list_files'::regproc,
    '{"dir": "/data"}',
    '{}'
);
```

**Example file listing function:**

```sql
CREATE FUNCTION list_files(args JSONB) RETURNS TEXT[] AS $$
BEGIN
    RETURN ARRAY(
        SELECT args->>'dir' || '/' || filename
        FROM pg_ls_dir(args->>'dir') AS files(filename)
        WHERE filename LIKE '%.parquet'
    );
END;
$$ LANGUAGE plpgsql;
```

## Security

By default, **only superusers** can create foreign tables, since the extension can read any file accessible to the PostgreSQL server process.

### Allowing Non-Superuser Access

A superuser can configure allowed directories:

```sql
ALTER SYSTEM SET parquet_fdw.allowed_directories = '/data/parquet, /data/analytics';
SELECT pg_reload_conf();
```

### Access Model

| User Type     | `allowed_directories` | Access |
|---------------|----------------------|--------|
| Superuser     | Any                  | All files accessible to PostgreSQL |
| Non-superuser | Empty (default)      | **Denied** |
| Non-superuser | Configured           | Only files within listed directories |

### Security Notes

- Paths are validated using `realpath()` to prevent symlink bypasses.
- Path traversal attempts (e.g., `../`) are blocked by canonicalizing paths.
- The `allowed_directories` setting requires superuser privileges to modify.
- Paths returned by `files_func` are also validated against allowed directories.

## Query Planning and Statistics

### Automatic Statistics from Parquet Metadata

The FDW automatically extracts row count statistics from Parquet file metadata during query planning. This provides accurate row estimates to PostgreSQL's query planner without requiring `ANALYZE`.

**How it works:**
1. During planning, the FDW reads metadata from each Parquet file
2. Row group pruning reduces I/O by skipping row groups that can't match query filters
3. The actual row count from matching row groups is used for cost estimation

This enables PostgreSQL to choose optimal join strategies (hash join vs nested loop) and parallel query plans based on actual data sizes.

**Cost Model:**
The FDW uses a cost model that accounts for:
- **I/O cost**: Based on estimated data size using `seq_page_cost`
- **Column projection**: Fewer columns = less data to read
- **CPU cost**: Tuple processing with decompression overhead
- **Startup cost**: Per-file metadata reading

This helps PostgreSQL make better decisions when choosing between different join strategies and parallel plans.

### Row Group Pruning

The FDW uses multiple techniques to skip row groups that cannot contain matching rows:

**Min/Max Statistics Pruning:**
- Parquet stores min/max values per column per row group
- Works for comparison operators: `=`, `<`, `<=`, `>`, `>=`, `!=`, `IN`
- Supported types: integers, floats, booleans, strings, dates, timestamps
- Note: `!=` (not equal) pruning only works when min == max, i.e., all values in the row group are identical

**IN Operator Pruning:**
- For `IN (value1, value2, ...)` filters, row groups are pruned if none of the values fall within the min/max range
- Example: `WHERE id IN (100, 200, 300)` will skip row groups where max < 100 or min > 300

**NULL Statistics Pruning:**
- Uses Parquet's `null_count` statistics per column per row group
- `IS NULL` skips row groups where `null_count = 0`
- `IS NOT NULL` skips row groups where all values are NULL (`null_count = num_values`)

**LIKE Prefix Pruning:**
- For patterns like `'prefix%'`, uses min/max statistics on string columns
- Skips row groups where `max < prefix` or `min > prefix`
- Only works for patterns with a fixed prefix before the first wildcard

**Bloom Filter Pruning:**
- For equality filters (`=`), bloom filters provide additional pruning when min/max statistics are ineffective
- Particularly useful for high-cardinality columns where each row group contains diverse values
- Parquet files must be written with bloom filters enabled for the relevant columns

**Example:** For a query like `WHERE event_name = 'click'`, min/max statistics may not help if each row group contains events from 'a' to 'z'. However, if the Parquet file has bloom filters on `event_name`, the FDW can quickly exclude row groups that definitely don't contain 'click'.

**Dictionary Filtering:**
- For equality filters (`=`), dictionary-encoded columns can be pruned by checking if the filter value exists in the column's dictionary
- Useful when bloom filters are not available but the column uses dictionary encoding
- Supported types: INT32, INT64, STRING
- If a value is not in the dictionary, the entire row group can be skipped

**Example:** A column with status codes ('pending', 'approved', 'rejected') stored with dictionary encoding can be efficiently filtered without bloom filters. A query `WHERE status = 'unknown'` will skip row groups where 'unknown' is not in the dictionary.

To enable bloom filters when writing Parquet files (PyArrow example):
```python
import pyarrow.parquet as pq

pq.write_table(
    table,
    'events.parquet',
    write_statistics=True,
    write_batch_size=10000,
    # Enable bloom filters for specific columns
    column_encoding={'event_name': 'PLAIN'},
    bloom_filter_columns=['event_name', 'user_id']
)
```

Enable debug logging to see row group pruning in action:
```sql
SET client_min_messages = DEBUG1;
SELECT * FROM events WHERE event_name = 'click';
-- DEBUG: parquet_fdw: skip rowgroup 1 in events.parquet (stats)
-- DEBUG: parquet_fdw: skip rowgroup 2 in events.parquet (dict)
-- DEBUG: parquet_fdw: skip rowgroup 3 in events.parquet (bloom)
```

The pruning methods shown in parentheses indicate how the row group was pruned:
- `stats`: Min/max statistics from row group metadata
- `bloom`: Bloom filter check
- `dict`: Dictionary encoding check

### When to Run ANALYZE

While automatic statistics provide good row estimates, running `ANALYZE` can improve query plans by providing:
- Column value histograms for selectivity estimation
- Most common values for equality filters
- Correlation statistics for index scans

```sql
-- Analyze a foreign table for additional statistics
ANALYZE my_parquet_table;
```

For large tables with complex queries, running `ANALYZE` periodically can improve query performance.

### Aggregate Pushdown Detection

The FDW detects when simple aggregate queries can potentially be answered from Parquet metadata alone, and logs this information at DEBUG1 level:

**COUNT(*):**
When a query is `SELECT COUNT(*) FROM table` without filters, the FDW detects that the total row count could be computed from file metadata without reading any data.

**MIN/MAX:**
When a query computes MIN or MAX on a single column, the FDW detects that these values could be computed from row group statistics.

```sql
SET client_min_messages = DEBUG1;

SELECT COUNT(*) FROM events;
-- DEBUG: parquet_fdw: aggregate pushdown possible: COUNT(*)

SELECT MIN(id), MAX(id) FROM events;
-- DEBUG: parquet_fdw: aggregate pushdown possible: MIN on column 1
-- DEBUG: parquet_fdw: aggregate pushdown possible: MAX on column 1
```

**Note:** Currently this is detection only - the FDW logs when pushdown is possible but still executes the full scan. Full pushdown execution (returning results directly from metadata) is a potential future enhancement.

## Configuration

### GUC Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `parquet_fdw.use_threads` | true | Global toggle for Arrow's parallel decoding |
| `parquet_fdw.enable_multifile` | true | Enable the Multifile reader |
| `parquet_fdw.enable_multifile_merge` | true | Enable the Multifile Merge reader |
| `parquet_fdw.allowed_directories` | (empty) | Comma-separated list of allowed directories for non-superusers |

**Example:**

```sql
SET parquet_fdw.use_threads = false;
SET parquet_fdw.enable_multifile_merge = false;
```

## Packaging

### Debian / Ubuntu

The Makefile includes a target for creating Debian packages:

```sh
# Build and install the extension
sudo make install PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config

# Create the Debian package
make debian PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config
```

This creates a versioned package in the `Debian/` directory with the naming format:
```
postgresql-<PGVER>-parquet-fdw_<VERSION>.<TIMESTAMP>_<ARCH>.deb
```

For example: `postgresql-17-parquet-fdw_0.2.20260129163000_amd64.deb`

The package version includes a timestamp (YYYYMMDDHHMMSS) to uniquely identify each build. Timestamps are monotonically increasing, making it easy to sort and track package revisions.

**Build for different PostgreSQL versions:**

```sh
# PostgreSQL 16
make debian PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config

# PostgreSQL 17
make debian PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config

# PostgreSQL 18
make debian PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config
```

**Important:** Always use the same `PG_CONFIG` for both `make install` and `make debian`. The build verifies that installed files match the target PostgreSQL version to prevent mixing binaries.

**Install the package:**

```sh
sudo dpkg -i Debian/postgresql-17-parquet-fdw_0.2+g1a2b3c4_amd64.deb
```

The package declares proper dependencies on `postgresql-<version>`, `libarrow`, and `libparquet` to ensure compatible library versions are installed.

**Clean build artifacts:**

```sh
make debian-clean
```

After installation, restart the PostgreSQL server.

## Additional Resources

- [Synthetic documentation for developers (AI-generated)](https://deepwiki.com/adjust/parquet_fdw)
