#include "common.hpp"

#include <cctype>
#include <cfloat>
#include <climits>
#include <cstring>
#include <regex>

#include "arrow/util/decimal.h"

extern "C"
{
#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/uuid.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/memdebug.h"
#include "utils/timestamp.h"
#include "utils/palloc.h"
#include "utils/numeric.h"
}

#ifndef PG_VERSION_NUM
#error "PG_VERSION_NUM is not defined"
#endif

#if PG_VERSION_NUM < 130000
#define MAXINT8LEN 25
#endif

/* --- PG version guards for MemoryContext alloc APIs (PG14+ uses Extended with flags) --- */
#ifndef PARQUET_FDW_MCXT_GUARD
#define PARQUET_FDW_MCXT_GUARD

#if PG_VERSION_NUM >= 140000
  #define PF_MCTX_ALLOC(ctx, sz)        MemoryContextAllocExtended((ctx), (sz), 0)
  #define PF_MCTX_REALLOC(ctx, p, sz)   MemoryContextReallocExtended((ctx), (p), (sz), 0)
#else
  #define PF_MCTX_ALLOC(ctx, sz)        MemoryContextAlloc((ctx), (sz))
  /* repalloc does not require context in pre-14 */
  #define PF_MCTX_REALLOC(ctx, p, sz)   repalloc((p), (sz))
#endif

#endif /* PARQUET_FDW_MCXT_GUARD */

/*
 * exc_palloc
 *      C++ specific memory allocator that utilizes postgres allocation sets.
 */
void *
exc_palloc(std::size_t size)
{
	/* duplicates MemoryContextAllocZero to avoid increased overhead */
	void	   *ret;
	MemoryContext context = CurrentMemoryContext;

	Assert(MemoryContextIsValid(context));

	if (!AllocSizeIsValid(size))
		throw std::bad_alloc();

	context->isReset = false;

	ret = PF_MCTX_ALLOC(context, size);
	if (unlikely(ret == nullptr))
		throw std::bad_alloc();

	VALGRIND_MEMPOOL_ALLOC(context, ret, size);

	return ret;
}


/*
 * Check if an Arrow user-extension type is of type UUID.
 *
 * CAUTION!
 * ========
 * UUID are not presently coded according to https://arrow.apache.org/docs/format/CanonicalExtensions.html#uuid.
 * When it is the case, you can call this function after enabling user-extension
 * into the reader.
 */
bool
is_extension_uuid(const arrow::DataType *arrow_type)
{
    auto ext_type = dynamic_cast<arrow::ExtensionType *>(const_cast<arrow::DataType*>(arrow_type));

    if (ext_type != nullptr && ext_type->extension_name() == "arrow.uuid")
    {
        auto storage = ext_type->storage_type();

        return storage->id() == arrow::Type::FIXED_SIZE_BINARY &&
            static_cast<arrow::FixedSizeBinaryType*>(storage.get())->byte_width() == UUID_LEN;
    }
    return false;
}

/*
 * Check if the UUID is coded as a fixed-size binary.
 *
 * CAUTION!
 * ========
 * This function is presently a work around as UUID should be coded
 * as user-extension types, according to https://arrow.apache.org/docs/format/CanonicalExtensions.html#uuid.
 * But this is not presently the case and we assume that 16-bytes binaries
 * are UUID.
 * See is_extension_uuid() to correctly identify UUID.
 */
bool
is_fixed_size_uuid(const arrow::DataType *arrow_type)
{
    return (arrow_type != nullptr && arrow_type->name() == "fixed_size_binary" && arrow_type->byte_width() == UUID_LEN);
}


Oid
to_postgres_type(const arrow::DataType *arrow_type)
{
    switch (arrow_type->id())
    {
        case arrow::Type::BOOL:
            return BOOLOID;
        case arrow::Type::INT8:
        case arrow::Type::INT16:
            return INT2OID;
        case arrow::Type::INT32:
            return INT4OID;
        case arrow::Type::INT64:
            return INT8OID;
        case arrow::Type::FLOAT:
            return FLOAT4OID;
        case arrow::Type::DOUBLE:
            return FLOAT8OID;
        case arrow::Type::STRING:
            return TEXTOID;
        case arrow::Type::BINARY:
            return BYTEAOID;
        case arrow::Type::TIMESTAMP:
            return TIMESTAMPOID;
        case arrow::Type::TIME64:
            return TIMEOID;
        case arrow::Type::DATE32:
            return DATEOID;
        case arrow::Type::DECIMAL128:
        case arrow::Type::DECIMAL256:
            return NUMERICOID;
        // UUID should be user-extension types, but that's not the case presently...
        // If the size is 16 bytes, we consider it is a UUID.
        case arrow::Type::FIXED_SIZE_BINARY:
            if (is_fixed_size_uuid(arrow_type))
                return UUIDOID;
            return BYTEAOID;
        case arrow::Type::EXTENSION:
            if (is_extension_uuid(arrow_type))
                return UUIDOID;
            return InvalidOid;
        default:
            return InvalidOid;
    }
}

/*
 * bytes_to_postgres_type
 *      Convert min/max values from column statistics stored in parquet file as
 *      plain bytes to postgres Datum.
 */
Datum
bytes_to_postgres_type(const char *bytes, Size len, const arrow::DataType *arrow_type)
{
    switch(arrow_type->id())
    {
        case arrow::Type::BOOL:
            return BoolGetDatum(*(bool *) bytes);
        case arrow::Type::INT8:
            return Int16GetDatum(*(int8 *) bytes);
        case arrow::Type::INT16:
            return Int16GetDatum(*(int16 *) bytes);
        case arrow::Type::INT32:
            return Int32GetDatum(*(int32 *) bytes);
        case arrow::Type::INT64:
            return Int64GetDatum(*(int64 *) bytes);
        case arrow::Type::FLOAT:
            return Float4GetDatum(*(float *) bytes);
        case arrow::Type::DOUBLE:
            return Float8GetDatum(*(double *) bytes);
        case arrow::Type::STRING:
            return CStringGetTextDatum(bytes);
        case arrow::Type::BINARY:
            return PointerGetDatum(cstring_to_text_with_len(bytes, len));
        case arrow::Type::TIMESTAMP:
            {
                TimestampTz ts;
                auto tstype = (arrow::TimestampType *) arrow_type;

                to_postgres_timestamp(tstype, *(int64 *) bytes, ts);
                return TimestampGetDatum(ts);
            }
            break;
        case arrow::Type::DATE32:
            return DateADTGetDatum(*(int32 *) bytes +
                                   (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
        case arrow::Type::TIME64:
            {
                int64 time_val = *(int64 *) bytes;
                auto t64type = static_cast<const arrow::Time64Type *>(arrow_type);

                /* PostgreSQL TIME is stored in microseconds */
                if (t64type->unit() == arrow::TimeUnit::NANO)
                    time_val = time_val / 1000;  /* Convert nanoseconds to microseconds */
                /* If MICRO, use value as-is */

                return TimeADTGetDatum(time_val);
            }
        case arrow::Type::FIXED_SIZE_BINARY:
            if (is_fixed_size_uuid(arrow_type))
            {
                pg_uuid_t *uuid = (pg_uuid_t *) palloc(sizeof(pg_uuid_t));
                memcpy(uuid->data, bytes, UUID_LEN);
                return UUIDPGetDatum(uuid);
            }
            return PointerGetDatum(cstring_to_text_with_len(bytes, len));
        default:
            return PointerGetDatum(nullptr);
    }
}

/*
 * XXX Currently only supports ascii strings
 */
char *
tolowercase(const char *input, char *output)
{
    int i = 0;

    Assert(strlen(input) < NAMEDATALEN - 1);

    do
    {
        output[i] = tolower(input[i]);
    }
    while (input[i++]);

    return output;
}

arrow::DataType *
get_arrow_list_elem_type(arrow::DataType *type)
{
    auto children = type->fields();

    Assert(children.size() == 1);
    return children[0]->type().get();
}

void datum_to_jsonb(Datum value, Oid typoid, bool isnull, FmgrInfo *outfunc,
                    JsonbParseState *parseState, bool iskey)
{
    JsonbValue  jb;

	if (isnull)
	{
		Assert(!iskey);
		jb.type = jbvNull;
        pushJsonbValue(&parseState, WJB_VALUE, &jb);
        return;
	}
    switch (typoid)
    {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        {
            /* If key is integer, we must convert it to text, not numeric */
            if (iskey) {
                char    *strval;

                strval = DatumGetCString(FunctionCall1(outfunc, value));

                jb.type = jbvString;
                jb.val.string.len = strlen(strval);
                jb.val.string.val = strval;
            }
            else {
                Datum numeric = (Datum) 0;

                switch (typoid)
                {
                    case INT2OID:
                    case INT4OID:
                        numeric = DirectFunctionCall1(int4_numeric, value);
                        break;
                    case INT8OID:
                        numeric = DirectFunctionCall1(int8_numeric, value);
                        break;
                    case FLOAT4OID:
                        numeric = DirectFunctionCall1(float4_numeric, value);
                        break;
                    case FLOAT8OID:
                        numeric = DirectFunctionCall1(float8_numeric, value);
                        break;
                    default:
                        Assert(false && "should never happen");
                }

                jb.type = jbvNumeric;
                jb.val.numeric = DatumGetNumeric(numeric);
            }
            break;
        }
        case TEXTOID:
        {
            char *str = TextDatumGetCString(value);

            jb.type = jbvString;
            jb.val.string.len = strlen(str);
            jb.val.string.val = str;
            break;
        }
        default:
        {
            char    *strval;

            strval = DatumGetCString(FunctionCall1(outfunc, value));

            jb.type = jbvString;
            jb.val.string.len = strlen(strval);
            jb.val.string.val = strval;
        }
    }

    pushJsonbValue(&parseState, iskey ? WJB_KEY : WJB_VALUE, &jb);
}

/*
 * string_to_int
 *      Convert string to integer.
 *
 *      This is modified copy of pg_atoi() function which was removed from Postgres 15.
 */
int32
string_to_int32(const char *s)
{
	long		l;
	char	   *badp;

	/*
	 * Some versions of strtol treat the empty string as an error, but some
	 * seem not to.  Make an explicit test to be sure we catch it.
	 */
	if (s == nullptr)
		elog(ERROR, "NULL pointer");
	if (*s == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"integer", s)));

	errno = 0;
	l = strtol(s, &badp, 10);

	/* We made no progress parsing the string, so bail out */
	if (s == badp)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"integer", s)));

	if (errno == ERANGE
#if defined(HAVE_LONG_INT_64)
	/* won't get ERANGE on these with 64-bit longs... */
		|| l < INT_MIN || l > INT_MAX
#endif
		)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value \"%s\" is out of range for type %s", s,
						"integer")));

	/*
	 * Skip any trailing whitespace; if anything but whitespace remains before
	 * the terminating character, bail out
	 */
	while (*badp && *badp != '\0' && isspace((unsigned char) *badp))
		badp++;

	if (*badp && *badp != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"integer", s)));

	return (int32) l;
}

/*
 * string_to_int16
 *      Convert string to int16 with proper error handling.
 */
int16
string_to_int16(const char *s)
{
    int32 result = string_to_int32(s);

    if (result < SHRT_MIN || result > SHRT_MAX)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("value \"%s\" is out of range for type %s", s, "smallint")));

    return (int16) result;
}

/*
 * string_to_int64
 *      Convert string to int64 with proper error handling.
 */
int64
string_to_int64(const char *s)
{
    long long   l;
    char       *badp;

    if (s == nullptr)
        elog(ERROR, "NULL pointer");
    if (*s == 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"", "bigint", s)));

    errno = 0;
    l = strtoll(s, &badp, 10);

    if (s == badp)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"", "bigint", s)));

    if (errno == ERANGE)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("value \"%s\" is out of range for type %s", s, "bigint")));

    while (*badp && *badp != '\0' && isspace((unsigned char) *badp))
        badp++;

    if (*badp && *badp != '\0')
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"", "bigint", s)));

    return (int64) l;
}

/*
 * string_to_float4
 *      Convert string to float4 with proper error handling.
 */
float4
string_to_float4(const char *s)
{
    double      d;
    char       *badp;

    if (s == nullptr)
        elog(ERROR, "NULL pointer");
    if (*s == 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"", "real", s)));

    errno = 0;
    d = strtod(s, &badp);

    if (s == badp)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"", "real", s)));

    if (errno == ERANGE)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("value \"%s\" is out of range for type %s", s, "real")));

    /* Check if value fits in float4 range */
    if (d != 0 && (d < -FLT_MAX || d > FLT_MAX || (d > -FLT_MIN && d < FLT_MIN && d != 0)))
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("value \"%s\" is out of range for type %s", s, "real")));

    while (*badp && *badp != '\0' && isspace((unsigned char) *badp))
        badp++;

    if (*badp && *badp != '\0')
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"", "real", s)));

    return (float4) d;
}

/*
 * string_to_float8
 *      Convert string to float8 with proper error handling.
 */
float8
string_to_float8(const char *s)
{
    double      d;
    char       *badp;

    if (s == nullptr)
        elog(ERROR, "NULL pointer");
    if (*s == 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"", "double precision", s)));

    errno = 0;
    d = strtod(s, &badp);

    if (s == badp)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"", "double precision", s)));

    if (errno == ERANGE)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("value \"%s\" is out of range for type %s", s, "double precision")));

    while (*badp && *badp != '\0' && isspace((unsigned char) *badp))
        badp++;

    if (*badp && *badp != '\0')
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"", "double precision", s)));

    return (float8) d;
}

/*
 * Hive partition helper function implementations
 */

/* Hive NULL partition marker */
static const char *HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";

/*
 * is_hive_null_partition
 *      Check if a value represents the Hive NULL partition marker.
 */
bool
is_hive_null_partition(const char *value)
{
    return value != nullptr && strcmp(value, HIVE_DEFAULT_PARTITION) == 0;
}

/*
 * url_decode
 *      Decode URL-encoded string (handles %XX encoding).
 *      Returns decoded string.
 */
std::string
url_decode(const std::string &encoded)
{
    std::string decoded;
    decoded.reserve(encoded.length());

    for (size_t i = 0; i < encoded.length(); ++i)
    {
        if (encoded[i] == '%' && i + 2 < encoded.length())
        {
            char hex[3] = {encoded[i + 1], encoded[i + 2], '\0'};
            char *endptr;
            long val = strtol(hex, &endptr, 16);
            if (*endptr == '\0')
            {
                decoded += static_cast<char>(val);
                i += 2;
                continue;
            }
        }
        decoded += encoded[i];
    }
    return decoded;
}

/*
 * extract_hive_partitions
 *      Extract partition key=value pairs from a file path.
 *      Hive-style paths look like: /base/path/key1=value1/key2=value2/filename.parquet
 *
 *      Returns a vector of HivePartitionValue structs with the extracted partitions.
 */
std::vector<HivePartitionValue>
extract_hive_partitions(const char *path)
{
    std::vector<HivePartitionValue> partitions;

    if (path == nullptr)
        return partitions;

    std::string path_str(path);
    size_t pos = 0;
    size_t end;

    /* Split path by '/' and look for key=value patterns */
    while ((end = path_str.find('/', pos)) != std::string::npos || pos < path_str.length())
    {
        size_t segment_end = (end != std::string::npos) ? end : path_str.length();
        std::string segment = path_str.substr(pos, segment_end - pos);

        /* Look for '=' in the segment */
        size_t eq_pos = segment.find('=');
        if (eq_pos != std::string::npos && eq_pos > 0 && eq_pos < segment.length() - 1)
        {
            std::string key = segment.substr(0, eq_pos);
            std::string value = segment.substr(eq_pos + 1);

            /* URL-decode the value */
            value = url_decode(value);

            HivePartitionValue pv(key, value);
            pv.isnull = is_hive_null_partition(value.c_str());
            pv.pg_type = infer_partition_type(value.c_str());

            partitions.push_back(std::move(pv));
        }

        if (end == std::string::npos)
            break;
        pos = end + 1;
    }

    return partitions;
}

/*
 * infer_partition_type
 *      Infer PostgreSQL type from a partition value string.
 *      Tries to determine if value is: integer, date, or text.
 */
Oid
infer_partition_type(const char *value)
{
    if (value == nullptr || *value == '\0')
        return TEXTOID;

    /* Check for Hive NULL partition */
    if (is_hive_null_partition(value))
        return TEXTOID;  /* Type will be determined from schema */

    /* Check if it's an integer */
    char *endptr;
    errno = 0;
    long lval = strtol(value, &endptr, 10);
    (void)lval;  /* suppress unused warning */
    if (*endptr == '\0' && errno == 0)
        return INT4OID;

    /* Check if it looks like a date (YYYY-MM-DD) */
    if (strlen(value) == 10 && value[4] == '-' && value[7] == '-')
    {
        bool valid = true;
        for (int i = 0; i < 10 && valid; i++)
        {
            if (i == 4 || i == 7)
                continue;
            if (!isdigit((unsigned char)value[i]))
                valid = false;
        }
        if (valid)
            return DATEOID;
    }

    /* Default to text */
    return TEXTOID;
}

/*
 * Julian date to year/month/day conversion.
 * This is a portable implementation that doesn't require datetime.h
 * (which conflicts with Arrow's TimeUnit::SECOND).
 *
 * Algorithm from Fliegel & Van Flandern, Communications of the ACM, 1968.
 */
static void
julian_to_ymd(int julian, int *year, int *month, int *day)
{
    int l, n, i, j;

    l = julian + 68569;
    n = 4 * l / 146097;
    l = l - (146097 * n + 3) / 4;
    i = 4000 * (l + 1) / 1461001;
    l = l - 1461 * i / 4 + 31;
    j = 80 * l / 2447;
    *day = l - 2447 * j / 80;
    l = j / 11;
    *month = j + 2 - 12 * l;
    *year = 100 * (n - 49) + i + l;
}

/*
 * Year/month/day to Julian date conversion.
 */
static int
ymd_to_julian(int year, int month, int day)
{
    int a = (14 - month) / 12;
    int y = year + 4800 - a;
    int m = month + 12 * a - 3;

    return day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
}

/*
 * string_to_datum
 *      Convert a string value to a Datum of the specified PostgreSQL type.
 *      Sets *isnull to true if the value represents a Hive NULL partition.
 */
Datum
string_to_datum(const char *value, Oid pg_type, bool *isnull)
{
    *isnull = false;

    if (value == nullptr || is_hive_null_partition(value))
    {
        *isnull = true;
        return (Datum) 0;
    }

    switch (pg_type)
    {
        case INT2OID:
            return Int16GetDatum(string_to_int16(value));
        case INT4OID:
            return Int32GetDatum(string_to_int32(value));
        case INT8OID:
            return Int64GetDatum(string_to_int64(value));
        case FLOAT4OID:
            return Float4GetDatum(string_to_float4(value));
        case FLOAT8OID:
            return Float8GetDatum(string_to_float8(value));
        case TEXTOID:
        case VARCHAROID:
            return CStringGetTextDatum(value);
        case DATEOID:
        {
            /* Parse YYYY-MM-DD format */
            int year, month, day;
            if (sscanf(value, "%d-%d-%d", &year, &month, &day) == 3)
            {
                DateADT date = ymd_to_julian(year, month, day) - POSTGRES_EPOCH_JDATE;
                return DateADTGetDatum(date);
            }
            /* Fall through to input function if parsing fails */
        }
        /* fall through */
        default:
        {
            /* Use PostgreSQL's input function for other types */
            Oid         typinput;
            Oid         typioparam;

            getTypeInputInfo(pg_type, &typinput, &typioparam);
            return OidInputFunctionCall(typinput, pstrdup(value), typioparam, -1);
        }
    }
}

/*
 * parse_partition_map
 *      Parse a partition_map option string into a vector of HivePartitionMapping.
 *
 *      Format: "key1={FUNC(column1)}, key2={FUNC(column2)}"
 *      Where FUNC is one of: YEAR, MONTH, DAY, or omitted for identity mapping.
 *
 *      Examples:
 *        "year={YEAR(event_date)}, month={MONTH(event_date)}"
 *        "region={region}"   (identity mapping)
 */
std::vector<HivePartitionMapping>
parse_partition_map(const char *map_str)
{
    std::vector<HivePartitionMapping> mappings;

    if (map_str == nullptr || *map_str == '\0')
        return mappings;

    std::string input(map_str);

    /* Regex to match: key={FUNC(col)} or key={col} */
    std::regex mapping_re(R"(\s*(\w+)\s*=\s*\{\s*(?:(\w+)\s*\(\s*(\w+)\s*\)|(\w+))\s*\}\s*,?)");

    auto begin = std::sregex_iterator(input.begin(), input.end(), mapping_re);
    auto end = std::sregex_iterator();

    for (auto it = begin; it != end; ++it)
    {
        std::smatch match = *it;

        std::string partition_key = match[1].str();
        std::string func_name = match[2].str();
        std::string column_name = match[3].str();
        std::string identity_col = match[4].str();

        HivePartitionMapping mapping;
        mapping.partition_key = partition_key;

        if (!identity_col.empty())
        {
            /* Identity mapping: key={column} */
            mapping.column_name = identity_col;
            mapping.func = PEF_IDENTITY;
        }
        else
        {
            /* Function mapping: key={FUNC(column)} */
            mapping.column_name = column_name;

            /* Convert function name to enum */
            if (strcasecmp(func_name.c_str(), "YEAR") == 0)
                mapping.func = PEF_YEAR;
            else if (strcasecmp(func_name.c_str(), "MONTH") == 0)
                mapping.func = PEF_MONTH;
            else if (strcasecmp(func_name.c_str(), "DAY") == 0)
                mapping.func = PEF_DAY;
            else
                throw Error("parquet_fdw: unknown partition extraction function '%s'",
                            func_name.c_str());
        }

        mappings.push_back(std::move(mapping));
    }

    return mappings;
}

/*
 * Helper function to extract year/month/day from a PostgreSQL DateADT.
 */
void
date_to_ymd(DateADT date, int *year, int *month, int *day)
{
    julian_to_ymd(date + POSTGRES_EPOCH_JDATE, year, month, day);
}

/*
 * Helper function to extract year/month/day from a PostgreSQL Timestamp.
 * Converts timestamp to date first, then extracts components.
 */
void
timestamp_to_ymd(Timestamp ts, int *year, int *month, int *day)
{
    /*
     * Convert timestamp to days since PostgreSQL epoch.
     * Timestamp is microseconds since 2000-01-01 00:00:00.
     */
    int64 date_val;

    /* Convert microseconds to days (86400 seconds * 1000000 microseconds) */
    if (ts >= 0)
        date_val = ts / INT64CONST(86400000000);
    else
        date_val = (ts - INT64CONST(86400000000) + 1) / INT64CONST(86400000000);

    julian_to_ymd(static_cast<int>(date_val) + POSTGRES_EPOCH_JDATE, year, month, day);
}

/*
 * Helper function to convert year/month/day to a PostgreSQL DateADT.
 */
DateADT
date_to_adt(int year, int month, int day)
{
    return ymd_to_julian(year, month, day) - POSTGRES_EPOCH_JDATE;
}

/*
 * extract_partition_value
 *      Extract a partition value from a column datum using the specified function.
 *      Returns the extracted value as a Datum and sets *result_type to its type.
 */
Datum
extract_partition_value(Datum column_value, Oid column_type,
                        PartitionExtractFunc func, Oid *result_type)
{
    int year, month, day;

    switch (func)
    {
        case PEF_IDENTITY:
            *result_type = column_type;
            return column_value;

        case PEF_YEAR:
        {
            *result_type = INT4OID;

            if (column_type == DATEOID)
            {
                date_to_ymd(DatumGetDateADT(column_value), &year, &month, &day);
                return Int32GetDatum(year);
            }
            else if (column_type == TIMESTAMPOID || column_type == TIMESTAMPTZOID)
            {
                timestamp_to_ymd(DatumGetTimestamp(column_value), &year, &month, &day);
                return Int32GetDatum(year);
            }
            throw Error("parquet_fdw: YEAR() requires DATE or TIMESTAMP column");
        }

        case PEF_MONTH:
        {
            *result_type = INT4OID;

            if (column_type == DATEOID)
            {
                date_to_ymd(DatumGetDateADT(column_value), &year, &month, &day);
                return Int32GetDatum(month);
            }
            else if (column_type == TIMESTAMPOID || column_type == TIMESTAMPTZOID)
            {
                timestamp_to_ymd(DatumGetTimestamp(column_value), &year, &month, &day);
                return Int32GetDatum(month);
            }
            throw Error("parquet_fdw: MONTH() requires DATE or TIMESTAMP column");
        }

        case PEF_DAY:
        {
            *result_type = INT4OID;

            if (column_type == DATEOID)
            {
                date_to_ymd(DatumGetDateADT(column_value), &year, &month, &day);
                return Int32GetDatum(day);
            }
            else if (column_type == TIMESTAMPOID || column_type == TIMESTAMPTZOID)
            {
                timestamp_to_ymd(DatumGetTimestamp(column_value), &year, &month, &day);
                return Int32GetDatum(day);
            }
            throw Error("parquet_fdw: DAY() requires DATE or TIMESTAMP column");
        }

        default:
            throw Error("parquet_fdw: unknown partition extraction function");
    }
}
