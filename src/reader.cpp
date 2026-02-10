#include <list>

#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/file_reader.h"
#include "parquet/statistics.h"

#include "common.hpp"
#include "reader.hpp"

extern "C"
{
#include "postgres.h"
#include "access/sysattr.h"
#include "parser/parse_coerce.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/uuid.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/numeric.h"

#if PG_VERSION_NUM < 110000
#include "catalog/pg_type.h"
#else
#include "catalog/pg_type_d.h"
#endif
}

#define SEGMENT_SIZE (1024 * 1024)


bool parquet_fdw_use_threads = true;


class FastAllocator
{
private:
    /*
     * Special memory segment to speed up bytea/Text allocations.
     */
    MemoryContext       segments_cxt;
    char               *segment_start_ptr;
    char               *segment_cur_ptr;
    char               *segment_last_ptr;
    std::list<char *>   garbage_segments;

public:
    FastAllocator(MemoryContext cxt)
        : segments_cxt(cxt), segment_start_ptr(nullptr), segment_cur_ptr(nullptr),
          segment_last_ptr(nullptr), garbage_segments()
    {}

    ~FastAllocator()
    {
        this->recycle();
    }

    /*
     * fast_alloc
     *      Preallocate a big memory segment and distribute blocks from it. When
     *      segment is exhausted it is added to garbage_segments list and freed
     *      on the next executor's iteration. If requested size is bigger that
     *      SEGMENT_SIZE then just palloc is used.
     */
    inline void *fast_alloc(long size)
    {
        void   *ret;

        Assert(size >= 0);

        /* If allocation is bigger than segment then just palloc */
        if (size > SEGMENT_SIZE)
        {
            PgMemoryContextGuard guard(this->segments_cxt);
            void *block = exc_palloc(size);
            this->garbage_segments.push_back((char *) block);

            return block;
        }

        size = MAXALIGN(size);

        /* If there is not enough space in current segment create a new one */
        if (this->segment_last_ptr - this->segment_cur_ptr < size)
        {
            /*
             * Recycle the last segment at the next iteration (if there
             * was one)
             */
            if (this->segment_start_ptr)
                this->garbage_segments.push_back(this->segment_start_ptr);

            {
                PgMemoryContextGuard guard(this->segments_cxt);
                this->segment_start_ptr = (char *) exc_palloc(SEGMENT_SIZE);
            }
            this->segment_cur_ptr = this->segment_start_ptr;
            this->segment_last_ptr =
                this->segment_start_ptr + SEGMENT_SIZE - 1;
        }

        ret = (void *) this->segment_cur_ptr;
        this->segment_cur_ptr += size;

        return ret;
    }

    void recycle(void)
    {
        /* recycle old segments if any */
        if (!this->garbage_segments.empty())
        {
            bool    error = false;

            PG_TRY();
            {
                for (auto it : this->garbage_segments)
                    pfree(it);
            }
            PG_CATCH();
            {
                error = true;
            }
            PG_END_TRY();
            if (error)
                throw std::runtime_error("garbage segments recycle failed");

            this->garbage_segments.clear();
            elog(DEBUG2, "parquet_fdw: garbage segments recycled");
        }
    }

    MemoryContext context()
    {
        return segments_cxt;
    }
};


ParquetReader::ParquetReader(MemoryContext cxt)
    : allocator(new FastAllocator(cxt))
{}

int32_t ParquetReader::id()
{
    return reader_id;
}

/*
 * create_column_mapping
 *      Create mapping between tuple descriptor and parquet columns.
 */
void ParquetReader::create_column_mapping(TupleDesc tupleDesc, const std::set<int> &attrs_used)
{
    parquet::ArrowReaderProperties  props;
    std::shared_ptr<arrow::Schema>  a_schema;
    parquet::arrow::SchemaManifest  manifest;
    auto    p_schema = this->reader->parquet_reader()->metadata()->schema();

    if (!parquet::arrow::SchemaManifest::Make(p_schema, nullptr, props, &manifest).ok())
        throw Error("error creating arrow schema ('%s')", this->filename.c_str());

    this->map.resize(tupleDesc->natts);
    for (int i = 0; i < tupleDesc->natts; i++)
    {
        AttrNumber  attnum = i + 1 - FirstLowInvalidHeapAttributeNumber;
        char        pg_colname[255];
        const char *attname = NameStr(TupleDescAttr(tupleDesc, i)->attname);

        this->map[i] = -1;

        /* Skip columns we don't intend to use in query */
        if (attrs_used.find(attnum) == attrs_used.end())
            continue;

        tolowercase(NameStr(TupleDescAttr(tupleDesc, i)->attname), pg_colname);

        for (auto &schema_field : manifest.schema_fields)
        {
            auto field_name = schema_field.field->name();
            auto arrow_type = schema_field.field->type();
            char arrow_colname[255];

            if (field_name.length() > NAMEDATALEN)
                throw Error("parquet column name '%s' is too long (max: %d, file: '%s')",
                            field_name.c_str(), NAMEDATALEN - 1, this->filename.c_str());
            tolowercase(schema_field.field->name().c_str(), arrow_colname);

            /*
             * Compare postgres attribute name to the column name in arrow
             * schema.
             */
            if (strcmp(pg_colname, arrow_colname) == 0)
            {
                TypeInfo        typinfo(arrow_type);
                bool            error(false);

                /* Found mapping! */

                this->column_names.push_back(std::move(arrow_colname));

                /* index of last element */
                this->map[i] = this->column_names.size() - 1;

                typinfo.pg.oid = TupleDescAttr(tupleDesc, i)->atttypid;
                typinfo.pg.typmod = TupleDescAttr(tupleDesc, i)->atttypmod;
                switch (arrow_type->id())
                {
                    case arrow::Type::LIST:
                    case arrow::Type::LARGE_LIST:
                    {
                        Assert(schema_field.children.size() == 1);

                        Oid     elem_type;
                        int16   elem_len;
                        bool    elem_byval;
                        char    elem_align;

                        PG_TRY();
                        {
                            elem_type = get_element_type(typinfo.pg.oid);
                            if (OidIsValid(elem_type)) {
                                get_typlenbyvalalign(elem_type, &elem_len,
                                                     &elem_byval, &elem_align);
                            }
                        }
                        PG_CATCH();
                        {
                            error = true;
                        }
                        PG_END_TRY();
                        if (error)
                            throw Error("failed to get type length (column '%s')",
                                        pg_colname);

                        if (!OidIsValid(elem_type))
                            throw Error("cannot convert parquet column of type "
                                        "LIST to scalar type of postgres column '%s'",
                                        pg_colname);

                        auto     &child = schema_field.children[0];
                        typinfo.children.emplace_back(child.field->type(),
                                                      elem_type);
                        TypeInfo &elem = typinfo.children[0];
                        elem.pg.len = elem_len;
                        elem.pg.byval = elem_byval;
                        elem.pg.align = elem_align;
                        initialize_cast(elem, attname);

                        this->indices.push_back(child.column_index);
                        break;
                    }
                    case arrow::Type::MAP:
                    {
                        /* 
                         * Map has the following structure:
                         *
                         * Type::MAP
                         * └─ Type::STRUCT
                         *    ├─  key type
                         *    └─  item type
                         */

                        Assert(schema_field.children.size() == 1);
                        auto &strct = schema_field.children[0];

                        Assert(strct.children.size() == 2);
                        auto &key = strct.children[0];
                        auto &item = strct.children[1];
                        Oid pg_key_type = to_postgres_type(key.field->type().get());
                        Oid pg_item_type = to_postgres_type(item.field->type().get());

                        typinfo.children.emplace_back(key.field->type(),
                                                      pg_key_type);
                        typinfo.children.emplace_back(item.field->type(),
                                                      pg_item_type);

                        PG_TRY();
                        {
                            typinfo.children[0].outfunc = find_outfunc(pg_key_type);
                            typinfo.children[1].outfunc = find_outfunc(pg_item_type);
                        }
                        PG_CATCH();
                        {
                            error = true;
                        }
                        PG_END_TRY();
                        if (error)
                            throw Error("failed to initialize output function for "
                                        "Map column '%s'", attname);

                        this->indices.push_back(key.column_index);
                        this->indices.push_back(item.column_index);

                        /* JSONB might need cast (e.g. to TEXT) */
                        initialize_cast(typinfo, attname);
                        break;
                    }
                    default:
                        initialize_cast(typinfo, attname);
                        typinfo.index = schema_field.column_index;
                        this->indices.push_back(schema_field.column_index);
                }
                this->types.push_back(std::move(typinfo));

                break;
            }
        }
    }
}

Datum ParquetReader::do_cast(Datum val, const TypeInfo &typinfo)
{
    MemoryContext   ccxt = CurrentMemoryContext;
    bool            error = false;
    char            errstr[ERROR_STR_LEN];

    /* du, du cast, du cast mich... */
    PG_TRY();
    {
        if (typinfo.castfunc != nullptr)
        {
            val = FunctionCall1(typinfo.castfunc, val);
        }
        else if (typinfo.outfunc && typinfo.infunc)
        {
            char *str;

            str = OutputFunctionCall(typinfo.outfunc, val);

            /* TODO: specify typioparam and typmod */
            val = InputFunctionCall(typinfo.infunc, str, 0, 0);
        }
    }
    PG_CATCH();
    {
        ErrorData *errdata;

        MemoryContextSwitchTo(ccxt);
        error = true;
        errdata = CopyErrorData();
        FlushErrorState();

        strncpy(errstr, errdata->message, ERROR_STR_LEN - 1);
        errstr[ERROR_STR_LEN - 1] = '\0';
        FreeErrorData(errdata);
    }
    PG_END_TRY();
    if (error)
        throw std::runtime_error(errstr);

    return val;
}

/*
 * read_primitive_type
 *      Returns primitive type value from arrow array
 */
Datum ParquetReader::read_primitive_type(arrow::Array *array,
                                         const TypeInfo &typinfo,
                                         int64_t i)
{
    Datum   res;

    /* Get datum depending on the column type */
    switch (typinfo.arrow.type_id)
    {
        case arrow::Type::BOOL:
        {
            auto *boolarray = static_cast<arrow::BooleanArray *>(array);

            res = BoolGetDatum(boolarray->Value(i));
            break;
        }
        case arrow::Type::INT8:
        {
            auto *intarray = static_cast<arrow::Int8Array *>(array);
            int value = intarray->Value(i);

            res = Int8GetDatum(value);
            break;
        }
        case arrow::Type::INT16:
        {
            auto *intarray = static_cast<arrow::Int16Array *>(array);
            int value = intarray->Value(i);

            res = Int16GetDatum(value);
            break;
        }
        case arrow::Type::INT32:
        {
            auto *intarray = static_cast<arrow::Int32Array *>(array);
            int value = intarray->Value(i);

            res = Int32GetDatum(value);
            break;
        }
        case arrow::Type::INT64:
        {
            auto *intarray = static_cast<arrow::Int64Array *>(array);
            int64 value = intarray->Value(i);

            res = Int64GetDatum(value);
            break;
        }
        case arrow::Type::UINT8:
        {
            auto *intarray = static_cast<arrow::UInt8Array *>(array);
            int16 value = (int16) intarray->Value(i);

            res = Int16GetDatum(value);
            break;
        }
        case arrow::Type::UINT16:
        {
            auto *intarray = static_cast<arrow::UInt16Array *>(array);
            int32 value = (int32) intarray->Value(i);

            res = Int32GetDatum(value);
            break;
        }
        case arrow::Type::UINT32:
        {
            auto *intarray = static_cast<arrow::UInt32Array *>(array);
            int64 value = (int64) intarray->Value(i);

            res = Int64GetDatum(value);
            break;
        }
        case arrow::Type::UINT64:
        {
            auto *intarray = static_cast<arrow::UInt64Array *>(array);
            uint64 value = intarray->Value(i);
            char buf[32];

            snprintf(buf, sizeof(buf), "%llu", (unsigned long long) value);
            char *cval = pstrdup(buf);
            res = DirectFunctionCall3(numeric_in,
                                      CStringGetDatum(cval),
                                      ObjectIdGetDatum(InvalidOid),
                                      Int32GetDatum(-1));
            break;
        }
        case arrow::Type::FLOAT:
        {
            auto *farray = static_cast<arrow::FloatArray *>(array);
            float value = farray->Value(i);

            res = Float4GetDatum(value);
            break;
        }
        case arrow::Type::DOUBLE:
        {
            auto *darray = static_cast<arrow::DoubleArray *>(array);
            double value = darray->Value(i);

            res = Float8GetDatum(value);
            break;
        }
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_STRING:
        case arrow::Type::LARGE_BINARY:
        {
            /*
             * The schema manifest may report STRING/BINARY while the actual
             * array data uses LARGE_STRING/LARGE_BINARY (64-bit offsets).
             * Check the actual array type to use the correct accessor.
             */
            const char *value;
            int64 vallen;

            if (array->type_id() == arrow::Type::LARGE_STRING ||
                array->type_id() == arrow::Type::LARGE_BINARY)
            {
                auto *binarray = static_cast<arrow::LargeBinaryArray *>(array);
                int64_t len64 = 0;
                value = reinterpret_cast<const char*>(binarray->GetValue(i, &len64));
                vallen = len64;
            }
            else
            {
                auto *binarray = static_cast<arrow::BinaryArray *>(array);
                int32_t len32 = 0;
                value = reinterpret_cast<const char*>(binarray->GetValue(i, &len32));
                vallen = len32;
            }

            /* Build bytea */
            int64 bytea_len = vallen + VARHDRSZ;
            bytea *b = (bytea *) this->allocator->fast_alloc(bytea_len);
            SET_VARSIZE(b, bytea_len);
            memcpy(VARDATA(b), value, vallen);

            res = PointerGetDatum(b);
            break;
        }
        case arrow::Type::TIMESTAMP:
        {
            /* TODO: deal with timezones */
            TimestampTz ts;
            auto *tsarray = static_cast<arrow::TimestampArray *>(array);
            auto *tstype = static_cast<arrow::TimestampType *>(array->type().get());

            to_postgres_timestamp(tstype, tsarray->Value(i), ts);
            res = TimestampGetDatum(ts);
            break;
        }
        case arrow::Type::DATE32:
        {
            auto *tsarray = static_cast<arrow::Date32Array *>(array);
            int32 d = tsarray->Value(i);

            /*
            * Postgres date starts with 2000-01-01 while unix date (which
            * Parquet is using) starts with 1970-01-01. So we need to do
            * simple calculations here.
            */
            res = DateADTGetDatum(d + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
            break;
        }
        case arrow::Type::TIME64:
        {
            auto *tarray = static_cast<arrow::Time64Array *>(array);
            auto *ttype = static_cast<arrow::Time64Type *>(array->type().get());
            int64 value = tarray->Value(i);

            /* PostgreSQL TIME is stored in microseconds */
            if (ttype->unit() == arrow::TimeUnit::NANO)
            {
                /* Round to nearest microsecond instead of truncating */
                value = (value + 500) / 1000;
            }
            /* If MICRO, use value as-is */

            /*
             * Validate TIME range: PostgreSQL TIME is 00:00:00 to 24:00:00
             * which is 0 to 24*60*60*1000000 = 86400000000 microseconds
             */
            const int64 MAX_TIME_USEC = INT64CONST(86400000000);
            if (value < 0 || value > MAX_TIME_USEC)
            {
                ereport(WARNING,
                        (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                         errmsg("parquet_fdw: TIME value out of range, clamping"),
                         errdetail("Value %lld microseconds is outside 00:00:00-24:00:00 range.", (long long) value)));
                if (value < 0)
                    value = 0;
                else
                    value = MAX_TIME_USEC;
            }

            res = TimeADTGetDatum(value);
            break;
        }
        case arrow::Type::DECIMAL128:
        {
            auto *decarray = static_cast<arrow::Decimal128Array *>(array);
            auto *dectype = static_cast<arrow::Decimal128Type *>(array->type().get());
            std::string val = decarray->FormatValue(i);

            /*
             * Check if Parquet precision exceeds PostgreSQL column precision.
             * Only warn once per column to avoid log flooding.
             *
             * NUMERIC typmod encodes precision as: ((typmod - VARHDRSZ) >> 16) & 0xFFFF
             * If typmod == -1, no precision was specified (variable precision).
             */
            if (!typinfo.decimal_precision_warned && typinfo.pg.typmod != -1)
            {
                int32 parquet_precision = dectype->precision();
                int32 pg_precision = ((typinfo.pg.typmod - VARHDRSZ) >> 16) & 0xFFFF;

                if (parquet_precision > pg_precision)
                {
                    ereport(WARNING,
                            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                             errmsg("parquet_fdw: DECIMAL precision loss possible"),
                             errdetail("Parquet column has precision %d but PostgreSQL column has precision %d.",
                                       parquet_precision, pg_precision)));
                    typinfo.decimal_precision_warned = true;
                }
            }

            /* Copy to palloc'd memory to ensure it survives */
            char *cval = pstrdup(val.c_str());
            res = DirectFunctionCall3(numeric_in,
                                      CStringGetDatum(cval),
                                      ObjectIdGetDatum(InvalidOid),
                                      Int32GetDatum(-1));
            break;
        }
        case arrow::Type::DECIMAL256:
        {
            auto *decarray = static_cast<arrow::Decimal256Array *>(array);
            auto *dectype = static_cast<arrow::Decimal256Type *>(array->type().get());
            std::string val = decarray->FormatValue(i);

            /*
             * Check if Parquet precision exceeds PostgreSQL column precision.
             * Only warn once per column to avoid log flooding.
             */
            if (!typinfo.decimal_precision_warned && typinfo.pg.typmod != -1)
            {
                int32 parquet_precision = dectype->precision();
                int32 pg_precision = ((typinfo.pg.typmod - VARHDRSZ) >> 16) & 0xFFFF;

                if (parquet_precision > pg_precision)
                {
                    ereport(WARNING,
                            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                             errmsg("parquet_fdw: DECIMAL precision loss possible"),
                             errdetail("Parquet column has precision %d but PostgreSQL column has precision %d.",
                                       parquet_precision, pg_precision)));
                    typinfo.decimal_precision_warned = true;
                }
            }

            /* Copy to palloc'd memory to ensure it survives */
            char *cval = pstrdup(val.c_str());
            res = DirectFunctionCall3(numeric_in,
                                      CStringGetDatum(cval),
                                      ObjectIdGetDatum(InvalidOid),
                                      Int32GetDatum(-1));
            break;
        }
        case arrow::Type::FIXED_SIZE_BINARY:
        {
            arrow::FixedSizeBinaryArray *binarray = dynamic_cast<arrow::FixedSizeBinaryArray*>(array);
            if (!binarray)
            {
                elog(ERROR, "parquet_fdw: Expected FixedSizeBinaryArray but got %s", array->type()->ToString().c_str());
            }

            if (typinfo.is_uuid)
            {
                const uint8_t *value = binarray->GetValue(i);

                elog(DEBUG3, "parquet_fdw: Reading UUID from FIXED_SIZE_BINARY: %02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                    value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
                    value[8], value[9], value[10], value[11], value[12], value[13], value[14], value[15]
                );
                pg_uuid_t *uuid_val = (pg_uuid_t*) this->allocator->fast_alloc(sizeof(pg_uuid_t));
                memcpy(uuid_val->data, value, UUID_LEN);
                res = UUIDPGetDatum(uuid_val);  // convert pointer to Datum
            }
            else
            {
                elog(DEBUG3, "parquet_fdw: Reading actual FIXED_SIZE_BINARY");
                const int32_t vallen = binarray->byte_width();
                const uint8_t *value = binarray->GetValue(i);

                /* Build bytea */
                int64 bytea_len = vallen + VARHDRSZ;
                bytea *b = (bytea *) this->allocator->fast_alloc(bytea_len);
                SET_VARSIZE(b, bytea_len);
                memcpy(VARDATA(b), value, vallen);

                res = PointerGetDatum(b);
            }
            break;
        }
        case arrow::Type::EXTENSION:
        {
            auto *binarray = static_cast<arrow::BinaryArray *>(array);

            int32_t vallen = 0;
            const char *value = reinterpret_cast<const char*>(binarray->GetValue(i, &vallen));

            if (typinfo.is_uuid)
            {
                elog(DEBUG3, "parquet_fdw: Reading UUID from EXTENSION: %02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                    value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
                    value[8], value[9], value[10], value[11], value[12], value[13], value[14], value[15]
                );
                pg_uuid_t *uuid_val = (pg_uuid_t*) this->allocator->fast_alloc(sizeof(pg_uuid_t));
                memcpy(uuid_val->data, value, UUID_LEN);
                res = UUIDPGetDatum(uuid_val);  // convert pointer to Datum
            }
            else
            {
                /* Build bytea */
                int64 bytea_len = vallen + VARHDRSZ;
                bytea *b = (bytea *) this->allocator->fast_alloc(bytea_len);
                SET_VARSIZE(b, bytea_len);
                memcpy(VARDATA(b), value, vallen);

                res = PointerGetDatum(b);
            }
            break;
        throw Error("unsupported user-extension column type: %s",
                    typinfo.arrow.type_name.c_str());
        }
        /* TODO: add other types */
        default:
            throw Error("unsupported column type: %s",
                        typinfo.arrow.type_name.c_str());
    }

    /* Call cast function if needed */
    if (typinfo.need_cast)
        res = do_cast(res, typinfo);

    return res;
}

/*
 * nested_list_to_datum
 *      Returns postgres array build from elements of array. Only one
 *      dimensional arrays are supported.
 */
Datum ParquetReader::nested_list_to_datum(arrow::Array *array, int pos,
                                           const TypeInfo &typinfo)
{
    ArrayType  *res;
    Datum      *values;
    bool       *nulls = nullptr;
    int         dims[1];
    int         lbs[1];
    bool        error = false;

    /*
     * The schema manifest may report LIST while the actual array data uses
     * LARGE_LIST (64-bit offsets). Check the actual array type to use the
     * correct accessor.
     */
    std::shared_ptr<arrow::Array> sliced;
    if (array->type_id() == arrow::Type::LARGE_LIST)
    {
        auto *larray = static_cast<arrow::LargeListArray *>(array);
        sliced = larray->values()->Slice(larray->value_offset(pos),
                                         larray->value_length(pos));
    }
    else
    {
        auto *larray = static_cast<arrow::ListArray *>(array);
        sliced = larray->values()->Slice(larray->value_offset(pos),
                                         larray->value_length(pos));
    }

    const TypeInfo &elemtypinfo = typinfo.children[0];

    values = (Datum *) this->allocator->fast_alloc(sizeof(Datum) * sliced->length());

#if SIZEOF_DATUM == 8
    /* Fill values and nulls arrays */
    if (sliced->null_count() == 0 &&
        (elemtypinfo.arrow.type_id == arrow::Type::INT64 ||
         elemtypinfo.arrow.type_id == arrow::Type::UINT32))
    {
        /*
         * Ok, there are no nulls, so probably we could just memcpy the
         * entire array.
         *
         * Warning: the code below is based on the assumption that Datum is
         * 8 bytes long, which is true for most contemporary systems but this
         * will not work on some exotic or really old systems.
         */
        copy_to_c_array<int64_t>((int64_t *) values, sliced.get(), elemtypinfo.pg.len);
        goto construct_array;
    }
#endif
    for (int64_t i = 0; i < sliced->length(); ++i)
    {
        if (!sliced->IsNull(i))
            values[i] = this->read_primitive_type(sliced.get(), elemtypinfo, i);
        else
        {
            if (!nulls)
            {
                Size size = sizeof(bool) * sliced->length();

                nulls = (bool *) this->allocator->fast_alloc(size);
                memset(nulls, 0, size);
            }
            nulls[i] = true;
        }
    }

construct_array:
    /*
     * Construct one dimensional array. We have to use PG_TRY / PG_CATCH
     * to prevent any kind leaks of resources allocated by c++ in case of
     * errors.
     */
    dims[0] = sliced->length();
    lbs[0] = 1;
    PG_TRY();
    {
        PgMemoryContextGuard guard(allocator->context());
        res = construct_md_array(values, nulls, 1, dims, lbs,
                                 elemtypinfo.pg.oid, elemtypinfo.pg.len,
                                 elemtypinfo.pg.byval, elemtypinfo.pg.align);
    }
    PG_CATCH();
    {
        error = true;
    }
    PG_END_TRY();
    if (error)
        throw std::runtime_error("failed to constuct an array");

    return PointerGetDatum(res);
}

Datum
ParquetReader::map_to_datum(arrow::MapArray *maparray, int pos,
                            const TypeInfo &typinfo)
{
	JsonbParseState *parseState = nullptr;
    JsonbValue *jb;
    Datum       res;

    auto keys = maparray->keys()->Slice(maparray->value_offset(pos),
                                        maparray->value_length(pos));
    auto values = maparray->items()->Slice(maparray->value_offset(pos),
                                           maparray->value_length(pos));

    Assert(keys->length() == values->length());
    Assert(typinfo.children.size() == 2);

    jb = pushJsonbValue(&parseState, WJB_BEGIN_OBJECT, nullptr);

    for (int i = 0; i < keys->length(); ++i)
    {
        Datum   key = (Datum) 0,
                value = (Datum) 0;
        bool    isnull = false;
        const TypeInfo &key_typinfo = typinfo.children[0];
        const TypeInfo &val_typinfo = typinfo.children[1];

        if (keys->IsNull(i))
            throw std::runtime_error("key is null");
        
        if (!values->IsNull(i))
        {
            key = this->read_primitive_type(keys.get(), key_typinfo, i);
            value = this->read_primitive_type(values.get(), val_typinfo, i);
        } else
            isnull = true;

        /* TODO: adding cstring would be cheaper than adding text */
        datum_to_jsonb(key, key_typinfo.pg.oid, false, key_typinfo.outfunc,
                       parseState, true);
        datum_to_jsonb(value, val_typinfo.pg.oid, isnull, val_typinfo.outfunc,
                       parseState, false);
    }

    jb = pushJsonbValue(&parseState, WJB_END_OBJECT, nullptr);
    res = JsonbPGetDatum(JsonbValueToJsonb(jb));

    if (typinfo.need_cast)
        res = do_cast(res, typinfo);

    return res;
}


/*
 * find_castfunc
 *      Check wether implicit cast will be required and prepare cast function
 *      call.
 */
void ParquetReader::initialize_cast(TypeInfo &typinfo, const char *attname)
{
    MemoryContext ccxt = CurrentMemoryContext;
    Oid         src_oid = to_postgres_type(typinfo.arrow.type.get());
    Oid         dst_oid = typinfo.pg.oid;
    bool        error = false;
    char        errstr[ERROR_STR_LEN];

    if (!OidIsValid(src_oid))
    {
        if (typinfo.arrow.type_id == arrow::Type::MAP)
            src_oid = JSONBOID;
        else
            elog(ERROR, "failed to initialize cast function for column '%s'",
                 attname);
    }

    PG_TRY();
    {

        if (IsBinaryCoercible(src_oid, dst_oid))
        {
            typinfo.castfunc = nullptr;
        }
        else
        {
            CoercionPathType ct;
            Oid     funcid;

            ct = find_coercion_pathway(dst_oid, src_oid,
                                       COERCION_EXPLICIT,
                                       &funcid);
            switch (ct)
            {
                case COERCION_PATH_FUNC:
                    {
                        PgMemoryContextGuard guard(CurTransactionContext);
                        typinfo.castfunc = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
                        fmgr_info(funcid, typinfo.castfunc);
                        typinfo.need_cast = true;
                        break;
                    }

                case COERCION_PATH_RELABELTYPE:
                    /* Cast is not needed */
                    typinfo.castfunc = nullptr;
                    break;

                case COERCION_PATH_COERCEVIAIO:
                    /* Cast via IO */
                    typinfo.outfunc = find_outfunc(src_oid);
                    typinfo.infunc = find_infunc(dst_oid);
                    typinfo.need_cast = true;
                    break;

                default:
                    elog(ERROR, "coercion pathway from '%s' to '%s' not found",
                         format_type_be(src_oid), format_type_be(dst_oid));
            }
        }
    }
    PG_CATCH();
    {
        ErrorData *errdata;

        ccxt = MemoryContextSwitchTo(ccxt);
        error = true;
        errdata = CopyErrorData();
        FlushErrorState();

        strncpy(errstr, errdata->message, ERROR_STR_LEN - 1);
        errstr[ERROR_STR_LEN - 1] = '\0';
        FreeErrorData(errdata);
        MemoryContextSwitchTo(ccxt);
    }
    PG_END_TRY();
    if (error)
        throw Error("failed to initialize cast function for column '%s' (%s)",
                    attname, errstr);
}

FmgrInfo *ParquetReader::find_outfunc(Oid typoid)
{
    Oid         funcoid;
    bool        isvarlena;
    FmgrInfo   *outfunc;

    getTypeOutputInfo(typoid, &funcoid, &isvarlena);

    if (!OidIsValid(funcoid))
        elog(ERROR, "output function for '%s' not found", format_type_be(typoid));

    PgMemoryContextGuard guard(CurTransactionContext);
    outfunc = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
    fmgr_info(funcoid, outfunc);

    return outfunc;
}

FmgrInfo *ParquetReader::find_infunc(Oid typoid)
{
    Oid         funcoid;
    Oid         typIOParam;
    FmgrInfo   *infunc;

    getTypeInputInfo(typoid, &funcoid, &typIOParam);

    if (!OidIsValid(funcoid))
        elog(ERROR, "input function for '%s' not found", format_type_be(typoid));

    PgMemoryContextGuard guard(CurTransactionContext);
    infunc = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
    fmgr_info(funcoid, infunc);

    return infunc;
}

/*
 * copy_to_c_array
 *      memcpy plain values from Arrow array to a C array.
 */
template<typename T> inline void
ParquetReader::copy_to_c_array(T *values, const arrow::Array *array, int elem_size)
{
    const T *in = GetPrimitiveValues<T>(*array);

    memcpy(values, in, elem_size * array->length());
}

/*
 * GetPrimitiveValues
 *      Get plain C value array. Copy-pasted from Arrow.
 */
template <typename T> inline const T*
ParquetReader::GetPrimitiveValues(const arrow::Array& arr) {
    if (arr.length() == 0) {
        return nullptr;
    }
    const auto& prim_arr = arrow::internal::checked_cast<const arrow::PrimitiveArray&>(arr);
    const T* raw_values = reinterpret_cast<const T*>(prim_arr.values()->data());
    return raw_values + arr.offset();
}

void ParquetReader::set_rowgroups_list(const std::vector<int> &rowgroups)
{
    this->rowgroups = rowgroups;
}

void ParquetReader::set_options(bool use_threads, bool use_mmap)
{
    this->use_threads = use_threads;
    this->use_mmap = use_mmap;
}

void ParquetReader::set_coordinator(ParallelCoordinator *coord)
{
    this->coordinator = coord;
}

/*
 * set_partition_values
 *      Set the partition values for this reader, extracted from the file path.
 *      This is called when hive_partitioning is enabled to provide values for
 *      virtual columns that are not in the Parquet file but come from the path.
 */
void ParquetReader::set_partition_values(const std::vector<HivePartitionValue> &partitions,
                                         TupleDesc tupleDesc,
                                         const std::set<int> &attrs_used)
{
    this->partition_columns.clear();
    this->partition_columns.resize(tupleDesc->natts);

    for (int i = 0; i < tupleDesc->natts; i++)
    {
        this->partition_columns[i].attnum = -1;  /* not a partition column by default */
    }

    /*
     * For each partition value from the path, check if there's a matching
     * column in the tuple descriptor that's not in the Parquet file.
     */
    for (const auto &pv : partitions)
    {
        char pg_colname[NAMEDATALEN];

        /* Find matching column in tuple descriptor */
        for (int i = 0; i < tupleDesc->natts; i++)
        {
            AttrNumber attnum = i + 1 - FirstLowInvalidHeapAttributeNumber;

            /* Skip columns not used in the query */
            if (attrs_used.find(attnum) == attrs_used.end())
                continue;

            /* Skip columns that already have a mapping (from Parquet file) */
            if (this->map[i] >= 0)
                continue;

            tolowercase(NameStr(TupleDescAttr(tupleDesc, i)->attname), pg_colname);

            /* Check if this column name matches the partition key */
            if (pv.key == pg_colname)
            {
                Oid col_type = TupleDescAttr(tupleDesc, i)->atttypid;

                /* Convert the partition value to the column's type */
                bool isnull;
                Datum value = string_to_datum(pv.value.c_str(), col_type, &isnull);

                this->partition_columns[i].attnum = i;
                this->partition_columns[i].pg_type = col_type;
                this->partition_columns[i].value = value;
                this->partition_columns[i].isnull = isnull;
                this->partition_columns[i].key = pv.key;

                elog(DEBUG2, "parquet_fdw: mapped partition column %s=%s to attribute %d",
                     pv.key.c_str(), pv.value.c_str(), i);
                break;
            }
        }
    }
}

int ParquetReader::acquire_next_rowgroup(int &row_group)
{
    /*
     * In case of parallel query get the row group index from the
     * coordinator. Otherwise just increment it.
     */
    if (coordinator)
    {
        SpinLockGuard guard(*coordinator);
        if ((row_group = coordinator->next_rowgroup(reader_id)) == -1)
            return -1;
    }
    else
        row_group++;

    /*
     * row_group cannot be less than zero at this point so it is safe to cast
     * it to unsigned int
     */
    Assert(row_group >= 0);
    if (static_cast<size_t>(row_group) >= this->rowgroups.size())
        return -1;

    return this->rowgroups[row_group];
}

class DefaultParquetReader : public ParquetReader
{
private:
    struct ChunkInfo
    {
        int     chunk;      /* current chunk number */
        int64   pos;        /* current pos within chunk */
        int64   len;        /* current chunk length */

        ChunkInfo (int64 len) : chunk(0), pos(0), len(len) {}
    };

    /* Current row group */
    std::shared_ptr<arrow::Table>   table;

    /*
     * Plain pointers to inner the structures of row group. It's needed to
     * prevent excessive shared_ptr management.
     */
    std::vector<arrow::Array *>     chunks;

    int             row_group;          /* current row group index */
    uint32_t        row;                /* current row within row group */
    uint32_t        num_rows;           /* total rows in row group */
    std::vector<ChunkInfo> chunk_info;  /* current chunk and position per-column */

public:
    /* 
     * Constructor.
     * The reader_id parameter is only used for parallel execution of
     * MultifileExecutionState.
     */
    DefaultParquetReader(const char* filename, MemoryContext cxt, int reader_id = -1)
        : ParquetReader(cxt), row_group(-1), row(0), num_rows(0)
    {
        this->filename = filename;
        this->reader_id = reader_id;
        this->coordinator = nullptr;
        this->initialized = false;
    }

    ~DefaultParquetReader()
    {}

    void open()
    {
        auto result = parquet::arrow::FileReader::Make(
                        arrow::default_memory_pool(),
                        parquet::ParquetFileReader::OpenFile(filename, use_mmap));
        if (!result.ok())
            throw Error("failed to open Parquet file %s ('%s')",
                        result.status().message().c_str(), filename.c_str());
        this->reader = std::move(result).ValueUnsafe();

        /* Enable parallel columns decoding/decompression if needed */
        this->reader->set_use_threads(this->use_threads && parquet_fdw_use_threads);
    }

    void close()
    {
        throw std::runtime_error("DefaultParquetReader::close() not implemented");
    }

    bool read_next_rowgroup()
    {
        arrow::Status               status;

        int rowgroup = acquire_next_rowgroup(this->row_group);
        if (rowgroup < 0)
            return false;
        auto rowgroup_meta = this->reader
                                ->parquet_reader()
                                ->metadata()
                                ->RowGroup(rowgroup);

        status = this->reader
            ->RowGroup(rowgroup)
            ->ReadTable(this->indices, &this->table);

        if (!status.ok())
            throw Error("failed to read rowgroup #%i: %s ('%s')",
                        rowgroup, status.message().c_str(), this->filename.c_str());

        if (!this->table)
            throw std::runtime_error("got empty table");

        /* TODO: don't clear each time */
        this->chunk_info.clear();
        this->chunks.clear();

        for (uint64_t i = 0; i < types.size(); ++i)
        {
            const auto &column = this->table->column(i);

            int64 len = column->chunk(0)->length();
            this->chunk_info.emplace_back(len);
            this->chunks.push_back(column->chunk(0).get());
        }

        this->row = 0;
        this->num_rows = this->table->num_rows();

        return true;
    }

    ReadStatus next(TupleTableSlot *slot, bool fake=false)
    {
        allocator->recycle();

        if (this->row >= this->num_rows)
        {
            /*
             * Read next row group. We do it in a loop to skip possibly empty
             * row groups.
             */
            do
            {
                if (!this->read_next_rowgroup())
                    return RS_EOF;
            }
            while (!this->num_rows);
        }

        this->populate_slot(slot, fake);
        this->row++;

        return RS_SUCCESS;
    }

    /*
     * populate_slot
     *      Fill slot with the values from parquet row.
     *
     * If `fake` set to true the actual reading and populating the slot is skipped.
     * The purpose of this feature is to correctly skip rows to collect sparse
     * samples.
     */
    void populate_slot(TupleTableSlot *slot, bool fake=false)
    {
        /* Fill slot values */
        for (int attr = 0; attr < slot->tts_tupleDescriptor->natts; attr++)
        {
            int arrow_col = this->map[attr];
            /*
             * We only fill slot attributes if column was referred in targetlist
             * or clauses. In other cases mark attribute as NULL.
             * */
            if (arrow_col >= 0)
            {
                ChunkInfo   &chunkInfo = this->chunk_info[arrow_col];
                arrow::Array *array = this->chunks[arrow_col];
                TypeInfo    &typinfo = this->types[arrow_col];

                if (chunkInfo.pos >= chunkInfo.len)
                {
                    const auto &column = this->table->column(arrow_col);

                    /* There are no more chunks */
                    if (++chunkInfo.chunk >= column->num_chunks())
                        break;

                    array = column->chunk(chunkInfo.chunk).get();
                    this->chunks[arrow_col] = array;
                    chunkInfo.pos = 0;
                    chunkInfo.len = array->length();
                }

                /* Don't do actual reading data into slot in fake mode */
                if (fake)
                    continue;

                if (array->IsNull(chunkInfo.pos))
                {
                    slot->tts_isnull[attr] = true;
                    chunkInfo.pos++;
                    continue;
                }
                slot->tts_isnull[attr] = false;

                /* Currently only primitive types and lists are supported */
                switch (typinfo.arrow.type_id)
                {
                    case arrow::Type::LIST:
                    case arrow::Type::LARGE_LIST:
                    {
                        slot->tts_values[attr] =
                            this->nested_list_to_datum(array, chunkInfo.pos,
                                                       typinfo);
                        break;
                    }
                    case arrow::Type::MAP:
                    {
                        auto *maparray = static_cast<arrow::MapArray *>(array);
                        Datum       jsonb = this->map_to_datum(maparray, chunkInfo.pos, typinfo);

                        /*
                         * Copy jsonb into memory block allocated by
                         * FastAllocator to prevent its destruction though
                         * to be able to recycle it once it fulfilled its
                         * purpose.
                         */
                        void       *jsonb_val = allocator->fast_alloc(VARSIZE_ANY(jsonb));

                        memcpy(jsonb_val, DatumGetPointer(jsonb), VARSIZE_ANY(jsonb));
                        pfree(DatumGetPointer(jsonb));

                        slot->tts_values[attr] = PointerGetDatum(jsonb_val);
                        break;
                    }
                    default:
                        slot->tts_values[attr] = 
                            this->read_primitive_type(array, typinfo, chunkInfo.pos);
                }

                chunkInfo.pos++;
            }
            else
            {
                /*
                 * Check if this is a virtual partition column (from Hive path).
                 * If so, use the partition value; otherwise mark as NULL.
                 */
                if (static_cast<size_t>(attr) < this->partition_columns.size() &&
                    this->partition_columns[attr].attnum >= 0)
                {
                    slot->tts_values[attr] = this->partition_columns[attr].value;
                    slot->tts_isnull[attr] = this->partition_columns[attr].isnull;
                }
                else
                {
                    slot->tts_isnull[attr] = true;
                }
            }
        }
    }

    void rescan(void)
    {
        this->row_group = -1;
        this->row = 0;
        this->num_rows = 0;
    }
};

class CachingParquetReader : public ParquetReader
{
private:
    std::vector<void *>             column_data;
    std::vector<std::vector<bool> > column_nulls;

    bool            is_active;          /* weather reader is active */

    int             row_group;          /* current row group index */
    uint32_t        row;                /* current row within row group */
    uint32_t        num_rows;           /* total rows in row group */

public:
    CachingParquetReader(const char* filename, MemoryContext cxt, int reader_id = -1)
        : ParquetReader(cxt), is_active(false), row_group(-1), row(0), num_rows(0)
    {
        this->filename = filename;
        this->reader_id = reader_id;
        this->coordinator = nullptr;
        this->initialized = false;
    }

    ~CachingParquetReader()
    {}

    void open()
    {
        auto result = parquet::arrow::FileReader::Make(
                        arrow::default_memory_pool(),
                        parquet::ParquetFileReader::OpenFile(filename, use_mmap));
        if (!result.ok())
            throw Error("failed to open Parquet file %s ('%s')",
                        result.status().message().c_str(), filename.c_str());
        this->reader = std::move(result).ValueUnsafe();

        /* Enable parallel columns decoding/decompression if needed */
        this->reader->set_use_threads(this->use_threads && parquet_fdw_use_threads);

        is_active = true;
    }

    void close()
    {
        this->reader = nullptr;  /* destroy the reader */
        is_active = false;
    }

    bool read_next_rowgroup()
    {
        arrow::Status                   status;
        std::shared_ptr<arrow::Table>   table;

        /* TODO: release previously stored data */
        this->column_data.resize(this->types.size(), nullptr);
        this->column_nulls.resize(this->types.size());

        int rowgroup = acquire_next_rowgroup(this->row_group);
        if (rowgroup < 0)
            return false;
        auto rowgroup_meta = this->reader
                                ->parquet_reader()
                                ->metadata()
                                ->RowGroup(rowgroup);

        status = this->reader
            ->RowGroup(rowgroup)
            ->ReadTable(this->indices, &table);
        if (!status.ok())
            throw Error("failed to read rowgroup #%i: %s ('%s')",
                        rowgroup, status.message().c_str(), this->filename.c_str());

        /* Release resources acquired in the previous iteration */
        allocator->recycle();

        /* Read columns data and store it into column_data vector */
        for (std::vector<TypeInfo>::size_type col = 0; col < types.size(); ++col)
        {
            bool            has_nulls;
            std::shared_ptr<parquet::Statistics>  stats;

            if (types[col].index >= 0)
                stats = rowgroup_meta->ColumnChunk(types[col].index)->statistics();
            has_nulls = stats ? stats->null_count() > 0 : true;

            this->num_rows = table->num_rows();
            this->column_nulls[col].resize(this->num_rows);

            this->read_column(table, col, has_nulls);
        }

        this->row = 0;
        return true;
    }

    void read_column(std::shared_ptr<arrow::Table> table,
                     int col,
                     bool has_nulls)
    {
        std::shared_ptr<arrow::ChunkedArray> column = table->column(col);
        TypeInfo &typinfo = this->types[col];
        void   *data;
        size_t  sz;
        int     row = 0;

        switch(typinfo.arrow.type_id) {
            case arrow::Type::BOOL:
                sz = sizeof(bool);
                break;
            case arrow::Type::INT8:
                sz = sizeof(int8);
                break;
            case arrow::Type::UINT8:
                sz = sizeof(uint8);
                break;
            case arrow::Type::INT16:
                sz = sizeof(int16);
                break;
            case arrow::Type::UINT16:
                sz = sizeof(uint16);
                break;
            case arrow::Type::INT32:
                sz = sizeof(int32);
                break;
            case arrow::Type::UINT32:
                sz = sizeof(uint32);
                break;
            case arrow::Type::FLOAT:
                sz = sizeof(float);
                break;
            case arrow::Type::DATE32:
                sz = sizeof(int);
                break;
            default:
                sz = sizeof(Datum);
        }

        data = allocator->fast_alloc(sz * num_rows);

        for (int i = 0; i < column->num_chunks(); ++i) {
            arrow::Array *array = column->chunk(i).get();

            /*
             * XXX We could probably optimize here by copying the entire array
             * by using copy_to_c_array when has_nulls = false.
             */

            for (int j = 0; j < array->length(); ++j) {
                if (has_nulls && array->IsNull(j)) {
                    this->column_nulls[col][row++] = true;
                    continue;
                }
                switch (typinfo.arrow.type_id)
                {
                    /*
                     * For types smaller than Datum (assuming 8 bytes) we
                     * copy raw values to save memory and only convert them
                     * into Datum on the slot population stage.
                     */
                    case arrow::Type::BOOL:
                        {
                            auto *boolarray = static_cast<arrow::BooleanArray *>(array);
                            static_cast<bool *>(data)[row] = boolarray->Value(row);
                            break;
                        }
                    case arrow::Type::INT8:
                        {
                            auto *intarray = static_cast<arrow::Int8Array *>(array);
                            static_cast<int8 *>(data)[row] = intarray->Value(row);
                            break;
                        }
                    case arrow::Type::INT16:
                        {
                            auto *intarray = static_cast<arrow::Int16Array *>(array);
                            static_cast<int16 *>(data)[row] = intarray->Value(row);
                            break;
                        }
                    case arrow::Type::INT32:
                        {
                            auto *intarray = static_cast<arrow::Int32Array *>(array);
                            static_cast<int32 *>(data)[row] = intarray->Value(row);
                            break;
                        }
                    case arrow::Type::FLOAT:
                        {
                            auto *farray = static_cast<arrow::FloatArray *>(array);
                            static_cast<float *>(data)[row] = farray->Value(row);
                            break;
                        }
                    case arrow::Type::DATE32:
                        {
                            auto *tsarray = static_cast<arrow::Date32Array *>(array);
                            static_cast<int *>(data)[row] = tsarray->Value(row);
                            break;
                        }
                    case arrow::Type::UINT8:
                        {
                            auto *intarray = static_cast<arrow::UInt8Array *>(array);
                            static_cast<uint8 *>(data)[row] = intarray->Value(row);
                            break;
                        }
                    case arrow::Type::UINT16:
                        {
                            auto *intarray = static_cast<arrow::UInt16Array *>(array);
                            static_cast<uint16 *>(data)[row] = intarray->Value(row);
                            break;
                        }
                    case arrow::Type::UINT32:
                        {
                            auto *intarray = static_cast<arrow::UInt32Array *>(array);
                            static_cast<uint32 *>(data)[row] = intarray->Value(row);
                            break;
                        }

                    case arrow::Type::LIST:
                    case arrow::Type::LARGE_LIST:
                        {
                            static_cast<Datum *>(data)[row] =
                                this->nested_list_to_datum(array, j, typinfo);
                            break;
                        }
                    case arrow::Type::MAP:
                        {
                            auto *maparray = static_cast<arrow::MapArray *>(array);
                            Datum       jsonb = this->map_to_datum(maparray, j, typinfo);

                            /*
                             * Copy jsonb into memory block allocated by
                             * FastAllocator to prevent its destruction though
                             * to be able to recycle it once it fulfilled its
                             * purpose.
                             */
                            void       *jsonb_val = allocator->fast_alloc(VARSIZE_ANY(jsonb));

                            memcpy(jsonb_val, DatumGetPointer(jsonb), VARSIZE_ANY(jsonb));
                            pfree(DatumGetPointer(jsonb));

                            static_cast<Datum *>(data)[row] = PointerGetDatum(jsonb_val);

                            break;
                        }
                    default:
                        /*
                         * For larger types we copy already converted into
                         * Datum values.
                         */
                        static_cast<Datum *>(data)[row] =
                            this->read_primitive_type(array, typinfo, j);
                }
                this->column_nulls[col][row] = false;

                row++;
            }
        }

        this->column_data[col] = data;
    }

    ReadStatus next(TupleTableSlot *slot, bool fake=false)
    {
        if (this->row >= this->num_rows)
        {
            if (!is_active)
                return RS_INACTIVE;

            /*
             * Read next row group. We do it in a loop to skip possibly empty
             * row groups.
             */
            do
            {
                if (!this->read_next_rowgroup())
                    return RS_EOF;
            }
            while (!this->num_rows);
        }

        if (!fake)
            this->populate_slot(slot, false);

        return RS_SUCCESS;
    }

    void populate_slot(TupleTableSlot *slot, bool fake=false)
    {
        /* Fill slot values */
        for (int attr = 0; attr < slot->tts_tupleDescriptor->natts; attr++)
        {
            int arrow_col = this->map[attr];
            /*
             * We only fill slot attributes if column was referred in targetlist
             * or clauses. In other cases mark attribute as NULL.
             * */
            if (arrow_col >= 0)
            {
                TypeInfo &typinfo = this->types[arrow_col];
                void     *data = this->column_data[arrow_col];
                bool      need_cast = typinfo.need_cast;

                switch(typinfo.arrow.type_id)
                {
                    case arrow::Type::BOOL:
                        slot->tts_values[attr] = BoolGetDatum(static_cast<bool *>(data)[this->row]);
                        break;
                    case arrow::Type::INT8:
                        slot->tts_values[attr] = Int8GetDatum(static_cast<int8 *>(data)[this->row]);
                        break;
                    case arrow::Type::INT16:
                        slot->tts_values[attr] = Int16GetDatum(static_cast<int16 *>(data)[this->row]);
                        break;
                    case arrow::Type::INT32:
                        slot->tts_values[attr] = Int32GetDatum(static_cast<int32 *>(data)[this->row]);
                        break;
                    case arrow::Type::FLOAT:
                        slot->tts_values[attr] = Float4GetDatum(static_cast<float *>(data)[this->row]);
                        break;
                    case arrow::Type::UINT8:
                        slot->tts_values[attr] = Int16GetDatum((int16) static_cast<uint8 *>(data)[this->row]);
                        break;
                    case arrow::Type::UINT16:
                        slot->tts_values[attr] = Int32GetDatum((int32) static_cast<uint16 *>(data)[this->row]);
                        break;
                    case arrow::Type::UINT32:
                        slot->tts_values[attr] = Int64GetDatum((int64) static_cast<uint32 *>(data)[this->row]);
                        break;
                    case arrow::Type::DATE32:
                        {
                            /*
                             * Postgres date starts with 2000-01-01 while unix date (which
                             * Parquet is using) starts with 1970-01-01. So we need to do
                             * simple calculations here.
                             */
                            int dt = static_cast<int *>(data)[this->row]
                                + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE);
                            slot->tts_values[attr] = DateADTGetDatum(dt);
                        }
                        break;
                    default:
                        slot->tts_values[attr] = static_cast<Datum *>(data)[this->row];
                        need_cast = false;
                }

                if (need_cast)
                    slot->tts_values[attr] = do_cast(slot->tts_values[attr], typinfo);
                slot->tts_isnull[attr] = this->column_nulls[arrow_col][this->row];
            }
            else
            {
                /*
                 * Check if this is a virtual partition column (from Hive path).
                 * If so, use the partition value; otherwise mark as NULL.
                 */
                if (static_cast<size_t>(attr) < this->partition_columns.size() &&
                    this->partition_columns[attr].attnum >= 0)
                {
                    slot->tts_values[attr] = this->partition_columns[attr].value;
                    slot->tts_isnull[attr] = this->partition_columns[attr].isnull;
                }
                else
                {
                    slot->tts_isnull[attr] = true;
                }
            }
        }

        this->row++;
    }

    void rescan(void)
    {
        this->row_group = -1;
        this->row = 0;
        this->num_rows = 0;
    }
};

ParquetReader *create_parquet_reader(const char *filename,
                                     MemoryContext cxt,
                                     int reader_id,
                                     bool caching)
{
#ifdef CACHING_TEST
    /* For testing purposes only */
    caching = true;
#endif

    if (!caching)
        return new DefaultParquetReader(filename, cxt, reader_id);
    else
        return new CachingParquetReader(filename, cxt, reader_id);
}


/* Default destructor is required */
ParquetReader::~ParquetReader()
{}
