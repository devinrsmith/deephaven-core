/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.CommonTransform;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.TypedFunction;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.regex.Pattern;

import static io.deephaven.util.type.ArrayTypeUtils.EMPTY_BYTE_ARRAY;
import static io.deephaven.util.type.ArrayTypeUtils.EMPTY_DOUBLE_ARRAY;
import static io.deephaven.util.type.ArrayTypeUtils.EMPTY_FLOAT_ARRAY;
import static io.deephaven.util.type.ArrayTypeUtils.EMPTY_INT_ARRAY;
import static io.deephaven.util.type.ArrayTypeUtils.EMPTY_LONG_ARRAY;

/**
 * Convert an Avro {@link GenericRecord} to Deephaven rows.
 * <p>
 * Each GenericRecord produces a single row of output, according to the maps of Table column names to Avro field names
 * for the keys and values.
 */
public class GenericRecordChunkAdapter extends MultiFieldChunkAdapter {

    private static final ObjectFunction<Object, GenericRecord> GENERIC_RECORD_OBJ = ObjectFunction.cast(Type.ofCustom(GenericRecord.class));

    private GenericRecordChunkAdapter(
            final TableDefinition definition,
            final IntFunction<ChunkType> chunkTypeForIndex,
            final Map<String, String> fieldNamesToColumnNames,
            final Pattern separator,
            final Schema schema,
            final boolean allowNulls) {
        super(definition, chunkTypeForIndex, fieldNamesToColumnNames, allowNulls, (fieldPathStr, chunkType,
                dataType, componentType) -> GenericRecordChunkAdapter.makeFieldCopier(schema, fieldPathStr, separator,
                        chunkType,
                        dataType, componentType));
    }

    /**
     * Create a GenericRecordChunkAdapter.
     *
     * @param definition the definition of the output table
     * @param chunkTypeForIndex a function from column index to chunk type
     * @param columns a map from Avro field paths to Deephaven column names
     * @param separator separator for composite fields names
     * @param schema the Avro schema for our input
     * @param allowNulls true if null records should be allowed, if false then an ISE is thrown
     * @return a GenericRecordChunkAdapter for the given definition and column mapping
     */
    public static GenericRecordChunkAdapter make(
            final TableDefinition definition,
            final IntFunction<ChunkType> chunkTypeForIndex,
            final Map<String, String> columns,
            final Pattern separator,
            final Schema schema,
            final boolean allowNulls) {
        return new GenericRecordChunkAdapter(
                definition, chunkTypeForIndex, columns, separator, schema, allowNulls);
    }

    private static Schema getFieldSchema(final Schema schema, final String fieldPathStr, Pattern separator) {
        final String[] fieldPath = GenericRecordUtil.getFieldPath(fieldPathStr, separator);
        final Schema fieldSchema = GenericRecordUtil.getFieldSchema(schema, fieldPath);
        return fieldSchema;
    }

    private static LogicalType getLogicalType(final Schema schema, final String fieldPathStr, Pattern separator) {
        final Schema fieldSchema = getFieldSchema(schema, fieldPathStr, separator);
        final LogicalType logicalType = fieldSchema.getLogicalType();
        return logicalType;
    }

    private static LogicalType getArrayTypeLogicalType(final Schema schema, final String fieldPathStr,
            Pattern separator) {
        final Schema fieldSchema = getFieldSchema(schema, fieldPathStr, separator);
        final LogicalType logicalType = fieldSchema.getElementType().getLogicalType();
        return logicalType;
    }

    private static FieldCopier makeFieldCopier(
            final Schema schema,
            final String fieldPathStr,
            final Pattern separator,
            final ChunkType chunkType,
            final Class<?> dataType,
            final Class<?> componentType) {
        final TypedFunction<GenericRecord> recordFunction = makeFunction(schema, fieldPathStr, separator, chunkType, dataType, componentType);
        final TypedFunction<Object> tf = GENERIC_RECORD_OBJ.map(recordFunction);
        return FieldCopierAdapter.of(CommonTransform.of(tf));
    }

    private static TypedFunction<GenericRecord> makeFunction(
            final Schema schema,
            final String fieldPathStr,
            final Pattern separator,
            final ChunkType chunkType,
            final Class<?> dataType,
            final Class<?> componentType) {
        final int[] fieldPath = GenericRecordUtil.getFieldPath(fieldPathStr, separator, schema);
        final ObjectFunction<GenericRecord, Object> objectFunction = ObjectFunction.of(r -> GenericRecordUtil.getPath(r, fieldPath), Type.ofCustom(Object.class));
        switch (chunkType) {
            case Char:
                return objectFunction.asChecked(BoxedCharType.of());
            case Byte:
                if (dataType == Boolean.class || dataType == boolean.class) {
                    return objectFunction.asChecked(BoxedBooleanType.of());
                }
                return objectFunction.asChecked(BoxedByteType.of());
            case Short:
                return objectFunction.asChecked(BoxedShortType.of());
            case Int:
                return objectFunction.asChecked(BoxedIntType.of());
            case Long:
                if (dataType == Instant.class) {
                    final LogicalType logicalType = getLogicalType(schema, fieldPathStr, separator);
                    if (logicalType == null) {
                        throw new IllegalArgumentException(
                                "Can not map field without a logical type to Instant: field=" + fieldPathStr);
                    }
                    if (logicalType instanceof LogicalTypes.TimestampMillis) {
                        return objectFunction
                                .asChecked(BoxedLongType.of())
                                .mapLong(x -> x == null ? QueryConstants.NULL_LONG : x * 1_000_000L);
                    }
                    if (logicalType instanceof LogicalTypes.TimestampMicros) {
                        return objectFunction
                                .asChecked(BoxedLongType.of())
                                .mapLong(x -> x == null ? QueryConstants.NULL_LONG : x * 1_000L);
                    }
                    throw new IllegalArgumentException(
                            "Can not map field with unknown logical type to Instant: field=" + fieldPathStr
                                    + ", logical type=" + logicalType);
                }
                return objectFunction.asChecked(BoxedLongType.of());
            case Float:
                return objectFunction.asChecked(BoxedFloatType.of());
            case Double:
                return objectFunction.asChecked(BoxedDoubleType.of());
            case Object:
                if (dataType == String.class) {
                    return objectFunction
                            .asChecked(Type.ofCustom(Utf8.class))
                            .mapObj(x -> x == null ? null : x.toString(), Type.stringType());
                }
                if (dataType == BigDecimal.class) {
                    final String[] fieldPath2 = GenericRecordUtil.getFieldPath(fieldPathStr, separator);
                    final Schema fieldSchema = GenericRecordUtil.getFieldSchema(schema, fieldPath2);
                    final LogicalType logicalType = fieldSchema.getLogicalType();
                    if (logicalType instanceof LogicalTypes.Decimal) {
                        final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
                        return objectFunction.mapObj(new BigDecimalFunction(decimalType.getPrecision(), decimalType.getScale()));
                    }
                    throw new IllegalArgumentException(
                            "Can not map field with non matching logical type to BigDecimal: " +
                                    "field=" + fieldPathStr + ", logical type=" + logicalType);
                }
                if (dataType.isArray()) {
                    final ObjectFunction<GenericRecord, GenericArray> arrayFunction = objectFunction.asChecked(Type.ofCustom(GenericArray.class));
                    if (Instant.class.isAssignableFrom(componentType)) {
                        final LogicalType logicalType = getArrayTypeLogicalType(schema, fieldPathStr, separator);
                        if (logicalType == null) {
                            throw new IllegalArgumentException(
                                    "Can not map field without a logical type to Instant[]: field=" + fieldPathStr);
                        }
                        if (logicalType instanceof LogicalTypes.TimestampMillis) {
                            return arrayFunction
                                    .mapObj(g -> convertArray(g, 1_000_000L), Type.instantType().arrayType());
                        }
                        if (logicalType instanceof LogicalTypes.TimestampMicros) {
                            return arrayFunction
                                    .mapObj(g -> convertArray(g, 1_000L), Type.instantType().arrayType());
                        }
                        throw new IllegalArgumentException(
                                "Can not map field with unknown logical type to Instant[]: field=" + fieldPathStr
                                        + ", logical type=" + logicalType);
                    }

                    if (componentType.equals(byte.class)) {
                        return arrayFunction
                                //.as((GenericType<GenericArray<Byte>>)null)
                                .mapObj(GenericRecordChunkAdapter::toByteArray, Type.byteType().arrayType());
                    }
                    // avro doesn't have short type
                    if (componentType.equals(int.class)) {
                        return arrayFunction
                                .mapObj(GenericRecordChunkAdapter::toIntArray, Type.intType().arrayType());
                    }
                    if (componentType.equals(long.class)) {
                        return arrayFunction
                                .mapObj(GenericRecordChunkAdapter::toLongArray, Type.longType().arrayType());
                    }
                    if (componentType.equals(float.class)) {
                        return arrayFunction
                                .mapObj(GenericRecordChunkAdapter::toFloatArray, Type.floatType().arrayType());
                    }
                    if (componentType.equals(double.class)) {
                        return arrayFunction
                                .mapObj(GenericRecordChunkAdapter::toDoubleArray, Type.doubleType().arrayType());
                    }
                    final NativeArrayType<?, ?> arrayType = Type.find(componentType).arrayType();
                    //noinspection unchecked,rawtypes
                    return arrayFunction.mapObj(ga -> toArray(ga, componentType), (NativeArrayType) arrayType);
                }
                return objectFunction;
        }
        throw new IllegalArgumentException("Can not convert field of type " + dataType);
    }

    private static Instant[] convertArray(final GenericArray<Long> ga, final long multiplier) {
        if (ga == null) {
            return null;
        }
        final int gaSize = ga.size();
        if (gaSize == 0) {
            return DateTimeUtils.ZERO_LENGTH_INSTANT_ARRAY;
        }
        final Instant[] out = new Instant[ga.size()];
        int i = 0;
        for (Long o : ga) {
            out[i] = DateTimeUtils.epochNanosToInstant(multiplier * o);
            ++i;
        }
        return out;
    }

    private static byte[] toByteArray(GenericArray<Byte> ga) {
        if (ga == null) {
            return null;
        }
        final int gaSize = ga.size();
        if (gaSize == 0) {
            return EMPTY_BYTE_ARRAY;
        }
        final byte[] out = new byte[gaSize];
        int i = 0;
        for (Byte o : ga) {
            out[i++] = TypeUtils.unbox(o);
        }
        return out;
    }

    private static int[] toIntArray(GenericArray<Integer> ga) {
        if (ga == null) {
            return null;
        }
        final int gaSize = ga.size();
        if (gaSize == 0) {
            return EMPTY_INT_ARRAY;
        }
        final int[] out = new int[gaSize];
        int i = 0;
        for (Integer o : ga) {
            out[i++] = TypeUtils.unbox(o);
        }
        return out;
    }

    private static long[] toLongArray(GenericArray<Long> ga) {
        if (ga == null) {
            return null;
        }
        final int gaSize = ga.size();
        if (gaSize == 0) {
            return EMPTY_LONG_ARRAY;
        }
        final long[] out = new long[gaSize];
        int i = 0;
        for (Long o : ga) {
            out[i++] = TypeUtils.unbox(o);
        }
        return out;
    }

    private static float[] toFloatArray(GenericArray<Float> ga) {
        if (ga == null) {
            return null;
        }
        final int gaSize = ga.size();
        if (gaSize == 0) {
            return EMPTY_FLOAT_ARRAY;
        }
        final float[] out = new float[gaSize];
        int i = 0;
        for (Float o : ga) {
            out[i++] = TypeUtils.unbox(o);
        }
        return out;
    }

    private static double[] toDoubleArray(GenericArray<Double> ga) {
        if (ga == null) {
            return null;
        }
        final int gaSize = ga.size();
        if (gaSize == 0) {
            return EMPTY_DOUBLE_ARRAY;
        }
        final double[] out = new double[gaSize];
        int i = 0;
        for (Double o : ga) {
            out[i++] = TypeUtils.unbox(o);
        }
        return out;
    }

    private static <T> T[] toArray(GenericArray<T> ga, Class<T> componentType) {
        if (ga == null) {
            return null;
        }
        //noinspection unchecked
        final T[] out = (T[]) Array.newInstance(componentType, ga.size());
        int i = 0;
        for (T o : ga) {
            out[i++] = o;
        }
        return out;
    }
}
