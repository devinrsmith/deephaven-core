/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.Type;
import io.deephaven.functions.BooleanFunction;
import io.deephaven.functions.ByteFunction;
import io.deephaven.functions.CharFunction;
import io.deephaven.functions.CommonTransform;
import io.deephaven.functions.DoubleFunction;
import io.deephaven.functions.FloatFunction;
import io.deephaven.functions.LongFunction;
import io.deephaven.functions.ObjectFunction;
import io.deephaven.functions.ShortFunction;
import io.deephaven.functions.TypedFunction;

import java.time.Instant;
import java.util.Map;
import java.util.function.IntFunction;

public class JsonNodeChunkAdapter extends MultiFieldChunkAdapter {

    private static final CustomType<JsonNode> JSON_NODE_TYPE = Type.ofCustom(JsonNode.class);
    private static final ObjectFunction<Object, JsonNode> JSON_NODE_OBJ = ObjectFunction.identity(JSON_NODE_TYPE);
    private static final ObjectFunction<Object, Instant> INSTANT_OBJ = ObjectFunction.identity(Type.instantType());

    private JsonNodeChunkAdapter(
            final TableDefinition definition,
            final IntFunction<ChunkType> chunkTypeForIndex,
            final Map<String, String> jsonPointerStrToColumnNames,
            final boolean allowNulls) {
        super(definition, chunkTypeForIndex, jsonPointerStrToColumnNames, allowNulls,
                JsonNodeChunkAdapter::makeFieldCopier);
    }

    /**
     * Create a JsonRecordChunkAdapter.
     *
     * @param definition the definition of the output table
     * @param chunkTypeForIndex a function from column index to chunk type
     * @param jsonPointerStrToColumnNames a map from JSON pointer strings to Deephaven column names
     * @param allowNulls true if null records should be allowed, if false then an ISE is thrown
     * @return a JsonRecordChunkAdapter for the given definition and column mapping
     */
    public static JsonNodeChunkAdapter make(
            final TableDefinition definition,
            final IntFunction<ChunkType> chunkTypeForIndex,
            final Map<String, String> jsonPointerStrToColumnNames,
            final boolean allowNulls) {
        return new JsonNodeChunkAdapter(
                definition, chunkTypeForIndex, jsonPointerStrToColumnNames, allowNulls);
    }

    private static FieldCopier makeFieldCopier(
            final String fieldName, final ChunkType chunkType, final Class<?> dataType, final Class<?> componentType) {
        final TypedFunction<JsonNode> nodeFunction = jsonNodeFunction(fieldName, chunkType, dataType, componentType);
        final TypedFunction<Object> tf = JSON_NODE_OBJ.map(nodeFunction);
        return FieldCopierAdapter.of(CommonTransform.of(tf));
    }

    private static TypedFunction<JsonNode> jsonNodeFunction(
            final String fieldName, final ChunkType chunkType, final Class<?> dataType, final Class<?> componentType) {
        final JsonPointer ptr = JsonPointer.compile(fieldName);
        final boolean allowMissingKeys = true;
        final boolean allowNullValues = true;
        switch (chunkType) {
            case Boolean:
                return (BooleanFunction<JsonNode>) (n -> JsonNodeUtil.getBoolean(n, ptr, allowMissingKeys,
                        allowNullValues));
            case Char:
                return (CharFunction<JsonNode>) (n -> JsonNodeUtil.getChar(n, ptr, allowMissingKeys, allowNullValues));
            case Byte:
                if (dataType == Boolean.class) {
                    return strFunction(ptr, allowMissingKeys, allowNullValues)
                            .mapObj(x -> toBoolean(x, ptr), BoxedBooleanType.of());
                }
                return (ByteFunction<JsonNode>) (n -> JsonNodeUtil.getByte(n, ptr, allowMissingKeys, allowNullValues));
            case Short:
                return (ShortFunction<JsonNode>) (n -> JsonNodeUtil.getShort(n, ptr, allowMissingKeys,
                        allowNullValues));
            case Int:
                return (io.deephaven.functions.IntFunction<JsonNode>) (n -> JsonNodeUtil.getInt(n, ptr,
                        allowMissingKeys, allowNullValues));
            case Long:
                if (dataType == Instant.class) {
                    return ObjectFunction.of(n -> JsonNodeUtil.getInstant(n, ptr, allowMissingKeys, allowNullValues),
                            Type.instantType());
                }
                return (LongFunction<JsonNode>) (n -> JsonNodeUtil.getLong(n, ptr, allowMissingKeys, allowNullValues));
            case Float:
                return (FloatFunction<JsonNode>) (n -> JsonNodeUtil.getFloat(n, ptr, allowMissingKeys,
                        allowNullValues));
            case Double:
                return (DoubleFunction<JsonNode>) (n -> JsonNodeUtil.getDouble(n, ptr, allowMissingKeys,
                        allowNullValues));
            case Object:
                if (dataType == String.class) {
                    return strFunction(ptr, allowMissingKeys, allowNullValues);
                }
                if (dataType.isAssignableFrom(JSON_NODE_TYPE.clazz())) {
                    return ObjectFunction.of(n -> n == null ? null : n.at(ptr), JSON_NODE_TYPE);
                }
                throw new UncheckedDeephavenException("Type " + dataType.getSimpleName() + " not supported for JSON");
        }
        throw new IllegalArgumentException("Can not convert field of type " + dataType);
    }

    private static ObjectFunction<JsonNode, String> strFunction(JsonPointer ptr, boolean allowMissingKeys,
            boolean allowNullValues) {
        return ObjectFunction.of(n -> JsonNodeUtil.getString(n, ptr, allowMissingKeys, allowNullValues),
                Type.stringType());
    }

    private static Boolean toBoolean(String valueAsString, JsonPointer ptr) {
        if (valueAsString == null) {
            return null;
        } else {
            switch (valueAsString.trim()) {
                case "TRUE":
                case "True":
                case "true":
                case "T":
                case "t":
                case "1":
                    return true;
                case "FALSE":
                case "False":
                case "false":
                case "F":
                case "f":
                    return false;
                case "":
                    return null;
                default:
                    throw new UncheckedDeephavenException(
                            "value " + valueAsString + " not recognized as Boolean for field " + ptr);
            }
        }
    }
}
