package io.deephaven.kafka;

import io.deephaven.functions.ToBooleanFunction;
import io.deephaven.functions.ToDoubleFunction;
import io.deephaven.functions.ToFloatFunction;
import io.deephaven.functions.ToIntFunction;
import io.deephaven.functions.ToLongFunction;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.functions.TypedFunction;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

class AvroSchemaDecoder {
    private static final ToIntFunction<AvroParsingState> DECODE_INT = AvroParsingState::decodeInt;
    private static final ToLongFunction<AvroParsingState> DECODE_LONG = AvroParsingState::decodeLong;
    private static final ToFloatFunction<AvroParsingState> DECODE_FLOAT = AvroParsingState::decodeFloat;
    private static final ToDoubleFunction<AvroParsingState> DECODE_DOUBLE = AvroParsingState::decodeDouble;
    private static final ToBooleanFunction<AvroParsingState> DECODE_BOOLEAN = AvroParsingState::decodeBoolean;
    private static final ToObjectFunction<AvroParsingState, String> DECODE_STRING =
            ToObjectFunction.of(AvroParsingState::decodeString, io.deephaven.qst.type.Type.stringType());
    private static final ToIntFunction<AvroParsingState> DECODE_NULLABLE_INT = AvroParsingState::decodeNullableInt;
    private static final ToLongFunction<AvroParsingState> DECODE_NULLABLE_LONG = AvroParsingState::decodeNullableLong;
    private static final ToFloatFunction<AvroParsingState> DECODE_NULLABLE_FLOAT =
            AvroParsingState::decodeNullableFloat;
    private static final ToDoubleFunction<AvroParsingState> DECODE_NULLABLE_DOUBLE =
            AvroParsingState::decodeNullableDouble;
    private static final ToObjectFunction<AvroParsingState, String> DECODE_NULLABLE_STRING =
            ToObjectFunction.of(AvroParsingState::decodeNullableString, io.deephaven.qst.type.Type.stringType());
    private static final ToObjectFunction<AvroParsingState, Boolean> DECODE_NULLABLE_BOOLEAN = ToObjectFunction
            .of(AvroParsingState::decodeNullableBoolean, io.deephaven.qst.type.Type.booleanType().boxedType());

    static List<FieldFunction> of(Schema schema) {
        if (schema.getType() != Type.RECORD) {
            throw new IllegalArgumentException();
        }
        final List<FieldFunction> writers = new ArrayList<>();
        for (Field field : schema.getFields()) {
            writers.add(new FieldFunction(field, function(field)));
        }
        return writers;
    }

    static class FieldFunction {
        private final Field field;
        private final TypedFunction<AvroParsingState> function;

        FieldFunction(Field field, TypedFunction<AvroParsingState> function) {
            this.field = Objects.requireNonNull(field);
            this.function = Objects.requireNonNull(function);
        }

        public Field field() {
            return field;
        }

        public TypedFunction<AvroParsingState> function() {
            return function;
        }
    }

    private static TypedFunction<AvroParsingState> function(Field field) {
        final Schema schema = field.schema();
        switch (schema.getType()) {
            case UNION: {
                // Note: this impl is severely deficient, and assumes all unions are nullable unions;
                // and also assumes the null part comes second.
                final List<Schema> types = schema.getTypes();
                if (types.size() != 2) {
                    throw new RuntimeException();
                }
                if (types.get(1).getType() != Type.NULL) {
                    throw new RuntimeException();
                }
                switch (types.get(0).getType()) {
                    case STRING: {
                        return DECODE_NULLABLE_STRING;
                    }
                    case INT: {
                        return DECODE_NULLABLE_INT;
                    }
                    case LONG: {
                        return DECODE_NULLABLE_LONG;
                    }
                    case FLOAT: {
                        return DECODE_NULLABLE_FLOAT;
                    }
                    case DOUBLE: {
                        return DECODE_NULLABLE_DOUBLE;
                    }
                    case BOOLEAN: {
                        return DECODE_NULLABLE_BOOLEAN;
                    }
                    default:
                        throw new RuntimeException("Not impl");
                }
            }
            case STRING: {
                return DECODE_STRING;
            }
            case INT: {
                return DECODE_INT;
            }
            case LONG: {
                return DECODE_LONG;
            }
            case FLOAT: {
                return DECODE_FLOAT;
            }
            case DOUBLE: {
                return DECODE_DOUBLE;
            }
            case BOOLEAN: {
                return DECODE_BOOLEAN;
            }
            default:
                throw new RuntimeException("Not impl");
        }
    }
}
