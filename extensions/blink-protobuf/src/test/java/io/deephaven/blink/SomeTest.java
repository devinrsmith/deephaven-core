package io.deephaven.blink;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import io.deephaven.blink.protobuf.test.ADuration;
import io.deephaven.blink.protobuf.test.AStringStringMap;
import io.deephaven.blink.protobuf.test.ATimestamp;
import io.deephaven.blink.protobuf.test.AnEnum;
import io.deephaven.blink.protobuf.test.AnEnum.TheEnum;
import io.deephaven.blink.protobuf.test.TheWrappers;
import io.deephaven.blink.protobuf.test.UnionType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.ApplyVisitor;
import io.deephaven.stream.blink.tf.TypedFunction;
import io.deephaven.util.QueryConstants;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.ByteVectorDirect;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static org.assertj.core.api.Assertions.assertThat;

public class SomeTest {

    @Test
    public void string() {
        checkKey(
                StringValue.getDescriptor(),
                "value",
                Type.stringType(),
                Map.of(
                        StringValue.of("foo"), "foo",
                        StringValue.of("bar"), "bar"));
    }

    @Test
    public void int32() {
        checkKey(
                Int32Value.getDescriptor(),
                "value",
                Type.intType(),
                Map.of(
                        Int32Value.of(42), 42,
                        Int32Value.of(43), 43));
    }

    @Test
    public void uint32() {
        checkKey(
                UInt32Value.getDescriptor(),
                "value",
                Type.intType(),
                Map.of(
                        UInt32Value.of(42), 42,
                        UInt32Value.of(43), 43));
    }

    @Test
    public void int64() {
        checkKey(
                Int64Value.getDescriptor(),
                "value",
                Type.longType(),
                Map.of(
                        Int64Value.of(42), 42L,
                        Int64Value.of(43), 43L));
    }

    @Test
    public void uint64() {
        checkKey(
                UInt64Value.getDescriptor(),
                "value",
                Type.longType(),
                Map.of(
                        UInt64Value.of(42), 42L,
                        UInt64Value.of(43), 43L));
    }

    @Test
    public void float_() {
        checkKey(
                FloatValue.getDescriptor(),
                "value",
                Type.floatType(),
                Map.of(
                        FloatValue.of(42), 42.0f,
                        FloatValue.of(43), 43.0f));
    }

    @Test
    public void double_() {
        checkKey(
                DoubleValue.getDescriptor(),
                "value",
                Type.doubleType(),
                Map.of(
                        DoubleValue.of(42), 42.0d,
                        DoubleValue.of(43), 43.0d));
    }

    @Test
    public void bool() {
        checkKey(
                BoolValue.getDescriptor(),
                "value",
                Type.booleanType(),
                Map.of(
                        BoolValue.of(true), true,
                        BoolValue.of(false), false));
    }

    @Test
    public void bytes() {
        checkKey(
                BytesValue.getDescriptor(),
                "value",
                ByteVector.type(),
                Map.of(
                        BytesValue.of(ByteString.copyFromUtf8("foo")),
                        new ByteVectorDirect("foo".getBytes(StandardCharsets.UTF_8)),
                        BytesValue.of(ByteString.copyFromUtf8("bar")),
                        new ByteVectorDirect("bar".getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void oneOfBool() {
        final Map<String, TypedFunction<Message>> nf = Protobuf.namedFunctions(UnionType.getDescriptor());
        assertThat(nf.keySet()).containsExactly("bool", "int32", "uint32", "int64", "uint64", "float", "double",
                "string", "bytes");
        final UnionType message = UnionType.newBuilder().setBool(true).build();
        assertThat(nf.get("bool").walk(new ApplyVisitor<>(message))).isEqualTo(true);
        assertThat(nf.get("int32").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get("uint32").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get("int64").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get("uint64").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get("float").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_FLOAT);
        assertThat(nf.get("double").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_DOUBLE);
    }

    @Test
    void oneOfInt32() {
        final Map<String, TypedFunction<Message>> nf = Protobuf.namedFunctions(UnionType.getDescriptor());
        assertThat(nf.keySet()).containsExactly("bool", "int32", "uint32", "int64", "uint64", "float", "double",
                "string", "bytes");
        final UnionType message = UnionType.newBuilder().setInt32(42).build();
        assertThat(nf.get("bool").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_BOOLEAN);
        assertThat(nf.get("int32").walk(new ApplyVisitor<>(message))).isEqualTo(42);
        assertThat(nf.get("uint32").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get("int64").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get("uint64").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get("float").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_FLOAT);
        assertThat(nf.get("double").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_DOUBLE);
    }

    @Test
    void oneOfUInt32() {
        final Map<String, TypedFunction<Message>> nf = Protobuf.namedFunctions(UnionType.getDescriptor());
        assertThat(nf.keySet()).containsExactly("bool", "int32", "uint32", "int64", "uint64", "float", "double",
                "string", "bytes");
        final UnionType message = UnionType.newBuilder().setUint32(42).build();
        assertThat(nf.get("bool").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_BOOLEAN);
        assertThat(nf.get("int32").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get("uint32").walk(new ApplyVisitor<>(message))).isEqualTo(42);
        assertThat(nf.get("int64").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get("uint64").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get("float").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_FLOAT);
        assertThat(nf.get("double").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_DOUBLE);
    }

    @Test
    void oneOfInt64() {
        final Map<String, TypedFunction<Message>> nf = Protobuf.namedFunctions(UnionType.getDescriptor());
        assertThat(nf.keySet()).containsExactly("bool", "int32", "uint32", "int64", "uint64", "float", "double",
                "string", "bytes");
        final UnionType message = UnionType.newBuilder().setInt64(42).build();
        assertThat(nf.get("bool").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_BOOLEAN);
        assertThat(nf.get("int32").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get("uint32").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get("int64").walk(new ApplyVisitor<>(message))).isEqualTo(42L);
        assertThat(nf.get("uint64").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get("float").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_FLOAT);
        assertThat(nf.get("double").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_DOUBLE);
    }

    @Test
    void oneOfUInt64() {
        final Map<String, TypedFunction<Message>> nf = Protobuf.namedFunctions(UnionType.getDescriptor());
        assertThat(nf.keySet()).containsExactly("bool", "int32", "uint32", "int64", "uint64", "float", "double",
                "string", "bytes");
        final UnionType message = UnionType.newBuilder().setUint64(42).build();
        assertThat(nf.get("bool").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_BOOLEAN);
        assertThat(nf.get("int32").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get("uint32").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get("int64").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get("uint64").walk(new ApplyVisitor<>(message))).isEqualTo(42L);
        assertThat(nf.get("float").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_FLOAT);
        assertThat(nf.get("double").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_DOUBLE);
    }

    @Test
    void oneOfFloat() {
        final Map<String, TypedFunction<Message>> nf = Protobuf.namedFunctions(UnionType.getDescriptor());
        assertThat(nf.keySet()).containsExactly("bool", "int32", "uint32", "int64", "uint64", "float", "double",
                "string", "bytes");
        final UnionType message = UnionType.newBuilder().setFloat(42.0f).build();
        assertThat(nf.get("bool").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_BOOLEAN);
        assertThat(nf.get("int32").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get("uint32").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get("int64").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get("uint64").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get("float").walk(new ApplyVisitor<>(message))).isEqualTo(42.0f);
        assertThat(nf.get("double").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_DOUBLE);
    }

    @Test
    void oneOfDouble() {
        final Map<String, TypedFunction<Message>> nf = Protobuf.namedFunctions(UnionType.getDescriptor());
        assertThat(nf.keySet()).containsExactly("bool", "int32", "uint32", "int64", "uint64", "float", "double",
                "string", "bytes");
        final UnionType message = UnionType.newBuilder().setDouble(42.0d).build();
        assertThat(nf.get("bool").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_BOOLEAN);
        assertThat(nf.get("int32").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get("uint32").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get("int64").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get("uint64").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get("float").walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_FLOAT);
        assertThat(nf.get("double").walk(new ApplyVisitor<>(message))).isEqualTo(42.0d);
    }

    @Test
    void stringStringMap() {
        checkKey(
                AStringStringMap.getDescriptor(),
                "properties",
                Type.ofCustom(Map.class),
                Map.of(AStringStringMap.newBuilder()
                        .putProperties("foo", "bar")
                        .putProperties("hello", "world").build(),
                        Map.of("foo", "bar", "hello", "world")));
    }

    @Test
    void timestamp() {
        checkKey(
                ATimestamp.getDescriptor(),
                "ts",
                Type.instantType(),
                Map.of(
                        ATimestamp.newBuilder().setTs(Timestamp.newBuilder().setSeconds(42).setNanos(43).build()).build(),
                        Instant.ofEpochSecond(42, 43)));
    }

    @Test
    void duration() {
        checkKey(
                ADuration.getDescriptor(),
                "dur",
                Type.ofCustom(Duration.class),
                Map.of(
                        ADuration.newBuilder().setDur(com.google.protobuf.Duration.newBuilder().setSeconds(4200).setNanos(4300).build()).build(),
                        Duration.ofSeconds(4200, 4300)));
    }

    @Test
    void enum_() {
        checkKey(
                AnEnum.getDescriptor(),
                "fbb",
                Type.ofCustom(EnumValueDescriptor.class),
                Map.of(
                        AnEnum.newBuilder().setFbb(TheEnum.FOO).build(), TheEnum.FOO.getValueDescriptor(),
                        AnEnum.newBuilder().setFbb(TheEnum.BAR).build(), TheEnum.BAR.getValueDescriptor(),
                        AnEnum.newBuilder().setFbb(TheEnum.BAZ).build(), TheEnum.BAZ.getValueDescriptor(),
                        AnEnum.newBuilder().setFbbValue(999).build(), AnEnum.TheEnum.getDescriptor().findValueByNumberCreatingIfUnknown(999)));
    }

    @Test
    void wrappers() {
        final Map<String, TypedFunction<Message>> nf = Protobuf.namedFunctions(TheWrappers.getDescriptor());
        assertThat(nf.keySet()).containsExactly("bool", "int32", "uint32", "int64", "uint64", "float", "double", "string", "bytes");

        TheWrappers allNull = TheWrappers.getDefaultInstance();
        checkKey(TheWrappers.getDescriptor(), "bool", Type.booleanType(), new HashMap<>() {{
            put(allNull, null);
            put(TheWrappers.newBuilder().setBool(BoolValue.newBuilder().build()).build(), true);
        }});

    }

    private static <T> void checkKey(
            Descriptor descriptor,
            String expectedName,
            Type<T> expectedType,
            Map<Message, T> expectedExamples) {
        final Map<String, TypedFunction<Message>> map = Protobuf.namedFunctions(descriptor);
        //assertThat(map).containsOnlyKeys(expectedName);
        assertThat(map)
                .extractingByKey(expectedName)
                .extracting(TypedFunction::returnType)
                .isEqualTo(expectedType);
        for (Entry<Message, T> e : expectedExamples.entrySet()) {
            assertThat(map)
                    .extractingByKey(expectedName)
                    .extracting(t -> t.walk(new ApplyVisitor<>(e.getKey())))
                    .isEqualTo(e.getValue());
        }
    }
}
