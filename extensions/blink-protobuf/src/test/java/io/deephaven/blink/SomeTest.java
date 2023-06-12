package io.deephaven.blink;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import io.deephaven.blink.protobuf.test.UnionType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.ApplyVisitor;
import io.deephaven.stream.blink.tf.TypedFunction;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.ByteVectorDirect;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Map.Entry;

import static org.assertj.core.api.Assertions.assertThat;

public class SomeTest {

    @Test
    public void string() {
        singular(
                StringValue.getDescriptor(),
                "value",
                Type.stringType(),
                Map.of(
                        StringValue.of("foo"), "foo",
                        StringValue.of("bar"), "bar"));
    }

    @Test
    public void int32() {
        singular(
                Int32Value.getDescriptor(),
                "value",
                Type.intType(),
                Map.of(
                        Int32Value.of(42), 42,
                        Int32Value.of(43), 43));
    }

    @Test
    public void uint32() {
        singular(
                UInt32Value.getDescriptor(),
                "value",
                Type.intType(),
                Map.of(
                        UInt32Value.of(42), 42,
                        UInt32Value.of(43), 43));
    }

    @Test
    public void int64() {
        singular(
                Int64Value.getDescriptor(),
                "value",
                Type.longType(),
                Map.of(
                        Int64Value.of(42), 42L,
                        Int64Value.of(43), 43L));
    }

    @Test
    public void uint64() {
        singular(
                UInt64Value.getDescriptor(),
                "value",
                Type.longType(),
                Map.of(
                        UInt64Value.of(42), 42L,
                        UInt64Value.of(43), 43L));
    }

    @Test
    public void float_() {
        singular(
                FloatValue.getDescriptor(),
                "value",
                Type.floatType(),
                Map.of(
                        FloatValue.of(42), 42.0f,
                        FloatValue.of(43), 43.0f));
    }

    @Test
    public void double_() {
        singular(
                DoubleValue.getDescriptor(),
                "value",
                Type.doubleType(),
                Map.of(
                        DoubleValue.of(42), 42.0d,
                        DoubleValue.of(43), 43.0d));
    }

    @Test
    public void bool() {
        singular(
                BoolValue.getDescriptor(),
                "value",
                Type.booleanType(),
                Map.of(
                        BoolValue.of(true), true,
                        BoolValue.of(false), false));
    }

    @Test
    public void bytes() {
        singular(
                BytesValue.getDescriptor(),
                "value",
                ByteVector.type(),
                Map.of(
                        BytesValue.of(ByteString.copyFromUtf8("foo")), new ByteVectorDirect("foo".getBytes(StandardCharsets.UTF_8)),
                        BytesValue.of(ByteString.copyFromUtf8("bar")), new ByteVectorDirect("bar".getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void name() {
        /*
        message UnionType {
  oneof type {
    bool bool = 1;
    int32 int32 = 2;
    uint32 uint32 = 3;
    int32 int64 = 4;
    uint32 uint64 = 5;
    float float = 6;
    double double = 7;
    string string = 8;
    bytes bytes = 9;
  }
}
         */
        final Map<String, TypedFunction<Message>> nf = Protobuf.namedFunctions(UnionType.getDescriptor());
        assertThat(nf.keySet()).containsExactly("bool", "int32", "uint32", "int64", "uint64", "float", "double", "string", "bytes");

        UnionType.newBuilder().setBool(true).build();
    }

    private static void singular(
            Descriptor descriptor,
            String expectedName,
            Type<?> expectedType,
            Map<Message, Object> expectedExamples) {
        final Map<String, TypedFunction<Message>> map = Protobuf.namedFunctions(descriptor);
        assertThat(map).containsOnlyKeys(expectedName);
        assertThat(map)
                .extractingByKey(expectedName)
                .extracting(TypedFunction::returnType)
                .isEqualTo(expectedType);
        for (Entry<Message, Object> e : expectedExamples.entrySet()) {
            assertThat(map)
                    .extractingByKey(expectedName)
                    .extracting(t -> t.walk(new ApplyVisitor<>(e.getKey())))
                    .isEqualTo(e.getValue());
        }
    }
}

/*
 * message RawEvent { string id = 1; string name = 2; int64 created_at = 3; int64 received_at = 4; map<string, string>
 * properties = 5; }
 */
