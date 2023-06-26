package io.deephaven.protobuf;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import io.deephaven.protobuf.Protobuf.Unmapped;
import io.deephaven.protobuf.test.ADuration;
import io.deephaven.protobuf.test.AMultiNested;
import io.deephaven.protobuf.test.AMultiNested.SubMessage1;
import io.deephaven.protobuf.test.AMultiNested.SubMessage1.SubMessage2;
import io.deephaven.protobuf.test.ANested;
import io.deephaven.protobuf.test.ANested.SubMessage;
import io.deephaven.protobuf.test.AStringStringMap;
import io.deephaven.protobuf.test.ATimestamp;
import io.deephaven.protobuf.test.AnEnum;
import io.deephaven.protobuf.test.AnEnum.TheEnum;
import io.deephaven.protobuf.test.NestedRepeatedTimestamps;
import io.deephaven.protobuf.test.RepeatedBasics;
import io.deephaven.protobuf.test.RepeatedDuration;
import io.deephaven.protobuf.test.RepeatedMessage;
import io.deephaven.protobuf.test.RepeatedMessage.Person;
import io.deephaven.protobuf.test.RepeatedTimestamp;
import io.deephaven.protobuf.test.RepeatedWrappers;
import io.deephaven.protobuf.test.TheWrappers;
import io.deephaven.protobuf.test.UnionType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.ApplyVisitor;
import io.deephaven.stream.blink.tf.TypedFunction;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.assertj.core.api.Assertions.assertThat;

public class ProtobufTest {

    @Test
    public void string() {
        checkKey(StringValue.getDescriptor(), List.of(), Type.stringType(), Map.of(
                StringValue.of("foo"), "foo",
                StringValue.of("bar"), "bar"));
    }

    @Test
    public void int32() {
        checkKey(Int32Value.getDescriptor(), List.of(), Type.intType(), Map.of(
                Int32Value.of(42), 42,
                Int32Value.of(43), 43));
    }

    @Test
    public void uint32() {
        checkKey(UInt32Value.getDescriptor(), List.of(), Type.intType(), Map.of(
                UInt32Value.of(42), 42,
                UInt32Value.of(43), 43));
    }

    @Test
    public void int64() {
        checkKey(Int64Value.getDescriptor(), List.of(), Type.longType(), Map.of(
                Int64Value.of(42), 42L,
                Int64Value.of(43), 43L));
    }

    @Test
    public void uint64() {
        checkKey(UInt64Value.getDescriptor(), List.of(), Type.longType(), Map.of(
                UInt64Value.of(42), 42L,
                UInt64Value.of(43), 43L));
    }

    @Test
    public void float_() {
        checkKey(FloatValue.getDescriptor(), List.of(), Type.floatType(), Map.of(
                FloatValue.of(42), 42.0f,
                FloatValue.of(43), 43.0f));
    }

    @Test
    public void double_() {
        checkKey(DoubleValue.getDescriptor(), List.of(), Type.doubleType(), Map.of(
                DoubleValue.of(42), 42.0d,
                DoubleValue.of(43), 43.0d));
    }

    @Test
    public void bool() {
        checkKey(BoolValue.getDescriptor(), List.of(), Type.booleanType(), Map.of(
                BoolValue.of(true), true,
                BoolValue.of(false), false));
    }

    @Test
    public void bytes() {
        final ByteString foo = ByteString.copyFromUtf8("foo");
        final ByteString bar = ByteString.copyFromUtf8("bar");
        checkKey(BytesValue.getDescriptor(), List.of(), Type.byteType().arrayType(), Map.of(
                BytesValue.of(foo), "foo".getBytes(StandardCharsets.UTF_8),
                BytesValue.of(bar), "bar".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void timestamp() {
        final Map<List<String>, TypedFunction<Message>> nf = nf(Timestamp.getDescriptor());
        assertThat(nf.keySet()).containsExactly(List.of());
        checkKey(
                Timestamp.getDescriptor(),
                List.of(),
                Type.instantType(),
                Map.of(
                        Timestamp.getDefaultInstance(),
                        Instant.ofEpochSecond(0),
                        Timestamp.newBuilder().setSeconds(1).setNanos(2).build(),
                        Instant.ofEpochSecond(1, 2)));
    }

    @Test
    void unionTypes() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        assertThat(nf.keySet()).containsExactly(
                List.of("bool"),
                List.of("int32"),
                List.of("uint32"),
                List.of("int64"),
                List.of("uint64"),
                List.of("float"),
                List.of("double"),
                List.of("string"),
                List.of("bytes"));
    }

    @Test
    void oneOfBool() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setBool(true).build();
        assertThat(nf.get(List.of("bool")).walk(new ApplyVisitor<>(message))).isEqualTo(true);
        assertThat(nf.get(List.of("int32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("uint32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("int64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("uint64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("float")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_FLOAT);
        assertThat(nf.get(List.of("double")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_DOUBLE);
        assertThat(nf.get(List.of("string")).walk(new ApplyVisitor<>(message))).isEqualTo(null);
        assertThat(nf.get(List.of("bytes")).walk(new ApplyVisitor<>(message))).isEqualTo(null);

    }

    @Test
    void oneOfInt32() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setInt32(42).build();
        assertThat(nf.get(List.of("bool")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_BOOLEAN);
        assertThat(nf.get(List.of("int32")).walk(new ApplyVisitor<>(message))).isEqualTo(42);
        assertThat(nf.get(List.of("uint32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("int64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("uint64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("float")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_FLOAT);
        assertThat(nf.get(List.of("double")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_DOUBLE);
        assertThat(nf.get(List.of("string")).walk(new ApplyVisitor<>(message))).isEqualTo(null);
        assertThat(nf.get(List.of("bytes")).walk(new ApplyVisitor<>(message))).isEqualTo(null);
    }

    @Test
    void oneOfUInt32() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setUint32(42).build();
        assertThat(nf.get(List.of("bool")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_BOOLEAN);
        assertThat(nf.get(List.of("int32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("uint32")).walk(new ApplyVisitor<>(message))).isEqualTo(42);
        assertThat(nf.get(List.of("int64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("uint64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("float")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_FLOAT);
        assertThat(nf.get(List.of("double")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_DOUBLE);
        assertThat(nf.get(List.of("string")).walk(new ApplyVisitor<>(message))).isEqualTo(null);
        assertThat(nf.get(List.of("bytes")).walk(new ApplyVisitor<>(message))).isEqualTo(null);
    }

    @Test
    void oneOfInt64() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setInt64(42).build();
        assertThat(nf.get(List.of("bool")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_BOOLEAN);
        assertThat(nf.get(List.of("int32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("uint32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("int64")).walk(new ApplyVisitor<>(message))).isEqualTo(42L);
        assertThat(nf.get(List.of("uint64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("float")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_FLOAT);
        assertThat(nf.get(List.of("double")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_DOUBLE);
        assertThat(nf.get(List.of("string")).walk(new ApplyVisitor<>(message))).isEqualTo(null);
        assertThat(nf.get(List.of("bytes")).walk(new ApplyVisitor<>(message))).isEqualTo(null);
    }

    @Test
    void oneOfUInt64() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setUint64(42).build();
        assertThat(nf.get(List.of("bool")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_BOOLEAN);
        assertThat(nf.get(List.of("int32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("uint32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("int64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("uint64")).walk(new ApplyVisitor<>(message))).isEqualTo(42L);
        assertThat(nf.get(List.of("float")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_FLOAT);
        assertThat(nf.get(List.of("double")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_DOUBLE);
        assertThat(nf.get(List.of("string")).walk(new ApplyVisitor<>(message))).isEqualTo(null);
        assertThat(nf.get(List.of("bytes")).walk(new ApplyVisitor<>(message))).isEqualTo(null);
    }

    @Test
    void oneOfFloat() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setFloat(42.0f).build();
        assertThat(nf.get(List.of("bool")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_BOOLEAN);
        assertThat(nf.get(List.of("int32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("uint32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("int64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("uint64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("float")).walk(new ApplyVisitor<>(message))).isEqualTo(42.0f);
        assertThat(nf.get(List.of("double")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_DOUBLE);
        assertThat(nf.get(List.of("string")).walk(new ApplyVisitor<>(message))).isEqualTo(null);
        assertThat(nf.get(List.of("bytes")).walk(new ApplyVisitor<>(message))).isEqualTo(null);
    }

    @Test
    void oneOfDouble() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setDouble(42.0d).build();
        assertThat(nf.get(List.of("bool")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_BOOLEAN);
        assertThat(nf.get(List.of("int32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("uint32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("int64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("uint64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("float")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_FLOAT);
        assertThat(nf.get(List.of("double")).walk(new ApplyVisitor<>(message))).isEqualTo(42.0d);
        assertThat(nf.get(List.of("string")).walk(new ApplyVisitor<>(message))).isEqualTo(null);
        assertThat(nf.get(List.of("bytes")).walk(new ApplyVisitor<>(message))).isEqualTo(null);
    }

    @Test
    void oneOfString() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setString("hello").build();
        assertThat(nf.get(List.of("bool")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_BOOLEAN);
        assertThat(nf.get(List.of("int32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("uint32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("int64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("uint64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("float")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_FLOAT);
        assertThat(nf.get(List.of("double")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_DOUBLE);
        assertThat(nf.get(List.of("string")).walk(new ApplyVisitor<>(message))).isEqualTo("hello");
        assertThat(nf.get(List.of("bytes")).walk(new ApplyVisitor<>(message))).isEqualTo(null);
    }

    @Test
    void oneOfBytes() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setBytes(ByteString.copyFromUtf8("world")).build();
        assertThat(nf.get(List.of("bool")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_BOOLEAN);
        assertThat(nf.get(List.of("int32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("uint32")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_INT);
        assertThat(nf.get(List.of("int64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("uint64")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(nf.get(List.of("float")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_FLOAT);
        assertThat(nf.get(List.of("double")).walk(new ApplyVisitor<>(message))).isEqualTo(QueryConstants.NULL_DOUBLE);
        assertThat(nf.get(List.of("string")).walk(new ApplyVisitor<>(message))).isEqualTo(null);
        assertThat(nf.get(List.of("bytes")).walk(new ApplyVisitor<>(message)))
                .isEqualTo("world".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void stringStringMap() {
        checkKey(AStringStringMap.getDescriptor(), List.of("properties"), Type.ofCustom(Map.class),
                Map.of(AStringStringMap.newBuilder()
                        .putProperties("foo", "bar")
                        .putProperties("hello", "world").build(),
                        Map.of("foo", "bar", "hello", "world")));
    }

    @Test
    void aTimestamp() {
        checkKey(ATimestamp.getDescriptor(), List.of("ts"), Type.instantType(), Map.of(
                ATimestamp.newBuilder().setTs(Timestamp.newBuilder().setSeconds(42).setNanos(43).build())
                        .build(),
                Instant.ofEpochSecond(42, 43)));
    }

    @Test
    void aDuration() {
        checkKey(ADuration.getDescriptor(), List.of("dur"), Type.ofCustom(Duration.class), Map.of(
                ADuration.newBuilder()
                        .setDur(com.google.protobuf.Duration.newBuilder().setSeconds(4200).setNanos(4300)
                                .build())
                        .build(),
                Duration.ofSeconds(4200, 4300)));
    }

    @Test
    void enum_() {
        checkKey(AnEnum.getDescriptor(), List.of("fbb"), Type.ofCustom(EnumValueDescriptor.class), Map.of(
                AnEnum.newBuilder().setFbb(TheEnum.FOO).build(), TheEnum.FOO.getValueDescriptor(),
                AnEnum.newBuilder().setFbb(TheEnum.BAR).build(), TheEnum.BAR.getValueDescriptor(),
                AnEnum.newBuilder().setFbb(TheEnum.BAZ).build(), TheEnum.BAZ.getValueDescriptor(),
                AnEnum.newBuilder().setFbbValue(999).build(),
                TheEnum.getDescriptor().findValueByNumberCreatingIfUnknown(999)));
    }

    @Test
    void wrappers() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(TheWrappers.getDescriptor());
        assertThat(nf.keySet()).containsExactly(
                List.of("bool"),
                List.of("int32"),
                List.of("uint32"),
                List.of("int64"),
                List.of("uint64"),
                List.of("float"),
                List.of("double"),
                List.of("string"),
                List.of("bytes"));

        TheWrappers allNull = TheWrappers.getDefaultInstance();
        checkKey(TheWrappers.getDescriptor(), List.of("bool"), Type.booleanType(), new HashMap<Message, Boolean>() {
            {
                put(allNull, null);
                put(TheWrappers.newBuilder().setBool(BoolValue.newBuilder().setValue(true).build()).build(), true);
            }
        });

        checkKey(TheWrappers.getDescriptor(), List.of("int32"), Type.intType(), Map.of(
                allNull, QueryConstants.NULL_INT,
                TheWrappers.newBuilder().setInt32(Int32Value.newBuilder().setValue(42).build()).build(), 42));

        checkKey(TheWrappers.getDescriptor(), List.of("uint32"), Type.intType(), Map.of(
                allNull, QueryConstants.NULL_INT,
                TheWrappers.newBuilder().setUint32(UInt32Value.newBuilder().setValue(42).build()).build(), 42));

        checkKey(TheWrappers.getDescriptor(), List.of("int64"), Type.longType(), Map.of(
                allNull, QueryConstants.NULL_LONG,
                TheWrappers.newBuilder().setInt64(Int64Value.newBuilder().setValue(42).build()).build(), 42L));

        checkKey(TheWrappers.getDescriptor(), List.of("uint64"), Type.longType(), Map.of(
                allNull, QueryConstants.NULL_LONG,
                TheWrappers.newBuilder().setUint64(UInt64Value.newBuilder().setValue(42).build()).build(),
                42L));

        checkKey(TheWrappers.getDescriptor(), List.of("float"), Type.floatType(), Map.of(
                allNull, QueryConstants.NULL_FLOAT,
                TheWrappers.newBuilder().setFloat(FloatValue.newBuilder().setValue(42).build()).build(),
                42.0f));

        checkKey(TheWrappers.getDescriptor(), List.of("double"), Type.doubleType(), Map.of(
                allNull, QueryConstants.NULL_DOUBLE,
                TheWrappers.newBuilder().setDouble(DoubleValue.newBuilder().setValue(42).build()).build(),
                42.0d));

        checkKey(TheWrappers.getDescriptor(), List.of("string"), Type.stringType(), new HashMap<Message, String>() {
            {
                put(allNull, null);
                put(TheWrappers.newBuilder().setString(StringValue.newBuilder().setValue("foo").build())
                        .build(), "foo");
            }
        });

        checkKey(TheWrappers.getDescriptor(), List.of("bytes"), Type.byteType().arrayType(),
                new HashMap<Message, byte[]>() {
                    {
                        put(allNull, null);
                        final ByteString foo = ByteString.copyFromUtf8("foo");
                        put(TheWrappers.newBuilder().setBytes(BytesValue.newBuilder().setValue(foo).build()).build(),
                                "foo".getBytes(StandardCharsets.UTF_8));
                    }
                });
    }

    @Test
    void repeated() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(RepeatedBasics.getDescriptor());
        assertThat(nf.keySet()).containsExactly(
                List.of("bool"),
                List.of("int32"),
                List.of("uint32"),
                List.of("int64"),
                List.of("uint64"),
                List.of("float"),
                List.of("double"),
                List.of("string"),
                List.of("bytes"));

        final RepeatedBasics allEmpty = RepeatedBasics.getDefaultInstance();

        checkKey(RepeatedBasics.getDescriptor(), List.of("bool"), Type.ofCustom(Unmapped.class).arrayType(), Map.of(
                allEmpty, new Unmapped[] {},
                RepeatedBasics.newBuilder().addBool(true).addBool(false).build(),
                new Unmapped[] {null, null}));

        checkKey(RepeatedBasics.getDescriptor(), List.of("int32"), Type.intType().arrayType(), Map.of(
                allEmpty, new int[] {},
                RepeatedBasics.newBuilder().addInt32(42).addInt32(43).build(), new int[] {42, 43}));

        checkKey(RepeatedBasics.getDescriptor(), List.of("uint32"), Type.intType().arrayType(), Map.of(
                allEmpty, new int[] {},
                RepeatedBasics.newBuilder().addUint32(42).addUint32(43).build(), new int[] {42, 43}));

        checkKey(RepeatedBasics.getDescriptor(), List.of("int64"), Type.longType().arrayType(), Map.of(
                allEmpty, new long[] {},
                RepeatedBasics.newBuilder().addInt64(42).addInt64(43).build(), new long[] {42, 43}));

        checkKey(RepeatedBasics.getDescriptor(), List.of("uint64"), Type.longType().arrayType(), Map.of(
                allEmpty, new long[] {},
                RepeatedBasics.newBuilder().addUint64(42).addUint64(43).build(), new long[] {42, 43}));

        checkKey(RepeatedBasics.getDescriptor(), List.of("float"), Type.floatType().arrayType(), Map.of(
                allEmpty, new float[] {},
                RepeatedBasics.newBuilder().addFloat(42).addFloat(43).build(), new float[] {42, 43}));

        checkKey(RepeatedBasics.getDescriptor(), List.of("double"), Type.doubleType().arrayType(), Map.of(
                allEmpty, new double[] {},
                RepeatedBasics.newBuilder().addDouble(42).addDouble(43).build(),
                new double[] {42, 43}));

        checkKey(RepeatedBasics.getDescriptor(), List.of("string"), Type.stringType().arrayType(), Map.of(
                allEmpty, new String[] {},
                RepeatedBasics.newBuilder().addString("foo").addString("bar").build(),
                new String[] {"foo", "bar"}));

        checkKey(RepeatedBasics.getDescriptor(), List.of("bytes"), Type.byteType().arrayType().arrayType(), Map.of(
                allEmpty, new byte[][] {},
                RepeatedBasics.newBuilder().addBytes(ByteString.copyFromUtf8("hello"))
                        .addBytes(ByteString.copyFromUtf8("foo")).build(),
                new byte[][] {"hello".getBytes(StandardCharsets.UTF_8), "foo".getBytes(StandardCharsets.UTF_8)}));
    }

    @Test
    void repeatedWrappers() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(RepeatedWrappers.getDescriptor());
        assertThat(nf.keySet()).containsExactly(
                List.of("bool"),
                List.of("int32"),
                List.of("uint32"),
                List.of("int64"),
                List.of("uint64"),
                List.of("float"),
                List.of("double"),
                List.of("string"),
                List.of("bytes"));

        final RepeatedWrappers allEmpty = RepeatedWrappers.getDefaultInstance();

        // checkKey(
        // RepeatedWrappers.getDescriptor(),
        // "bool",
        // BooleanVector.type(),
        // Map.of(
        // allEmpty, BooleanVector.empty(),
        // RepeatedWrappers.newBuilder().addBool(BoolValue.of(true)).addBool(BoolValue.of(false)).build(),
        // BooleanVector.proxy(new ObjectVectorDirect<>(true, false))));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("int32"), Type.intType().arrayType(), Map.of(
                allEmpty, new int[] {},
                RepeatedWrappers.newBuilder().addInt32(Int32Value.of(42)).addInt32(Int32Value.of(43)).build(),
                new int[] {42, 43}));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("uint32"), Type.intType().arrayType(), Map.of(
                allEmpty, new int[] {},
                RepeatedWrappers.newBuilder().addUint32(UInt32Value.of(42)).addUint32(UInt32Value.of(43))
                        .build(),
                new int[] {42, 43}));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("int64"), Type.longType().arrayType(), Map.of(
                allEmpty, new long[] {},
                RepeatedWrappers.newBuilder().addInt64(Int64Value.of(42)).addInt64(Int64Value.of(43)).build(),
                new long[] {42, 43}));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("uint64"), Type.longType().arrayType(), Map.of(
                allEmpty, new long[] {},
                RepeatedWrappers.newBuilder().addUint64(UInt64Value.of(42)).addUint64(UInt64Value.of(43))
                        .build(),
                new long[] {42, 43}));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("float"), Type.floatType().arrayType(), Map.of(
                allEmpty, new float[] {},
                RepeatedWrappers.newBuilder().addFloat(FloatValue.of(42)).addFloat(FloatValue.of(43)).build(),
                new float[] {42, 43}));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("double"), Type.doubleType().arrayType(), Map.of(
                allEmpty, new double[] {},
                RepeatedWrappers.newBuilder().addDouble(DoubleValue.of(42)).addDouble(DoubleValue.of(43))
                        .build(),
                new double[] {42, 43}));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("string"), Type.stringType().arrayType(), Map.of(
                allEmpty, new String[] {},
                RepeatedWrappers.newBuilder().addString(StringValue.of("foo")).addString(StringValue.of("bar")).build(),
                new String[] {"foo", "bar"}));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("bytes"), Type.byteType().arrayType().arrayType(), Map.of(
                allEmpty, new byte[][] {},
                RepeatedWrappers.newBuilder().addBytes(BytesValue.of(ByteString.copyFromUtf8("hello")))
                        .addBytes(BytesValue.of(ByteString.copyFromUtf8("foo"))).build(),
                new byte[][] {"hello".getBytes(StandardCharsets.UTF_8), "foo".getBytes(StandardCharsets.UTF_8)}));
    }

    @Test
    void repeatedTimestamp() {
        checkKey(
                RepeatedTimestamp.getDescriptor(),
                List.of("ts"),
                Type.instantType().arrayType(),
                Map.of(
                        RepeatedTimestamp.getDefaultInstance(), new Instant[] {},
                        RepeatedTimestamp.newBuilder()
                                .addTs(Timestamp.newBuilder().setSeconds(1).setNanos(2).build())
                                .addTs(Timestamp.newBuilder().setSeconds(3).setNanos(4).build())
                                .build(),
                        new Instant[] {
                                Instant.ofEpochSecond(1, 2),
                                Instant.ofEpochSecond(3, 4)}));
    }

    @Test
    void repeatedDuration() {
        checkKey(RepeatedDuration.getDescriptor(), List.of("dur"), Type.ofCustom(Duration.class).arrayType(), Map.of(
                RepeatedDuration.getDefaultInstance(), new Duration[] {},
                RepeatedDuration.newBuilder()
                        .addDur(com.google.protobuf.Duration.newBuilder().setSeconds(1).setNanos(2).build())
                        .addDur(com.google.protobuf.Duration.newBuilder().setSeconds(3).setNanos(4).build())
                        .build(),
                new Duration[] {
                        Duration.ofSeconds(1, 2),
                        Duration.ofSeconds(3, 4)}));
    }

    @Test
    void nested() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(ANested.getDescriptor());
        assertThat(nf.keySet()).containsExactly(List.of("baz", "foo"), List.of("baz", "bar"));

        checkKey(
                ANested.getDescriptor(),
                List.of("baz", "foo"),
                Type.intType(),
                Map.of(
                        ANested.getDefaultInstance(), QueryConstants.NULL_INT,
                        ANested.newBuilder().setBaz(SubMessage.newBuilder().setFoo(42).build()).build(), 42));

        checkKey(
                ANested.getDescriptor(),
                List.of("baz", "bar"),
                Type.longType(),
                Map.of(
                        ANested.getDefaultInstance(), QueryConstants.NULL_LONG,
                        ANested.newBuilder().setBaz(SubMessage.newBuilder().setBar(42L).build()).build(), 42L));
    }

    @Test
    void multiNested() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(AMultiNested.getDescriptor());
        assertThat(nf.keySet()).containsExactly(
                List.of("hello", "foo"),
                List.of("hello", "bar"),
                List.of("hello", "baz", "world"),
                List.of("hello", "baz", "world2"),
                List.of("hello", "baz", "world3"));

        final AMultiNested defaultInstance = AMultiNested.getDefaultInstance();
        final AMultiNested noBaz = AMultiNested.newBuilder()
                .setHello(SubMessage1.newBuilder().setFoo(42).setBar(43).build())
                .build();
        final AMultiNested bazDefault = AMultiNested.newBuilder()
                .setHello(SubMessage1.newBuilder().setBaz(SubMessage2.getDefaultInstance()).build())
                .build();
        final AMultiNested bazWorld = AMultiNested.newBuilder()
                .setHello(SubMessage1.newBuilder().setBaz(SubMessage2.newBuilder()
                        .setWorld("OK")
                        .setWorld2(StringValue.newBuilder().setValue("OK2"))
                        .setWorld3(DoubleValue.newBuilder().setValue(42.0d)).build())
                        .build())
                .build();

        checkKey(
                AMultiNested.getDescriptor(),
                List.of("hello", "foo"),
                Type.intType(),
                Map.of(
                        defaultInstance, QueryConstants.NULL_INT,
                        noBaz, 42,
                        bazDefault, 0,
                        bazWorld, 0));

        checkKey(
                AMultiNested.getDescriptor(),
                List.of("hello", "bar"),
                Type.longType(),
                Map.of(
                        defaultInstance, QueryConstants.NULL_LONG,
                        noBaz, 43L,
                        bazDefault, 0L,
                        bazWorld, 0L));

        checkKey(
                AMultiNested.getDescriptor(),
                List.of("hello", "baz", "world"),
                Type.stringType(),
                new HashMap<>() {
                    {
                        put(defaultInstance, null);
                        put(noBaz, null);
                        put(bazDefault, "");
                        put(bazWorld, "OK");
                    }
                });

        checkKey(
                AMultiNested.getDescriptor(),
                List.of("hello", "baz", "world2"),
                Type.stringType(),
                new HashMap<>() {
                    {
                        put(defaultInstance, null);
                        put(noBaz, null);
                        put(bazDefault, null);
                        put(bazWorld, "OK2");
                    }
                });

        checkKey(
                AMultiNested.getDescriptor(),
                List.of("hello", "baz", "world3"),
                Type.doubleType(),
                Map.of(
                        defaultInstance, QueryConstants.NULL_DOUBLE,
                        noBaz, QueryConstants.NULL_DOUBLE,
                        bazDefault, QueryConstants.NULL_DOUBLE,
                        bazWorld, 42.0d));
    }

    @Test
    void repeatedMessage() {
        final Map<List<String>, TypedFunction<Message>> nf = nf(
                RepeatedMessage.getDescriptor());
        assertThat(nf.keySet()).containsExactly(List.of("persons"));

        final Person p1 = Person.newBuilder().setFirstName("First").setLastName("Last").build();
        final Person p2 = Person.newBuilder().setFirstName("Foo").setLastName("Bar").build();
        checkKey(
                RepeatedMessage.getDescriptor(),
                List.of("persons"),
                Type.ofCustom(Message.class).arrayType(),
                Map.of(
                        RepeatedMessage.getDefaultInstance(), new Message[] {},
                        RepeatedMessage.newBuilder().addPersons(p1).addPersons(p2).build(), new Message[] {p1, p2}));
    }

    // This is a potential improvement in parsing we might want in the future
    @Disabled
    @Test
    void repeatedMessageDestructured() {
        final Map<List<String>, TypedFunction<Message>> nf = nf(
                RepeatedMessage.getDescriptor());
        assertThat(nf.keySet()).containsExactly(List.of("persons", "first_name"), List.of("persons", "last_name"));

        final Person p1 = Person.newBuilder().setFirstName("First").setLastName("Last").build();
        final Person p2 = Person.newBuilder().setFirstName("Foo").setLastName("Bar").build();

        checkKey(
                RepeatedMessage.getDescriptor(),
                List.of("persons", "first_name"),
                Type.stringType().arrayType(),
                Map.of(
                        RepeatedMessage.getDefaultInstance(), new String[] {},
                        RepeatedMessage.newBuilder().addPersons(p1).addPersons(p2).build(),
                        new String[] {"First", "Foo"}));

        checkKey(
                RepeatedMessage.getDescriptor(),
                List.of("persons", "last_name"),
                Type.stringType().arrayType(),
                Map.of(
                        RepeatedMessage.getDefaultInstance(), new String[] {},
                        RepeatedMessage.newBuilder().addPersons(p1).addPersons(p2).build(),
                        new String[] {"Last", "Bar"}));
    }

    // potential
    @Disabled
    @Test
    void nestedRepeatedTimestamps() {
        final Map<List<String>, TypedFunction<Message>> nf = nf(NestedRepeatedTimestamps.getDescriptor());
        assertThat(nf.keySet()).containsExactly(List.of("stamps", "ts"));
        checkKey(
                NestedRepeatedTimestamps.getDescriptor(),
                List.of("stamps", "ts"),
                Type.instantType().arrayType().arrayType(),
                Map.of());

    }

    @Test
    void dynamicMessage() throws InvalidProtocolBufferException, DescriptorValidationException {

        String x = "message MyDynamic {\n" +
                "  int32 foo = 1;\n" +
                "  string bar = 2;\n" +
                "}";

        final FileDescriptor fileDescriptor = FileDescriptor
                .buildFrom(FileDescriptorProto.parseFrom(x.getBytes(StandardCharsets.UTF_8)), new FileDescriptor[] {});

        int t = 0;
    }

    private static Map<List<String>, TypedFunction<Message>> nf(Descriptor descriptor) {
        return Protobuf.parser(descriptor).parser(ProtobufOptions.defaults());
    }

    private static <T> void checkKey(
            Descriptor descriptor,
            List<String> expectedPath,
            Type<T> expectedType,
            Map<Message, T> expectedExamples) {
        final Map<List<String>, TypedFunction<Message>> map = nf(descriptor);
        assertThat(map)
                .extractingByKey(expectedPath)
                .extracting(TypedFunction::returnType)
                .isEqualTo(expectedType);
        for (Entry<Message, T> e : expectedExamples.entrySet()) {
            assertThat(map)
                    .extractingByKey(expectedPath)
                    .extracting(t -> t.walk(new ApplyVisitor<>(e.getKey())))
                    .isEqualTo(e.getValue());
        }
    }
}
