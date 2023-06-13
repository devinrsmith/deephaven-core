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
import io.deephaven.blink.protobuf.test.AMultiNested;
import io.deephaven.blink.protobuf.test.AMultiNested.SubMessage1;
import io.deephaven.blink.protobuf.test.AMultiNested.SubMessage1.SubMessage2;
import io.deephaven.blink.protobuf.test.ANested;
import io.deephaven.blink.protobuf.test.ANested.SubMessage;
import io.deephaven.blink.protobuf.test.AStringStringMap;
import io.deephaven.blink.protobuf.test.ATimestamp;
import io.deephaven.blink.protobuf.test.AnEnum;
import io.deephaven.blink.protobuf.test.AnEnum.TheEnum;
import io.deephaven.blink.protobuf.test.RepeatedBasics;
import io.deephaven.blink.protobuf.test.RepeatedDuration;
import io.deephaven.blink.protobuf.test.RepeatedTimestamp;
import io.deephaven.blink.protobuf.test.RepeatedWrappers;
import io.deephaven.blink.protobuf.test.TheWrappers;
import io.deephaven.blink.protobuf.test.UnionType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.ApplyVisitor;
import io.deephaven.stream.blink.tf.TypedFunction;
import io.deephaven.util.QueryConstants;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.ByteVectorDirect;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.DoubleVectorDirect;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.FloatVectorDirect;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.IntVectorDirect;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.LongVectorDirect;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
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
        final ByteString foo = ByteString.copyFromUtf8("foo");
        final ByteString bar = ByteString.copyFromUtf8("bar");
        checkKey(
                BytesValue.getDescriptor(),
                "value",
                ByteVector.type(),
                Map.of(
                        BytesValue.of(foo), new ByteVectorByteStringWrapper(foo),
                        BytesValue.of(bar), new ByteVectorByteStringWrapper(bar)));
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
                        ATimestamp.newBuilder().setTs(Timestamp.newBuilder().setSeconds(42).setNanos(43).build())
                                .build(),
                        Instant.ofEpochSecond(42, 43)));
    }

    @Test
    void duration() {
        checkKey(
                ADuration.getDescriptor(),
                "dur",
                Type.ofCustom(Duration.class),
                Map.of(
                        ADuration.newBuilder()
                                .setDur(com.google.protobuf.Duration.newBuilder().setSeconds(4200).setNanos(4300)
                                        .build())
                                .build(),
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
                        AnEnum.newBuilder().setFbbValue(999).build(),
                        AnEnum.TheEnum.getDescriptor().findValueByNumberCreatingIfUnknown(999)));
    }

    @Test
    void wrappers() {
        final Map<String, TypedFunction<Message>> nf = Protobuf.namedFunctions(TheWrappers.getDescriptor());
        assertThat(nf.keySet()).containsExactly("bool", "int32", "uint32", "int64", "uint64", "float", "double",
                "string", "bytes");

        TheWrappers allNull = TheWrappers.getDefaultInstance();
        checkKey(TheWrappers.getDescriptor(), "bool", Type.booleanType(), new HashMap<>() {
            {
                put(allNull, null);
                put(TheWrappers.newBuilder().setBool(BoolValue.newBuilder().setValue(true).build()).build(), true);
            }
        });

        checkKey(
                TheWrappers.getDescriptor(),
                "int32",
                Type.intType(),
                Map.of(
                        allNull, QueryConstants.NULL_INT,
                        TheWrappers.newBuilder().setInt32(Int32Value.newBuilder().setValue(42).build()).build(), 42));

        checkKey(
                TheWrappers.getDescriptor(),
                "uint32",
                Type.intType(),
                Map.of(
                        allNull, QueryConstants.NULL_INT,
                        TheWrappers.newBuilder().setUint32(UInt32Value.newBuilder().setValue(42).build()).build(), 42));

        checkKey(
                TheWrappers.getDescriptor(),
                "int64",
                Type.longType(),
                Map.of(
                        allNull, QueryConstants.NULL_LONG,
                        TheWrappers.newBuilder().setInt64(Int64Value.newBuilder().setValue(42).build()).build(), 42L));

        checkKey(
                TheWrappers.getDescriptor(),
                "uint64",
                Type.longType(),
                Map.of(
                        allNull, QueryConstants.NULL_LONG,
                        TheWrappers.newBuilder().setUint64(UInt64Value.newBuilder().setValue(42).build()).build(),
                        42L));

        checkKey(
                TheWrappers.getDescriptor(),
                "float",
                Type.floatType(),
                Map.of(
                        allNull, QueryConstants.NULL_FLOAT,
                        TheWrappers.newBuilder().setFloat(FloatValue.newBuilder().setValue(42).build()).build(),
                        42.0f));

        checkKey(
                TheWrappers.getDescriptor(),
                "double",
                Type.doubleType(),
                Map.of(
                        allNull, QueryConstants.NULL_DOUBLE,
                        TheWrappers.newBuilder().setDouble(DoubleValue.newBuilder().setValue(42).build()).build(),
                        42.0d));

        checkKey(
                TheWrappers.getDescriptor(),
                "string",
                Type.stringType(),
                new HashMap<>() {
                    {
                        put(allNull, null);
                        put(TheWrappers.newBuilder().setString(StringValue.newBuilder().setValue("foo").build())
                                .build(), "foo");
                    }
                });

        checkKey(
                TheWrappers.getDescriptor(),
                "bytes",
                ByteVector.type(),
                new HashMap<>() {
                    {
                        put(allNull, null);
                        final ByteString foo = ByteString.copyFromUtf8("foo");
                        put(TheWrappers.newBuilder().setBytes(BytesValue.newBuilder().setValue(foo).build()).build(),
                                new ByteVectorByteStringWrapper(foo));
                    }
                });
    }

    @Test
    void repeated() {
        final Map<String, TypedFunction<Message>> nf = Protobuf.namedFunctions(RepeatedBasics.getDescriptor());
        assertThat(nf.keySet()).containsExactly("int32", "uint32", "int64", "uint64", "float", "double", "string",
                "bytes");

        final RepeatedBasics allEmpty = RepeatedBasics.getDefaultInstance();

        checkKey(
                RepeatedBasics.getDescriptor(),
                "int32",
                IntVector.type(),
                Map.of(
                        allEmpty, IntVectorDirect.ZERO_LENGTH_VECTOR,
                        RepeatedBasics.newBuilder().addInt32(42).addInt32(43).build(), new IntVectorDirect(42, 43)));

        checkKey(
                RepeatedBasics.getDescriptor(),
                "uint32",
                IntVector.type(),
                Map.of(
                        allEmpty, IntVectorDirect.ZERO_LENGTH_VECTOR,
                        RepeatedBasics.newBuilder().addUint32(42).addUint32(43).build(), new IntVectorDirect(42, 43)));

        checkKey(
                RepeatedBasics.getDescriptor(),
                "int64",
                LongVector.type(),
                Map.of(
                        allEmpty, LongVectorDirect.ZERO_LENGTH_VECTOR,
                        RepeatedBasics.newBuilder().addInt64(42).addInt64(43).build(), new LongVectorDirect(42, 43)));

        checkKey(
                RepeatedBasics.getDescriptor(),
                "uint64",
                LongVector.type(),
                Map.of(
                        allEmpty, LongVectorDirect.ZERO_LENGTH_VECTOR,
                        RepeatedBasics.newBuilder().addUint64(42).addUint64(43).build(), new LongVectorDirect(42, 43)));

        checkKey(
                RepeatedBasics.getDescriptor(),
                "float",
                FloatVector.type(),
                Map.of(
                        allEmpty, FloatVectorDirect.ZERO_LENGTH_VECTOR,
                        RepeatedBasics.newBuilder().addFloat(42).addFloat(43).build(), new FloatVectorDirect(42, 43)));

        checkKey(
                RepeatedBasics.getDescriptor(),
                "double",
                DoubleVector.type(),
                Map.of(
                        allEmpty, DoubleVectorDirect.ZERO_LENGTH_VECTOR,
                        RepeatedBasics.newBuilder().addDouble(42).addDouble(43).build(),
                        new DoubleVectorDirect(42, 43)));
    }

    @Test
    void repeatedWrappers() {
        final Map<String, TypedFunction<Message>> nf = Protobuf.namedFunctions(RepeatedWrappers.getDescriptor());
        assertThat(nf.keySet()).containsExactly("int32", "uint32", "int64", "uint64", "float", "double", "string",
                "bytes");

        final RepeatedWrappers allEmpty = RepeatedWrappers.getDefaultInstance();

        checkKey(
                RepeatedWrappers.getDescriptor(),
                "int32",
                IntVector.type(),
                Map.of(
                        allEmpty, IntVectorDirect.ZERO_LENGTH_VECTOR,
                        RepeatedWrappers.newBuilder().addInt32(Int32Value.of(42)).addInt32(Int32Value.of(43)).build(),
                        new IntVectorDirect(42, 43)));

        checkKey(
                RepeatedWrappers.getDescriptor(),
                "uint32",
                IntVector.type(),
                Map.of(
                        allEmpty, IntVectorDirect.ZERO_LENGTH_VECTOR,
                        RepeatedWrappers.newBuilder().addUint32(UInt32Value.of(42)).addUint32(UInt32Value.of(43))
                                .build(),
                        new IntVectorDirect(42, 43)));

        checkKey(
                RepeatedWrappers.getDescriptor(),
                "int64",
                LongVector.type(),
                Map.of(
                        allEmpty, LongVectorDirect.ZERO_LENGTH_VECTOR,
                        RepeatedWrappers.newBuilder().addInt64(Int64Value.of(42)).addInt64(Int64Value.of(43)).build(),
                        new LongVectorDirect(42, 43)));

        checkKey(
                RepeatedWrappers.getDescriptor(),
                "uint64",
                LongVector.type(),
                Map.of(
                        allEmpty, LongVectorDirect.ZERO_LENGTH_VECTOR,
                        RepeatedWrappers.newBuilder().addUint64(UInt64Value.of(42)).addUint64(UInt64Value.of(43))
                                .build(),
                        new LongVectorDirect(42, 43)));

        checkKey(
                RepeatedWrappers.getDescriptor(),
                "float",
                FloatVector.type(),
                Map.of(
                        allEmpty, FloatVectorDirect.ZERO_LENGTH_VECTOR,
                        RepeatedWrappers.newBuilder().addFloat(FloatValue.of(42)).addFloat(FloatValue.of(43)).build(),
                        new FloatVectorDirect(42, 43)));

        checkKey(
                RepeatedWrappers.getDescriptor(),
                "double",
                DoubleVector.type(),
                Map.of(
                        allEmpty, DoubleVectorDirect.ZERO_LENGTH_VECTOR,
                        RepeatedWrappers.newBuilder().addDouble(DoubleValue.of(42)).addDouble(DoubleValue.of(43))
                                .build(),
                        new DoubleVectorDirect(42, 43)));
    }

    @Test
    void repeatedTimestamp() {
        checkKey(
                RepeatedTimestamp.getDescriptor(),
                "ts",
                ObjectVector.type(Type.instantType()),
                Map.of(
                        RepeatedTimestamp.getDefaultInstance(), ObjectVectorDirect.empty(),
                        RepeatedTimestamp.newBuilder()
                                .addTs(Timestamp.newBuilder().setSeconds(1).setNanos(2).build())
                                .addTs(Timestamp.newBuilder().setSeconds(3).setNanos(4).build())
                                .build(),
                        new ObjectVectorDirect<>(
                                Instant.ofEpochSecond(1, 2),
                                Instant.ofEpochSecond(3, 4))));
    }

    @Test
    void repeatedDuration() {
        checkKey(
                RepeatedDuration.getDescriptor(),
                "dur",
                ObjectVector.type(Type.ofCustom(Duration.class)),
                Map.of(
                        RepeatedDuration.getDefaultInstance(), ObjectVectorDirect.empty(),
                        RepeatedDuration.newBuilder()
                                .addDur(com.google.protobuf.Duration.newBuilder().setSeconds(1).setNanos(2).build())
                                .addDur(com.google.protobuf.Duration.newBuilder().setSeconds(3).setNanos(4).build())
                                .build(),
                        new ObjectVectorDirect<>(
                                Duration.ofSeconds(1, 2),
                                Duration.ofSeconds(3, 4))));
    }

    @Test
    void nested() {
        final Map<String, TypedFunction<Message>> nf = Protobuf.namedFunctions(ANested.getDescriptor());
        assertThat(nf.keySet()).containsExactly("baz_foo", "baz_bar");

        checkKey(
                ANested.getDescriptor(),
                "baz_foo",
                Type.intType(),
                Map.of(
                        ANested.getDefaultInstance(), QueryConstants.NULL_INT,
                        ANested.newBuilder().setBaz(SubMessage.newBuilder().setFoo(42).build()).build(), 42));

        checkKey(
                ANested.getDescriptor(),
                "baz_bar",
                Type.longType(),
                Map.of(
                        ANested.getDefaultInstance(), QueryConstants.NULL_LONG,
                        ANested.newBuilder().setBaz(SubMessage.newBuilder().setBar(42L).build()).build(), 42L));
    }

    /*
    message AMultiNested {
  message SubMessage1 {
    message SubMessage2 {
      string world = 1;
    }
    int32 foo = 1;
    int64 bar = 2;
    SubMessage2 baz = 3;
  }
  SubMessage1 hello = 1;
}
     */

    @Test
    void multiNested() {
        final Map<String, TypedFunction<Message>> nf = Protobuf.namedFunctions(AMultiNested.getDescriptor());
        assertThat(nf.keySet()).containsExactly("hello_foo", "hello_bar", "hello_baz_world");

        final AMultiNested defaultInstance = AMultiNested.getDefaultInstance();
        final AMultiNested noBaz = AMultiNested.newBuilder()
                .setHello(SubMessage1.newBuilder().setFoo(42).setBar(43).build())
                .build();
        final AMultiNested bazDefault = AMultiNested.newBuilder()
                .setHello(SubMessage1.newBuilder().setBaz(SubMessage2.getDefaultInstance()).build())
                .build();
        final AMultiNested bazWorld = AMultiNested.newBuilder()
                .setHello(SubMessage1.newBuilder().setBaz(SubMessage2.newBuilder().setWorld("OK").build()).build())
                .build();

        checkKey(
                AMultiNested.getDescriptor(),
                "hello_foo",
                Type.intType(),
                Map.of(
                        defaultInstance, QueryConstants.NULL_INT,
                        noBaz, 42,
                        bazDefault, 0,
                        bazWorld, 0));

        checkKey(
                AMultiNested.getDescriptor(),
                "hello_bar",
                Type.longType(),
                Map.of(
                        defaultInstance, QueryConstants.NULL_LONG,
                        noBaz, 43L,
                        bazDefault, 0L,
                        bazWorld, 0L));

        checkKey(
                AMultiNested.getDescriptor(),
                "hello_baz_world",
                Type.stringType(),
                new HashMap<>() {{
                    put(defaultInstance, null);
                    put(noBaz, null);
                    put(bazDefault, "");
                    put(bazWorld, "OK");
                }});
    }

    private static <T> void checkKey(
            Descriptor descriptor,
            String expectedName,
            Type<T> expectedType,
            Map<Message, T> expectedExamples) {
        final Map<String, TypedFunction<Message>> map = Protobuf.namedFunctions(descriptor);
        // assertThat(map).containsOnlyKeys(expectedName);
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
