/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.avro;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore.Cache;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class FooBarTest {


    private static final io.deephaven.example.v1.User USER_V1 = io.deephaven.example.v1.User.newBuilder()
            .setFoo(io.deephaven.example.v1.Foo.newBuilder().setA("Foo").setB(42L).build())
            .setBar(io.deephaven.example.v1.Bar.newBuilder().setA("Bar").setB(43L).build())
            .build();

    private static final io.deephaven.example.v2.User USER_V2 = io.deephaven.example.v2.User.newBuilder()
            .setBar(io.deephaven.example.v2.Bar.newBuilder().setC(99).setB(43L).setA("Bar").build())
            .setFoo(io.deephaven.example.v2.Foo.newBuilder().setB(42L).setA("Foo").build())
            .build();

    private static final io.deephaven.example.v2.User USER_V2_DEFAULT = io.deephaven.example.v2.User.newBuilder()
            .setBar(io.deephaven.example.v2.Bar.newBuilder().setB(43L).setA("Bar").build())
            .setFoo(io.deephaven.example.v2.Foo.newBuilder().setB(42L).setA("Foo").build())
            .build();

    private static final io.deephaven.example.v3.User USER_V3_1 = io.deephaven.example.v3.User.newBuilder()
            .setBar(io.deephaven.example.v3.Bar.newBuilder().setC(99).setB(43L).setA("Bar").build())
            .setFoo(io.deephaven.example.v3.Foo.newBuilder().setB(42L).setA("Foo").build())
            .build();

    private static final io.deephaven.example.v3.User USER_V3_2 = io.deephaven.example.v3.User.newBuilder()
            .setBar(io.deephaven.example.v3.Bar.newBuilder().setC(null).setB(43L).setA("Bar").build())
            .setFoo(io.deephaven.example.v3.Foo.newBuilder().setB(42L).setA("Foo").build())
            .build();

    private static final Schema SCHEMA_V1 = io.deephaven.example.v1.User.getClassSchema();
    private static final Schema SCHEMA_V2 = io.deephaven.example.v2.User.getClassSchema();
    private static final Schema SCHEMA_V3 = io.deephaven.example.v3.User.getClassSchema();

    public static final Cache RESOLVER = new Cache();

    private static byte[] v1Bytes;
    private static byte[] v2Bytes;
    private static byte[] v3Bytes;

    private static BinaryDecoder v1RawDecoder() {
        return DecoderFactory.get().binaryDecoder(v1Bytes, 10, v1Bytes.length - 10, null);
    }

    private static BinaryDecoder v2RawDecoder() {
        return DecoderFactory.get().binaryDecoder(v2Bytes, 10, v2Bytes.length - 10, null);
    }

    private static BinaryDecoder v3RawDecoder() {
        return DecoderFactory.get().binaryDecoder(v3Bytes, 10, v3Bytes.length - 10, null);
    }

    @BeforeAll
    static void beforeAll() throws IOException {
        final ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
        io.deephaven.example.v1.User.getEncoder().encode(USER_V1, baos1);
        v1Bytes = baos1.toByteArray();

        final ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        io.deephaven.example.v2.User.getEncoder().encode(USER_V2, baos2);
        v2Bytes = baos2.toByteArray();

        final ByteArrayOutputStream baos3 = new ByteArrayOutputStream();
        io.deephaven.example.v3.User.getEncoder().encode(USER_V3_1, baos3);
        v3Bytes = baos3.toByteArray();

        RESOLVER.addSchema(SCHEMA_V1);
        RESOLVER.addSchema(SCHEMA_V2);
        RESOLVER.addSchema(SCHEMA_V3);
    }

    @Test
    void v1() throws IOException {
        final io.deephaven.example.v1.User decoded = io.deephaven.example.v1.User.getDecoder().decode(v1Bytes);
        assertThat(decoded).isEqualTo(USER_V1);
    }

    @Test
    void v1Generic() {
        readGenericV1(USER_V1);
    }

    @Test
    void v1Decoder() throws IOException {
        final BinaryDecoder decoder = v1RawDecoder();
        readDecoderV1(decoder);
        assertThat(decoder.isEnd()).isTrue();
    }

    @Test
    void v1ResolvingDecoder() throws IOException {
        final BinaryDecoder raw = v1RawDecoder();
        final ResolvingDecoder decoder = DecoderFactory.get().resolvingDecoder(SCHEMA_V1, SCHEMA_V1, raw);
        readDecoderV1(decoder);
        assertThat(raw.isEnd()).isTrue();
    }

    @Test
    void v2() throws IOException {
        final io.deephaven.example.v2.User decoded = io.deephaven.example.v2.User.getDecoder().decode(v2Bytes);
        assertThat(decoded).isEqualTo(USER_V2);
    }

    @Test
    void v2Generic() {
        readGenericV2(USER_V2, true);
    }

    @Test
    void v2Decoder() throws IOException {
        final BinaryDecoder decoder = v2RawDecoder();
        readDecoderV2(decoder);
        assertThat(decoder.isEnd()).isTrue();
    }

    @Test
    void v2ResolvingDecoder() throws IOException {
        final BinaryDecoder raw = v2RawDecoder();
        final ResolvingDecoder decoder = DecoderFactory.get().resolvingDecoder(SCHEMA_V2, SCHEMA_V2, raw);
        readDecoderV2(decoder);
        assertThat(raw.isEnd()).isTrue();
    }

    @Test
    void v1DecoderV2() throws IOException {
        final BinaryMessageDecoder<io.deephaven.example.v1.User> decoder = io.deephaven.example.v1.User.createDecoder(RESOLVER);
        final io.deephaven.example.v1.User decoded = decoder.decode(v2Bytes);
        assertThat(decoded).isEqualTo(USER_V1);
    }

    @Test
    void v2DecoderV1() throws IOException {
        final BinaryMessageDecoder<io.deephaven.example.v2.User> decoder = io.deephaven.example.v2.User.createDecoder(RESOLVER);
        final io.deephaven.example.v2.User decoded = decoder.decode(v1Bytes);
        assertThat(decoded).isEqualTo(USER_V2_DEFAULT);
    }

    @Test
    void v1GenericV2() throws IOException {
        final GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(SCHEMA_V2, SCHEMA_V1);
        final GenericRecord genericRecord = reader.read(null, v2RawDecoder());
        readGenericV1(genericRecord);
    }

    @Test
    void v2GenericV1() throws IOException {
        final GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(SCHEMA_V1, SCHEMA_V2);
        final GenericRecord genericRecord = reader.read(null, v1RawDecoder());
        readGenericV2(genericRecord, false);
    }

    @Test
    void v1ResolvingDecoderV2() throws IOException {
        final ResolvingDecoder decoder = DecoderFactory.get().resolvingDecoder(SCHEMA_V2, SCHEMA_V1, v2RawDecoder());
        readResolvingDecoderV1(decoder);
    }

    @Test
    void v2ResolvingDecoderV1() throws IOException {
        final ResolvingDecoder decoder = DecoderFactory.get().resolvingDecoder(SCHEMA_V1, SCHEMA_V2, v1RawDecoder());
        readResolvingDecoderV2(decoder, true);
    }

    @Test
    void v2ResolvingDecoderV3() throws IOException {
        final ResolvingDecoder decoder = DecoderFactory.get().resolvingDecoder(SCHEMA_V3, SCHEMA_V2, v3RawDecoder());
        readResolvingDecoderV2(decoder, false);
    }

    private static void readGenericV1(GenericRecord record) {
        assertThat(((GenericRecord) record.get(0)).get(0)).isEqualTo("Foo");
        assertThat(((GenericRecord) record.get(0)).get(1)).isEqualTo(42L);
        assertThat(((GenericRecord) record.get(1)).get(0)).isEqualTo("Bar");
        assertThat(((GenericRecord) record.get(1)).get(1)).isEqualTo(43L);
    }

    private static void readGenericV2(GenericRecord record, boolean hasC) {
        if (hasC) {
            assertThat(((GenericRecord) record.get(0)).get(0)).isEqualTo(99);
        } else {
            assertThat(((GenericRecord) record.get(0)).get(0)).isNull();
        }
        assertThat(((GenericRecord) record.get(0)).get(1)).isEqualTo(43L);
        assertThat(((GenericRecord) record.get(0)).get(2)).isEqualTo("Bar");
        assertThat(((GenericRecord) record.get(1)).get(0)).isEqualTo(42L);
        assertThat(((GenericRecord) record.get(1)).get(1)).isEqualTo("Foo");
    }

    private static void readDecoderV1(Decoder decoder) throws IOException {
        assertThat(decoder.readString()).isEqualTo("Foo");
        assertThat(decoder.readLong()).isEqualTo(42L);
        assertThat(decoder.readString()).isEqualTo("Bar");
        assertThat(decoder.readLong()).isEqualTo(43L);
    }

    private static void readDecoderV2(Decoder decoder) throws IOException {
        switch (decoder.readIndex()) {
            case 0:
                decoder.readNull();
                break;
            case 1:
                assertThat(decoder.readInt()).isEqualTo(99);
                break;
            default:
                throw new IllegalStateException();
        }
        assertThat(decoder.readLong()).isEqualTo(43L);
        assertThat(decoder.readString()).isEqualTo("Bar");
        assertThat(decoder.readLong()).isEqualTo(42L);
        assertThat(decoder.readString()).isEqualTo("Foo");
    }

    private static void readResolvingDecoderV1(ResolvingDecoder decoder) throws IOException {
        for (Field field : decoder.readFieldOrder()) {
            switch (field.pos()) {
                case 0:
                    readFooV1(decoder);
                    break;
                case 1:
                    readBarV1(decoder);
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    private static void readFooV1(ResolvingDecoder decoder) throws IOException {
        for (Field field : decoder.readFieldOrder()) {
            switch (field.pos()) {
                case 0:
                    assertThat(decoder.readString()).isEqualTo("Foo");
                    break;
                case 1:
                    assertThat(decoder.readLong()).isEqualTo(42L);
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    private static void readBarV1(ResolvingDecoder decoder) throws IOException {
        for (Field field : decoder.readFieldOrder()) {
            switch (field.pos()) {
                case 0:
                    assertThat(decoder.readString()).isEqualTo("Bar");
                    break;
                case 1:
                    assertThat(decoder.readLong()).isEqualTo(43L);
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    private static void readResolvingDecoderV2(ResolvingDecoder decoder, boolean nullC) throws IOException {
        for (Field field : decoder.readFieldOrder()) {
            switch (field.pos()) {
                case 0:
                    readBarV2(decoder, nullC);
                    break;
                case 1:
                    readFooV2(decoder);
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    private static void readFooV2(ResolvingDecoder decoder) throws IOException {
        for (Field field : decoder.readFieldOrder()) {
            switch (field.pos()) {
                case 0:
                    assertThat(decoder.readLong()).isEqualTo(42L);
                    break;
                case 1:
                    assertThat(decoder.readString()).isEqualTo("Foo");
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    private static void readBarV2(ResolvingDecoder decoder, boolean nullC) throws IOException {
        for (Field field : decoder.readFieldOrder()) {
            switch (field.pos()) {
                case 0:
                    switch(decoder.readIndex()) {
                        case 0:
                            assertThat(nullC).isTrue();
                            decoder.readNull();
                            break;
                        case 1:
                            assertThat(nullC).isFalse();
                            assertThat(decoder.readInt()).isEqualTo(99);
                            break;
                        default:
                            throw new IllegalStateException();
                    }
                    break;
                case 1:
                    assertThat(decoder.readLong()).isEqualTo(43L);
                    break;
                case 2:
                    assertThat(decoder.readString()).isEqualTo("Bar");
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
    }
}
