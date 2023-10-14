package io.deephaven.avro;

import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.mutable.MutableInt;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

class AvroParsingState {
    private final byte[] payload;
    private final MutableInt pos;

    public AvroParsingState(byte[] payload, MutableInt pos) {
        this.payload = Objects.requireNonNull(payload);
        this.pos = Objects.requireNonNull(pos);
    }

    public boolean decodeBoolean() {
        return AvroPrimitiveDecode.decodeBoolean(payload, pos);
    }

    public int decodeInt() {
        return AvroPrimitiveDecode.decodeZigZag(payload, pos);
    }

    public long decodeLong() {
        return AvroPrimitiveDecode.decodeZigZagLong(payload, pos);
    }

    public float decodeFloat() {
        return AvroPrimitiveDecode.decodeFloat(payload, pos);
    }

    public double decodeDouble() {
        return AvroPrimitiveDecode.decodeDouble(payload, pos);
    }

    public String decodeString() {
        // why long?
        int numBytes = Math.toIntExact(AvroPrimitiveDecode.decodeZigZagLong(payload, pos));
        return new String(payload, pos.getAndAdd(numBytes), numBytes, StandardCharsets.UTF_8);
    }

    public Boolean decodeNullableBoolean0() {
        final int ut = decodeUnionIndex();
        switch (ut) {
            case 1: return null;
            case 0: return decodeBoolean();
            default: throw new IllegalArgumentException("Invalid union index " + ut);
        }
    }

    public int decodeNullableInt0() {
        final int ut = decodeUnionIndex();
        switch (ut) {
            case 1: return QueryConstants.NULL_INT;
            case 0: return decodeInt();
            default: throw new IllegalArgumentException("Invalid union index " + ut);
        }
    }

    public long decodeNullableLong0() {
        final int ut = decodeUnionIndex();
        switch (ut) {
            case 1: return QueryConstants.NULL_INT;
            case 0: return decodeLong();
            default: throw new IllegalArgumentException("Invalid union index " + ut);
        }
    }

    public float decodeNullableFloat0() {
        final int ut = decodeUnionIndex();
        switch (ut) {
            case 1: return QueryConstants.NULL_FLOAT;
            case 0: return decodeFloat();
            default: throw new IllegalArgumentException("Invalid union index " + ut);
        }
    }

    public double decodeNullableDouble0() {
        final int ut = decodeUnionIndex();
        switch (ut) {
            case 1: return QueryConstants.NULL_DOUBLE;
            case 0: return decodeDouble();
            default: throw new IllegalArgumentException("Invalid union index " + ut);
        }
    }

    public String decodeNullableString0() {
        final int ut = decodeUnionIndex();
        switch (ut) {
            case 1: return null;
            case 0: return decodeString();
            default: throw new IllegalArgumentException("Invalid union index" + ut);
        }
    }

    public Boolean decodeNullableBoolean1() {
        final int ut = decodeUnionIndex();
        switch (ut) {
            case 0: return null;
            case 1: return decodeBoolean();
            default: throw new IllegalArgumentException("Invalid union index " + ut);
        }
    }

    public int decodeNullableInt1() {
        final int ut = decodeUnionIndex();
        switch (ut) {
            case 0: return QueryConstants.NULL_INT;
            case 1: return decodeInt();
            default: throw new IllegalArgumentException("Invalid union index " + ut);
        }
    }

    public long decodeNullableLong1() {
        final int ut = decodeUnionIndex();
        switch (ut) {
            case 0: return QueryConstants.NULL_INT;
            case 1: return decodeLong();
            default: throw new IllegalArgumentException("Invalid union index " + ut);
        }
    }

    public float decodeNullableFloat1() {
        final int ut = decodeUnionIndex();
        switch (ut) {
            case 0: return QueryConstants.NULL_FLOAT;
            case 1: return decodeFloat();
            default: throw new IllegalArgumentException("Invalid union index " + ut);
        }
    }

    public double decodeNullableDouble1() {
        final int ut = decodeUnionIndex();
        switch (ut) {
            case 0: return QueryConstants.NULL_DOUBLE;
            case 1: return decodeDouble();
            default: throw new IllegalArgumentException("Invalid union index " + ut);
        }
    }

    public String decodeNullableString1() {
        final int ut = decodeUnionIndex();
        switch (ut) {
            case 0: return null;
            case 1: return decodeString();
            default: throw new IllegalArgumentException("Invalid union index" + ut);
        }
    }

    private int decodeUnionIndex() {
        // Why long?
        // Should we use Math.toIntExact?
        return (int) AvroPrimitiveDecode.decodeZigZagLong(payload, pos);
    }
}
