package io.deephaven.kafka;

import io.deephaven.UncheckedDeephavenException;
import org.apache.commons.lang3.mutable.MutableInt;

class AvroPrimitiveDecode {
    public static int decodeZigZag(byte[] bytes, MutableInt mutablePos) {
        final int pos = mutablePos.getValue();
        int len = 1;
        int b = bytes[pos] & 0xff;
        int n = b & 0x7f;
        if (b > 0x7f) {
            b = bytes[pos + len++] & 0xff;
            n ^= (b & 0x7f) << 7;
            if (b > 0x7f) {
                b = bytes[pos + len++] & 0xff;
                n ^= (b & 0x7f) << 14;
                if (b > 0x7f) {
                    b = bytes[pos + len++] & 0xff;
                    n ^= (b & 0x7f) << 21;
                    if (b > 0x7f) {
                        b = bytes[pos + len++] & 0xff;
                        n ^= (b & 0x7f) << 28;
                        if (b > 0x7f) {
                            throw new UncheckedDeephavenException("Invalid Avro int encoding");
                        }
                    }
                }
            }
        }
        mutablePos.setValue(pos + len);
        return (n >>> 1) ^ -(n & 1); // back to two's-complement
    }

    public static boolean decodeBoolean(byte[] bytes, MutableInt mpos) {
        byte b = bytes[mpos.getAndAdd(1)];
        switch (b) {
            case 0:
                return false;
            case 1:
                return true;
            default:
                throw new IllegalArgumentException("Invalid boolean: " + b);
        }
    }

    public static float decodeFloat(byte[] bytes, MutableInt mpos) {
        int pos = mpos.getAndAdd(4);
        int n = getInt32(bytes, pos);
        return Float.intBitsToFloat(n);
    }

    private static int getInt32(byte[] bytes, int pos) {
        byte b1 = bytes[pos];
        byte b2 = bytes[pos + 1];
        byte b3 = bytes[pos + 2];
        byte b4 = bytes[pos + 3];

        return (b1 & 0xff) | ((b2 & 0xff) << 8) | ((b3 & 0xff) << 16) | ((b4 & 0xff) << 24);
    }

    // only for the schema version
    public static int getBe32(byte[] bytes, int pos) {
        byte b1 = bytes[pos];
        byte b2 = bytes[pos + 1];
        byte b3 = bytes[pos + 2];
        byte b4 = bytes[pos + 3];

        return (b4 & 0xff) | ((b3 & 0xff) << 8) | ((b2 & 0xff) << 16) | ((b1 & 0xff) << 24);
    }

    public static double decodeDouble(byte[] bytes, MutableInt mpos) {
        int pos = mpos.getAndAdd(8);
        long n1 = getInt32(bytes, pos);
        long n2 = getInt32(bytes, pos + 4);
        long n = (n2 << 32) | (n1 & 0xffff_ffffL);
        return Double.longBitsToDouble(n);
    }

    public static long decodeZigZagLongSimple(byte[] bytes, MutableInt mutablePos) {
        final int pos = mutablePos.getValue();
        int len = 1;
        long b = bytes[pos] & 0xff;
        long n = b & 0x7f;
        while (b > 0x7f) {
            b = bytes[pos + len++] & 0xff;
            n ^= (b & 0x7f) << ((len - 1) * 7);
        }
        mutablePos.add(len);
        return (n >>> 1) ^ -(n & 1); // back to two's-complement
    }

    // taken from org.apache.avro.io.BinaryDecoder.readLong
    public static long decodeZigZagLong(byte[] bytes, MutableInt mutablePos) {
        int b = bytes[mutablePos.getAndIncrement()] & 255;
        int n = b & 127;
        long l;
        if (b > 127) {
            b = bytes[mutablePos.getAndIncrement()] & 255;
            n ^= (b & 127) << 7;
            if (b > 127) {
                b = bytes[mutablePos.getAndIncrement()] & 255;
                n ^= (b & 127) << 14;
                if (b > 127) {
                    b = bytes[mutablePos.getAndIncrement()] & 255;
                    n ^= (b & 127) << 21;
                    if (b > 127) {
                        l = innerLongDecode(n, bytes, mutablePos);
                    } else {
                        l = n;
                    }
                } else {
                    l = n;
                }
            } else {
                l = n;
            }
        } else {
            l = n;
        }

        return l >>> 1 ^ -(l & 1L);
    }

    private static long innerLongDecode(long l, byte[] bytes, MutableInt mutablePos) {
        int len = 1;
        int b = bytes[mutablePos.getValue()] & 255;
        l ^= ((long) b & 127L) << 28;
        if (b > 127) {
            b = bytes[mutablePos.getValue() + len++] & 255;
            l ^= ((long) b & 127L) << 35;
            if (b > 127) {
                b = bytes[mutablePos.getValue() + len++] & 255;
                l ^= ((long) b & 127L) << 42;
                if (b > 127) {
                    b = bytes[mutablePos.getValue() + len++] & 255;
                    l ^= ((long) b & 127L) << 49;
                    if (b > 127) {
                        b = bytes[mutablePos.getValue() + len++] & 255;
                        l ^= ((long) b & 127L) << 56;
                        if (b > 127) {
                            b = bytes[mutablePos.getValue() + len++] & 255;
                            l ^= ((long) b & 127L) << 63;
                            if (b > 127) {
                                throw new RuntimeException("Invalid long encoding");
                            }
                        }
                    }
                }
            }
        }

        mutablePos.add(len);
        return l;
    }
}
