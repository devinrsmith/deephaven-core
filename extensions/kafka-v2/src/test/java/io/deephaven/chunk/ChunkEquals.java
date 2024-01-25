package io.deephaven.chunk;

import java.util.Arrays;
import java.util.Objects;

public class ChunkEquals {

    // todo: should probably be moved into respective classes



    public static boolean equals(Chunk<?> x, Chunk<?> y) {
        if (x instanceof BooleanChunk) {
            if (!(y instanceof BooleanChunk)) {
                return false;
            }
            return equals((BooleanChunk<?>) x, (BooleanChunk<?>) y);
        }
        if (x instanceof ByteChunk) {
            if (!(y instanceof ByteChunk)) {
                return false;
            }
            return equals((ByteChunk<?>) x, (ByteChunk<?>) y);
        }
        if (x instanceof CharChunk) {
            if (!(y instanceof CharChunk)) {
                return false;
            }
            return equals((CharChunk<?>) x, (CharChunk<?>) y);
        }
        if (x instanceof ShortChunk) {
            if (!(y instanceof ShortChunk)) {
                return false;
            }
            return equals((ShortChunk<?>) x, (ShortChunk<?>) y);
        }
        if (x instanceof IntChunk) {
            if (!(y instanceof IntChunk)) {
                return false;
            }
            return equals((IntChunk<?>) x, (IntChunk<?>) y);
        }
        if (x instanceof LongChunk) {
            if (!(y instanceof LongChunk)) {
                return false;
            }
            return equals((LongChunk<?>) x, (LongChunk<?>) y);
        }
        if (x instanceof FloatChunk) {
            if (!(y instanceof FloatChunk)) {
                return false;
            }
            return equals((FloatChunk<?>) x, (FloatChunk<?>) y);
        }
        if (x instanceof DoubleChunk) {
            if (!(y instanceof DoubleChunk)) {
                return false;
            }
            return equals((DoubleChunk<?>) x, (DoubleChunk<?>) y);
        }
        if (!(y instanceof ObjectChunk)) {
            return false;
        }
        final ObjectChunk<?, ?> actualObjectChunk = (ObjectChunk<?, ?>) x;
        final ObjectChunk<?, ?> expectedObjectChunk = (ObjectChunk<?, ?>) y;
        // Note: we can't be this precise b/c io.deephaven.chunk.ObjectChunk.makeArray doesn't actually use the typed
        // class
        /*
         * if (!actualObjectChunk.data.getClass().equals(expectedObjectChunk.data.getClass())) { return false; }
         */
        // noinspection unchecked
        return equals((ObjectChunk<Object, ?>) actualObjectChunk, (ObjectChunk<Object, ?>) expectedObjectChunk);
    }

    public static boolean equals(BooleanChunk<?> actual, BooleanChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static boolean equals(ByteChunk<?> actual, ByteChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static boolean equals(CharChunk<?> actual, CharChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static boolean equals(ShortChunk<?> actual, ShortChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static boolean equals(IntChunk<?> actual, IntChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static boolean equals(LongChunk<?> actual, LongChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static boolean equals(FloatChunk<?> actual, FloatChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static boolean equals(DoubleChunk<?> actual, DoubleChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static <T> boolean equals(ObjectChunk<T, ?> actual, ObjectChunk<T, ?> expected) {
        // this is probably only suitable for unit testing, not something we'd want in real code?
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size,
                ChunkEquals::fakeDeepCompareForEquals);
    }

    private static int fakeDeepCompareForEquals(Object a, Object b) {
        return Objects.deepEquals(a, b) ? 0 : 1;
    }
}
