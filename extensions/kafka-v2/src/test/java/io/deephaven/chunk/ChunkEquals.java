package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;

import java.util.Arrays;

public class ChunkEquals {

    // todo: should probably be moved into respective classes

    public static boolean equals(Chunk<?> actual, Chunk<?> expected) {
        if (actual instanceof BooleanChunk) {
            if (!(expected instanceof BooleanChunk)) {
                return false;
            }
            return equals((BooleanChunk<?>) actual, (BooleanChunk<?>) expected);
        }
        if (actual instanceof ByteChunk) {
            if (!(expected instanceof ByteChunk)) {
                return false;
            }
            return equals((ByteChunk<?>) actual, (ByteChunk<?>) expected);
        }
        if (actual instanceof CharChunk) {
            if (!(expected instanceof CharChunk)) {
                return false;
            }
            return equals((CharChunk<?>) actual, (CharChunk<?>) expected);
        }
        if (actual instanceof ShortChunk) {
            if (!(expected instanceof ShortChunk)) {
                return false;
            }
            return equals((ShortChunk<?>) actual, (ShortChunk<?>) expected);
        }
        if (actual instanceof IntChunk) {
            if (!(expected instanceof IntChunk)) {
                return false;
            }
            return equals((IntChunk<?>) actual, (IntChunk<?>) expected);
        }
        if (actual instanceof LongChunk) {
            if (!(expected instanceof LongChunk)) {
                return false;
            }
            return equals((LongChunk<?>) actual, (LongChunk<?>) expected);
        }
        if (actual instanceof FloatChunk) {
            if (!(expected instanceof FloatChunk)) {
                return false;
            }
            return equals((FloatChunk<?>) actual, (FloatChunk<?>) expected);
        }
        if (actual instanceof DoubleChunk) {
            if (!(expected instanceof DoubleChunk)) {
                return false;
            }
            return equals((DoubleChunk<?>) actual, (DoubleChunk<?>) expected);
        }
        if (!(expected instanceof ObjectChunk)) {
            return false;
        }
        // Note: we can't be this precise b/c io.deephaven.chunk.ObjectChunk.makeArray doesn't actually use the typed
        // class
        /*
        final ObjectChunk<?, ?> actualObjectChunk = (ObjectChunk<?, ?>) actual;
        final ObjectChunk<?, ?> expectedObjectChunk = (ObjectChunk<?, ?>) expected;
        if (!actualObjectChunk.data.getClass().equals(expectedObjectChunk.data.getClass())) {
            return false;
        }*/
        //noinspection unchecked
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
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }
}
