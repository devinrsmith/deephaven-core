/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

public interface Offset {
    /**
     * The earliest offset.
     *
     * @return the earliest offset
     */
    static Offset earliest() {
        return OffsetDir.EARLIEST;
    }

    /**
     * The latest offset.
     *
     * @return the latest offset
     */
    static Offset latest() {
        return OffsetDir.LATEST;
    }

    /**
     * The offset.
     *
     * @param offset the offset
     * @return the offset
     */
    static Offset of(long offset) {
        return ImmutableOffsetReal.of(offset);
    }
}
