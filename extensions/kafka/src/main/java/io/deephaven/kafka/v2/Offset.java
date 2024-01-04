/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

public interface Offset {
    static Offset beginning() {
        return OffsetDir.BEGINNING;
    }

    static Offset end() {
        return OffsetDir.END;
    }

    static Offset of(long offset) {
        return ImmutableOffsetReal.of(offset);
    }
}
