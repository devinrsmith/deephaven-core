/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import java.time.Instant;

interface OffsetInternal {

    static OffsetInternal beginning() {
        return OffsetImpl.BEGINNING;
    }

    static OffsetInternal end() {
        return OffsetImpl.END;
    }

    static OffsetInternal committed() {
        return OffsetImpl.COMMITTED;
    }

    static OffsetInternal timestamp(Instant since) {
        return ImmutableOffsetTimestamp.of(since);
    }

    static OffsetInternal of(long offset) {
        return ImmutableOffsetExplicit.of(offset);
    }
}
