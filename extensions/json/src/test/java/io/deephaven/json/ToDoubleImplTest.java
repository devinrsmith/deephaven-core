/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import org.junit.jupiter.api.Test;

public class ToDoubleImplTest {
    @Test
    void name() {
        ToDoubleImpl.builder()
                .onNull(null)
                .build();
    }
}
