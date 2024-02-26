/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import org.junit.jupiter.api.Test;

public class JsonTableOptionsTest {

    static final ObjectOptions NAME_AGE = ObjectOptions.builder()
            .putFields("name", StringOptions.strict())
            .putFields("age", IntOptions.strict())
            .build();

    static Source source(String resource) {
        return Source.of(JsonTableOptionsTest.class.getResource(resource));
    }

    @Test
    void singleObject() {
        JsonTableOptions.builder()
                .source(source("test-single-object.json"))
                .options(NAME_AGE)
                .build();
    }

    @Test
    void arrayObjects() {
        JsonTableOptions.builder()
                .source(source("test-array-object.json"))
                .options(NAME_AGE.array())
                .build();
    }
}
