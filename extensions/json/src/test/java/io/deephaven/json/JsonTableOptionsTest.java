/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import org.junit.jupiter.api.Test;

import java.net.URL;

public class JsonTableOptionsTest {

    static final ObjectOptions NAME_AGE = ObjectOptions.builder()
            .putFields("name", StringOptions.strict())
            .putFields("age", IntOptions.strict())
            .build();

    static URL resource(String resource) {
        return JsonTableOptionsTest.class.getResource(resource);
    }

    @Test
    void singleObject() {
        JsonTableOptions.builder()
                .addSources(resource("test-single-object.json"))
                .options(NAME_AGE)
                .build();
    }

    @Test
    void arrayObjects() {
        JsonTableOptions.builder()
                .addSources(resource("test-array-objects.json"))
                .options(NAME_AGE.array())
                .build();
    }
}
