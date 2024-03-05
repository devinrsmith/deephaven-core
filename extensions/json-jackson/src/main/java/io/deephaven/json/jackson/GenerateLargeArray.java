/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Random;

public class GenerateLargeArray {

    public static void main(String[] args) throws IOException {
        final Random r = new Random(31337);
        final JsonFactory factory = new JsonFactory();
        final File file = new File("/tmp/test.double.json");
        final int length = 100000000;
        final Instant start = Instant.parse("2023-01-01T00:00:00Z");

        try (final JsonGenerator generator = factory.createGenerator(file, JsonEncoding.UTF8)) {
            generator.writeStartArray();
            for (int i = 0; i < length; ++i) {
                generator.writeNumber(r.nextDouble());
                // generator.writeString(Double.toString(r.nextDouble()));
                // generator.writeString(Long.toString(start.plusSeconds(i).toEpochMilli() * 1_000_000 +
                // start.getNano()));
            }
            generator.writeEndArray();
        }
    }
}
