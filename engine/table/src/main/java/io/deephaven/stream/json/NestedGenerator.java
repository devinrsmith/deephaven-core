//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stream.json;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class NestedGenerator {
    public static void main(String[] args) throws IOException {

        Random r = new Random(42);
        try (JsonGenerator generator =
                new JsonFactory().createGenerator(new File("/tmp/test.json"), JsonEncoding.UTF8)) {
            generator.writeStartArray();
            for (long outerIx = 0; outerIx < 100_000; ++outerIx) {
                generator.writeStartArray();
                final int innerLength = r.nextInt(1024);
                for (long innerIx = 0; innerIx < innerLength; ++innerIx) {
                    generator.writeStartArray();
                    final int arrayLength = r.nextInt(32);
                    for (int arrayIx = 0; arrayIx < arrayLength; ++arrayIx) {
                        generator.writeNumber((int) (r.nextGaussian() * 100));
                    }
                    generator.writeEndArray();
                }
                generator.writeEndArray();
            }
            generator.writeEndArray();
            generator.flush();
        }
    }
}
