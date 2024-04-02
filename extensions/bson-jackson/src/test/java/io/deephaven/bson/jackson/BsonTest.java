//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.bson.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.json.IntOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.StringOptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.deephaven.bson.jackson.TestHelper.parse;

public class BsonTest {

    private static final ObjectOptions OBJECT_NAME_AGE_FIELD = ObjectOptions.builder()
            .putFields("name", StringOptions.standard())
            .putFields("age", IntOptions.standard())
            .build();

    @Test
    void bson() throws IOException {
        final byte[] bsonExample = new ObjectMapper(new BsonFactory()).writeValueAsBytes(Map.of(
                "name", "foo",
                "age", 42));
        parse(
                new JacksonBsonProvider().provider(OBJECT_NAME_AGE_FIELD).bytesProcessor(),
                List.of(bsonExample),
                ObjectChunk.chunkWrap(new String[] {"foo"}),
                IntChunk.chunkWrap(new int[] {42}));
    }
}