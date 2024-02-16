/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.bson4jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.json.IntOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.StringOptions;
import io.deephaven.json.jackson.JacksonConfiguration;
import io.deephaven.json.jackson.JacksonProvider;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.deephaven.json.TestHelper.parse;

public class BsonExampleTest {
    private static final BsonFactory FACTORY = new BsonFactory();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(FACTORY);
    private static final JacksonConfiguration BSON_DEFAULT = JacksonConfiguration.of(FACTORY);
    private static final ObjectOptions OBJECT_NAME_AGE_FIELD = ObjectOptions.builder()
            .putFields("name", StringOptions.standard())
            .putFields("age", IntOptions.standard())
            .build();

    @Test
    void bson() throws IOException {
        final byte[] bsonExample = OBJECT_MAPPER.writeValueAsBytes(Map.of(
                "name", "foo",
                "age", 42));
        parse(
                JacksonProvider.of(OBJECT_NAME_AGE_FIELD, BSON_DEFAULT).bytesProcessor(),
                List.of(bsonExample),
                ObjectChunk.chunkWrap(new String[] {"foo"}),
                IntChunk.chunkWrap(new int[] {42}));
    }
}
