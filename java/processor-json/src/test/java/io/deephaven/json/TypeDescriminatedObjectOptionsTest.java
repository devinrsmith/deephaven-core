/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;

public class TypeDescriminatedObjectOptionsTest {

    // todo: enum side
    private static final TypeDescriminatedObjectOptions OPTS = TypeDescriminatedObjectOptions.builder()
            .typeFieldName("type")
            .putTypes("quote", ObjectOptions.builder()
                    .putFieldProcessors("symbol", StringOptions.standard()) // todo: add strict support
                    .putFieldProcessors("bid", DoubleOptions.standard())
                    .putFieldProcessors("ask", DoubleOptions.standard())
                    .build())
            .putTypes("trade", ObjectOptions.builder()
                    .putFieldProcessors("symbol", StringOptions.standard()) // todo: add strict support
                    .putFieldProcessors("price", DoubleOptions.standard())
                    .putFieldProcessors("size", DoubleOptions.standard())
                    .build())
            .build();

    @Test
    void name() throws IOException {

        parse(OPTS, List.of(
                // "",
                // "null",
                // "{}",
                // "{\"type\": null}",
                // "{\"type\": \"other\"}",
                "{\"type\": \"quote\", \"symbol\": \"foo\", \"bid\": 1.01, \"ask\": 1.05}",
                "{\"type\": \"trade\", \"symbol\": \"bar\", \"price\": 42.42, \"size\": 123}"),
                ObjectChunk.chunkWrap(new String[] {"quote", "trade"}), // type
                ObjectChunk.chunkWrap(new String[] {"foo", null}), // quote/symbol
                DoubleChunk.chunkWrap(new double[] {1.01, QueryConstants.NULL_DOUBLE}), // quote/bid
                DoubleChunk.chunkWrap(new double[] {1.05, QueryConstants.NULL_DOUBLE}), // quote/ask
                ObjectChunk.chunkWrap(new String[] {null, "bar"}), // trade/symbol
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 42.42}), // trade/price
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 123})); // trade/size
    }
}
