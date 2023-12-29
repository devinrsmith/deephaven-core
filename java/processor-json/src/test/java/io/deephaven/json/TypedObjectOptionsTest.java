/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;

public class TypedObjectOptionsTest {

    // todo: enum side
    private static final TypedObjectOptions OPTS = TypedObjectOptions.builder()
            .typeFieldName("type")
            .putSharedFields("symbol", StringOptions.strict())
            .putObjects("quote", ObjectOptions.builder()
                    .putFields("bid", DoubleOptions.standard())
                    .putFields("ask", DoubleOptions.standard())
                    .build())
            .putObjects("trade", ObjectOptions.builder()
                    .putFields("price", DoubleOptions.standard())
                    .putFields("size", DoubleOptions.standard())
                    .build())
            .build();

    @Test
    void typeDiscriminationQuoteTrade() throws IOException {
        parse(OPTS, List.of(
                // "",
                // "null",
                // "{}",
                // "{\"type\": null}",
                // "{\"type\": \"other\"}",
                "{\"type\": \"quote\", \"symbol\": \"foo\", \"bid\": 1.01, \"ask\": 1.05}",
                "{\"type\": \"trade\", \"symbol\": \"bar\", \"price\": 42.42, \"size\": 123}"),
                ObjectChunk.chunkWrap(new String[] {"quote", "trade"}), // type
                ObjectChunk.chunkWrap(new String[] {"foo", "bar"}), // symbol
                DoubleChunk.chunkWrap(new double[] {1.01, QueryConstants.NULL_DOUBLE}), // quote/bid
                DoubleChunk.chunkWrap(new double[] {1.05, QueryConstants.NULL_DOUBLE}), // quote/ask
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 42.42}), // trade/price
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 123})); // trade/size
    }
}
