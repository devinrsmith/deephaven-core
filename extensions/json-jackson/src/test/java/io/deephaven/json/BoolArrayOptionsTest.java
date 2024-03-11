/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.deephaven.json.TestHelper.parse;

public class BoolArrayOptionsTest {

    @Test
    void standard() throws IOException {
        parse(BoolOptions.standard().array(), "[true, null, false]",
                ObjectChunk.chunkWrap(new Object[] {new Boolean[] {true, null, false}}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(BoolOptions.standard().array(), "", ObjectChunk.chunkWrap(new Object[] {null}));
    }

    @Test
    void standardNull() throws IOException {
        parse(BoolOptions.standard().array(), "null", ObjectChunk.chunkWrap(new Object[] {null}));
    }
}
