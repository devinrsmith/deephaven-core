//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.deephaven.json.TestHelper.parse;

public class LongArrayOptionsTest {

    @Test
    void standard() throws IOException {
        parse(LongOptions.standard().array(), "[42, 43]", ObjectChunk.chunkWrap(new Object[] {new long[] {42, 43}}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(LongOptions.standard().array(), "", ObjectChunk.chunkWrap(new Object[] {null}));
    }

    @Test
    void standardNull() throws IOException {
        parse(LongOptions.standard().array(), "null", ObjectChunk.chunkWrap(new Object[] {null}));
    }
}
