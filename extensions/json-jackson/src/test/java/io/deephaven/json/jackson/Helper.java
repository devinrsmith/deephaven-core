//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.TestHelper;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class Helper {
    public static void check(final JacksonIterator it) throws IOException {
        {
            assertThat(it).hasNext();
            final List<WritableChunk<?>> chunks = it.nextChunks();
            assertThat(chunks.size()).isEqualTo(2);
            TestHelper.check(chunks.get(0), ObjectChunk.chunkWrap(new String[] {"foo", "bar"}));
            TestHelper.check(chunks.get(1), IntChunk.chunkWrap(new int[] {42, 43}));
        }
        assertThat(it).isExhausted();
    }

    public static void check2(final JacksonIterator it) throws IOException {
        {
            assertThat(it).hasNext();
            final List<WritableChunk<?>> chunks = it.nextChunks();
            assertThat(chunks.size()).isEqualTo(2);
            TestHelper.check(chunks.get(0), ObjectChunk.chunkWrap(new String[] {"foo"}));
            TestHelper.check(chunks.get(1), IntChunk.chunkWrap(new int[] {42}));
        }
        {
            assertThat(it).hasNext();
            final List<WritableChunk<?>> chunks = it.nextChunks();
            assertThat(chunks.size()).isEqualTo(2);
            TestHelper.check(chunks.get(0), ObjectChunk.chunkWrap(new String[] {"bar"}));
            TestHelper.check(chunks.get(1), IntChunk.chunkWrap(new int[] {43}));
        }
        assertThat(it).isExhausted();
    }
}
