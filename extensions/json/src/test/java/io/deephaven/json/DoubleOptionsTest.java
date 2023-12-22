/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.exc.InputCoercionException;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class DoubleOptionsTest {

    @Test
    void of() throws IOException {
        parse(DoubleOptions.of(), List.of("42", "42.42"), DoubleChunk.chunkWrap(new double[] {42, 42.42}));
    }

    @Test
    void ofMissing() throws IOException {
        parse(DoubleOptions.of(), "", DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE}));
    }

    @Test
    void ofNull() throws IOException {
        parse(DoubleOptions.of(), "null", DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE}));
    }

    @Test
    void ofMissingCustom() throws IOException {
        parse(DoubleOptions.builder().onMissing(-1).build(), "", DoubleChunk.chunkWrap(new double[] {-1}));
    }

    @Test
    void ofNullCustom() throws IOException {
        parse(DoubleOptions.builder().onNull(-2).build(), "null", DoubleChunk.chunkWrap(new double[] {-2}));
    }

    @Test
    void ofStrict() throws IOException {
        parse(DoubleOptions.ofStrict(), "42", DoubleChunk.chunkWrap(new double[] {42}));
    }

    @Test
    void ofStrictMissing() throws IOException {
        try {
            parse(DoubleOptions.ofStrict(), "", DoubleChunk.chunkWrap(new double[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("[<root>]: Unexpected missing value, allowMissing=false");
        }
    }

    @Test
    void ofStrictNull() throws IOException {
        try {
            parse(DoubleOptions.ofStrict(), "null", DoubleChunk.chunkWrap(new double[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("[<root>]: Unexpected null value, allowNull=false");
        }
    }

    @Test
    void ofString() throws IOException {
        try {
            parse(DoubleOptions.of(), "\"42\"", DoubleChunk.chunkWrap(new double[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'VALUE_STRING'");
        }
    }

    @Test
    void ofTrue() throws IOException {
        try {
            parse(DoubleOptions.of(), "true", DoubleChunk.chunkWrap(new double[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'VALUE_TRUE'");
        }
    }

    @Test
    void ofFalse() throws IOException {
        try {
            parse(DoubleOptions.of(), "false", DoubleChunk.chunkWrap(new double[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'VALUE_FALSE'");
        }
    }

    @Test
    void ofObject() throws IOException {
        try {
            parse(DoubleOptions.of(), "{}", DoubleChunk.chunkWrap(new double[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'START_OBJECT'");
        }
    }

    @Test
    void ofArray() throws IOException {
        try {
            parse(DoubleOptions.of(), "[]", DoubleChunk.chunkWrap(new double[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'START_ARRAY'");
        }
    }
}
