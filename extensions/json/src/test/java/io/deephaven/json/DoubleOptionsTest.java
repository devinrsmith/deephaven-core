/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class DoubleOptionsTest {

    @Test
    void standard() throws IOException {
        parse(DoubleOptions.standard(), List.of("42", "42.42"), DoubleChunk.chunkWrap(new double[] {42, 42.42}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(DoubleOptions.standard(), "", DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE}));
    }

    @Test
    void standardNull() throws IOException {
        parse(DoubleOptions.standard(), "null", DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE}));
    }

    @Test
    void customMissing() throws IOException {
        parse(DoubleOptions.builder().onMissing(-1.0).build(), "", DoubleChunk.chunkWrap(new double[] {-1}));
    }

    @Test
    void strict() throws IOException {
        parse(DoubleOptions.strict(), "42", DoubleChunk.chunkWrap(new double[] {42}));
    }

    @Test
    void strictMissing() throws IOException {
        try {
            parse(DoubleOptions.strict(), "", DoubleChunk.chunkWrap(new double[1]));
            failBecauseExceptionWasNotThrown(MismatchedInputException.class);
        } catch (MismatchedInputException e) {
            assertThat(e).hasMessageContaining("Missing token");
        }
    }

    @Test
    void strictNull() throws IOException {
        try {
            parse(DoubleOptions.strict(), "null", DoubleChunk.chunkWrap(new double[1]));
            failBecauseExceptionWasNotThrown(MismatchedInputException.class);
        } catch (MismatchedInputException e) {
            assertThat(e).hasMessageContaining("Not expecting token 'VALUE_NULL'");
        }
    }

    @Test
    void standardString() throws IOException {
        try {
            parse(DoubleOptions.standard(), "\"42\"", DoubleChunk.chunkWrap(new double[1]));
            failBecauseExceptionWasNotThrown(MismatchedInputException.class);
        } catch (MismatchedInputException e) {
            assertThat(e).hasMessageContaining("Not expecting token 'VALUE_STRING'");
        }
    }

    @Test
    void standardTrue() throws IOException {
        try {
            parse(DoubleOptions.standard(), "true", DoubleChunk.chunkWrap(new double[1]));
            failBecauseExceptionWasNotThrown(MismatchedInputException.class);
        } catch (MismatchedInputException e) {
            assertThat(e).hasMessageContaining("Not expecting token 'VALUE_TRUE'");
        }
    }

    @Test
    void standardFalse() throws IOException {
        try {
            parse(DoubleOptions.standard(), "false", DoubleChunk.chunkWrap(new double[1]));
            failBecauseExceptionWasNotThrown(MismatchedInputException.class);
        } catch (MismatchedInputException e) {
            assertThat(e).hasMessageContaining("Not expecting token 'VALUE_FALSE'");
        }
    }

    @Test
    void standardObject() throws IOException {
        try {
            parse(DoubleOptions.standard(), "{}", DoubleChunk.chunkWrap(new double[1]));
            failBecauseExceptionWasNotThrown(MismatchedInputException.class);
        } catch (MismatchedInputException e) {
            assertThat(e).hasMessageContaining("Not expecting token 'START_OBJECT'");
        }
    }

    @Test
    void standardArray() throws IOException {
        try {
            parse(DoubleOptions.standard(), "[]", DoubleChunk.chunkWrap(new double[1]));
            failBecauseExceptionWasNotThrown(MismatchedInputException.class);
        } catch (MismatchedInputException e) {
            assertThat(e).hasMessageContaining("Not expecting token 'START_ARRAY'");
        }
    }
}
