/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.exc.InputCoercionException;
import io.deephaven.chunk.IntChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class IntOptionsTest {

    @Test
    void of() throws IOException {
        parse(IntOptions.of(), "42", IntChunk.chunkWrap(new int[] {42}));
    }

    @Test
    void ofMissing() throws IOException {
        parse(IntOptions.of(), "", IntChunk.chunkWrap(new int[] {QueryConstants.NULL_INT}));
    }

    @Test
    void ofNull() throws IOException {
        parse(IntOptions.of(), "null", IntChunk.chunkWrap(new int[] {QueryConstants.NULL_INT}));
    }

    @Test
    void ofMissingCustom() throws IOException {
        parse(IntOptions.builder().onMissing(-1).build(), "", IntChunk.chunkWrap(new int[] {-1}));
    }

    @Test
    void ofNullCustom() throws IOException {
        parse(IntOptions.builder().onNull(-2).build(), "null", IntChunk.chunkWrap(new int[] {-2}));
    }

    @Test
    void ofStrict() throws IOException {
        parse(IntOptions.ofStrict(), "42", IntChunk.chunkWrap(new int[] {42}));
    }

    @Test
    void ofStrictMissing() throws IOException {
        try {
            parse(IntOptions.ofStrict(), "", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("[<root>]: Unexpected missing value, allowMissing=false");
        }
    }

    @Test
    void ofStrictNull() throws IOException {
        try {
            parse(IntOptions.ofStrict(), "null", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("[<root>]: Unexpected null value, allowNull=false");
        }
    }

    @Test
    void ofOverflow() throws IOException {
        try {
            parse(IntOptions.of(), "2147483648", IntChunk.chunkWrap(new int[1]));
        } catch (InputCoercionException e) {
            assertThat(e).hasMessageContaining(
                    "Numeric value (2147483648) out of range of int (-2147483648 - 2147483647)");
        }
    }

    @Test
    void ofString() throws IOException {
        try {
            parse(IntOptions.of(), "\"42\"", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'VALUE_STRING'");
        }
    }

    @Test
    void ofTrue() throws IOException {
        try {
            parse(IntOptions.of(), "true", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'VALUE_TRUE'");
        }
    }

    @Test
    void ofFalse() throws IOException {
        try {
            parse(IntOptions.of(), "false", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'VALUE_FALSE'");
        }
    }

    @Test
    void ofFloat() throws IOException {
        try {
            parse(IntOptions.of(), "42.0", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'VALUE_NUMBER_FLOAT'");
        }
    }

    @Test
    void ofObject() throws IOException {
        try {
            parse(IntOptions.of(), "{}", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'START_OBJECT'");
        }
    }

    @Test
    void ofArray() throws IOException {
        try {
            parse(IntOptions.of(), "[]", IntChunk.chunkWrap(new int[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'START_ARRAY'");
        }
    }
}
