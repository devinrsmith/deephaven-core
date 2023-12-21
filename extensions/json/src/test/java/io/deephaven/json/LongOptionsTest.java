/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.exc.InputCoercionException;
import io.deephaven.chunk.LongChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.deephaven.json.TestHelper.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class LongOptionsTest {

    @Test
    void of() throws IOException {
        parse(LongOptions.of(), "42", LongChunk.chunkWrap(new long[] {42}));
    }

    @Test
    void ofMissing() throws IOException {
        parse(LongOptions.of(), "", LongChunk.chunkWrap(new long[] {QueryConstants.NULL_LONG}));
    }

    @Test
    void ofNull() throws IOException {
        parse(LongOptions.of(), "null", LongChunk.chunkWrap(new long[] {QueryConstants.NULL_LONG}));
    }

    @Test
    void ofMissingCustom() throws IOException {
        parse(LongOptions.builder().onMissing(-1).build(), "", LongChunk.chunkWrap(new long[] {-1}));
    }

    @Test
    void ofNullCustom() throws IOException {
        parse(LongOptions.builder().onNull(-2).build(), "null", LongChunk.chunkWrap(new long[] {-2}));
    }

    @Test
    void ofStrict() throws IOException {
        parse(LongOptions.ofStrict(), "42", LongChunk.chunkWrap(new long[] {42}));
    }

    @Test
    void ofStrictMissing() throws IOException {
        try {
            parse(LongOptions.ofStrict(), "", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("[<root>]: Unexpected missing value, allowMissing=false");
        }
    }

    @Test
    void ofStrictNull() throws IOException {
        try {
            parse(LongOptions.ofStrict(), "null", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("[<root>]: Unexpected null value, allowNull=false");
        }
    }

    @Test
    void ofOverflow() throws IOException {
        try {
            parse(LongOptions.of(), "9223372036854775808", LongChunk.chunkWrap(new long[1]));
        } catch (InputCoercionException e) {
            assertThat(e).hasMessageContaining(
                    "Numeric value (9223372036854775808) out of range of long (-9223372036854775808 - 9223372036854775807)");
        }
    }

    @Test
    void ofString() throws IOException {
        try {
            parse(LongOptions.of(), "\"42\"", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'VALUE_STRING'");
        }
    }

    @Test
    void ofTrue() throws IOException {
        try {
            parse(LongOptions.of(), "true", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'VALUE_TRUE'");
        }
    }

    @Test
    void ofFalse() throws IOException {
        try {
            parse(LongOptions.of(), "false", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'VALUE_FALSE'");
        }
    }

    @Test
    void ofFloat() throws IOException {
        try {
            parse(LongOptions.of(), "42.0", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'VALUE_NUMBER_FLOAT'");
        }
    }

    @Test
    void ofObject() throws IOException {
        try {
            parse(LongOptions.of(), "{}", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'START_OBJECT'");
        }
    }

    @Test
    void ofArray() throws IOException {
        try {
            parse(LongOptions.of(), "[]", LongChunk.chunkWrap(new long[1]));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Unexpected start token 'START_ARRAY'");
        }
    }
}
