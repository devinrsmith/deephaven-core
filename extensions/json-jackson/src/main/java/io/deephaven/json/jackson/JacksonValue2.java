//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.Value;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public interface JacksonValue2 {

    /**
     * Creates an iterator specification for parsing JSON values from a JSON array. The iterator will be capable of
     * parsing from the start of a JSON array, or from an entry in a JSON array. An exhausted iterator will leave the
     * parser at the end array token.
     *
     * @param options the element options
     * @return the iterator specification
     */
    static JacksonValue2 array(final Value options) {
        return Mixin.of(options).arrayProvider();
    }

    /**
     * Creates an iterator specification for parsing root-level JSON values; for example, newline delimited json. The
     * iterator will be capable of starting from a root-level JSON value. An exhausted iterator will leave the parser at
     * the end of the document.
     *
     * @param options the element options
     * @return the iterator specification
     */
    static JacksonValue2 stream(final Value options) {
        return Mixin.of(options).streamProvider();
    }

    /**
     * Creates an iterator specification for parsing JSON object entries. The iterator will be capable of parsing from
     * the start of a JSON object, or from a field in a JSON object. An exhausted iterator will leave the parser at the
     * end object token.
     *
     * @param key the key options
     * @param value the value options
     * @return the iterator specification
     */
    static JacksonValue2 objectEntries(final Value key, final Value value) {
        return new ObjectEntriesIteratorSpec(Mixin.of(key), Mixin.of(value));
    }

    /**
     * Creates an iterator specification for parsing generic JSON tokens and values.
     *
     * @return the spec
     */
    static JacksonValue2 meta() {
        return JacksonMetaProcessor.Spec.META_SPEC;
    }

    /**
     * Creates an iterator. The {@code parser} must be in the context appropriate for this spec. The resulting chunks
     * will have capacity {@code chunkCapacity}. The
     *
     * @param parser the parser
     * @param chunkCapacity the chunk capacity
     * @return the iterator
     * @throws IOException if an IO exception occurs
     */
    JacksonIterator iterator(final JsonParser parser, final int chunkCapacity) throws IOException;

    // TODO processor(...), internal

    /**
     * The output chunk types that will be produced from an {@link #iterator(JsonParser, int) iterator}, corresponding
     * to {@link ObjectProcessor#chunkType(Type)}.
     * 
     * @return output chunk types
     */
    List<Type<?>> outputTypes();

    /**
     * The number of output chunks that will be produced from an {@link #iterator(JsonParser, int) iterator}.
     * 
     * @return the number of output chunks
     */
    int outputSize();

    List<String> names();

    List<String> names(Function<List<String>, String> f);
}
