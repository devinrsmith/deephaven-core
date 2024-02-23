/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.util.SafeCloseable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static io.deephaven.json.jackson.Helpers.assertNextToken;
import static io.deephaven.json.jackson.Helpers.assertNoCurrentToken;

public final class Tools {

    public static void what(Path path) {



    }

    public static Iterator<String> iterator(JsonParser parser) throws IOException {
        final ObjectCodec codec = parser.getCodec();
        return codec == null
                ? new JsonParserStringIterator(parser)
                : iterator(parser, codec);
    }

    public static Iterator<String> iterator(JsonParser parser, ObjectCodec codec) throws IOException {
        return codec.readValues(parser, String.class);
    }


    public static void array(
            JsonParser parser,
            ObjectProcessor<? super String> processor,
            Consumer<List<WritableChunk<?>>> consumer,
            int chunkSize) throws IOException {
        try (final WritableObjectChunk<String, ?> chunk = WritableObjectChunk.makeWritableChunk(chunkSize)) {
            chunk.setSize(0);
            assertNoCurrentToken(parser);
            assertNextToken(parser, JsonToken.START_ARRAY);
            JsonToken token;
            while (true) {
                int ix;
                for (ix = 0; ix < chunkSize && (token = parser.nextToken()) != JsonToken.END_ARRAY; ++ix) {
                    switch (token) {
                        case VALUE_STRING:
                            chunk.set(ix, parser.getText());
                            break;
                        case VALUE_NULL:
                            // todo: allow null?
                            chunk.set(ix, null);
                            break;
                        default:
                            throw new IllegalStateException();
                    }
                }
                if (ix == 0) {
                    break;
                }
                chunk.setSize(ix);
                final List<WritableChunk<?>> out = io.deephaven.processor.Tools.newProcessorChunks(processor, ix);
                try {
                    processor.processAll(chunk, out);
                } catch (Throwable t) {
                    SafeCloseable.closeAll(out.iterator());
                    throw t;
                }
                consumer.accept(out);
                chunk.fillWithNullValue(0, ix);
                chunk.setSize(0);
                if (ix != chunkSize) {
                    break;
                }
            }
            assertNextToken(parser, null);
        }
    }

    // note: we might prefer to exposes this as CloseableIterator in future instead of consumer


}
