/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.json.ValueOptions;
import io.deephaven.json.ValueOptions.Visitor;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.processor.ProcessedIterator;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamPublisherIt;
import io.deephaven.stream.StreamToBlinkTableAdapter;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;

public final class Tools {


    public static Iterator<String> iterator(JsonParser parser) throws IOException {
        final ObjectCodec codec = parser.getCodec();
        return codec == null
                ? new JsonParserStringIterator(parser)
                : iterator(parser, codec);
    }

    public static Iterator<String> iterator(JsonParser parser, ObjectCodec codec) throws IOException {
        return codec.readValues(parser, String.class);
    }

    public static Table array(JsonParser parser, NamedObjectProcessor<? super String> processor) throws IOException {
        final ObjectProcessor<? super String> op = processor.processor();
        final List<Type<?>> outputTypes = op.outputTypes();
        final TableDefinition definition = TableDefinition.from(processor.columnNames(), outputTypes);
        final StreamPublisherIt spit = new StreamPublisherIt(outputTypes);
        final StreamToBlinkTableAdapter adapter =
                new StreamToBlinkTableAdapter(definition, spit, ExecutionContext.getContext().getUpdateGraph(), "name");
        Helpers.assertNextToken(parser, JsonToken.START_ARRAY);
        Helpers.assertNextToken(parser, JsonToken.VALUE_STRING);
        final Iterator<String> it = iterator(parser);
        final ProcessedIterator<String> processedIt = ProcessedIterator.it(it, op, 1024);
        // todo; ability to do all on this thread
        spit.submit(Executors.newCachedThreadPool(), processedIt, processedIt::close);
        return adapter.table();
    }

    public static Table example(String file) throws IOException {
        final JsonParser parser = JacksonConfiguration.defaultFactory().createParser(new File(file));
        return array(parser, NamedObjectProcessor.of(ObjectProcessor.simple(Type.stringType()), "String"));
    }

    public static void what(ValueOptions options, JsonFactory factory, Path path) {

    }


}
