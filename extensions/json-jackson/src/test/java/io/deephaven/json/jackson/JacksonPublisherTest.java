//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.json.IntValue;
import io.deephaven.json.ObjectValue;
import io.deephaven.json.StringValue;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DeephavenEngineExtension.class)
class JacksonPublisherTest {

    private static ObjectValue options() {
        return ObjectValue.builder()
                .allowUnknownFields(false)
                .putFields("name", StringValue.strict())
                .putFields("age", IntValue.strict())
                .build();
    }

    @Test
    void stream() throws IOException, InterruptedException {
        final JacksonStreamPublisher publisher = JacksonStreamPublisher.stream(options());
        doTest(publisher, "/io/deephaven/json/test-newline-objects.json.txt");
    }

    @Test
    void array() throws IOException, InterruptedException {
        final JacksonStreamPublisher publisher = JacksonStreamPublisher.array(options());
        doTest(publisher, "/io/deephaven/json/test-array-objects.json");
    }

    private static void doTest(final JacksonStreamPublisher publisher, final String resourceName)
            throws IOException, InterruptedException {
        final TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofString("name"),
                ColumnDefinition.ofInt("age"));

        final Table expected = TableTools.newTable(td,
                TableTools.stringCol("name", "foo", "bar"),
                TableTools.intCol("age", 42, 43));

        final ControlledUpdateGraph ug = ExecutionContext.getContext().getUpdateGraph().cast();
        try (
                final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(td, publisher, ug, "test");
                final JsonParser parser = parser(resourceName)) {
            parser.nextToken();
            final Table table = BlinkTableTools.blinkToAppendOnly(adapter.table());
            publisher.process(parser, 128);
            assertThat(table.isEmpty()).isTrue();
            ug.runWithinUnitTestCycle(adapter::run);
            TstUtils.assertTableEquals(expected, table);
        }
    }

    private static URL resource(final String resourceName) {
        return JacksonIteratorProviderTest.class.getResource(resourceName);
    }

    private static JsonParser parser(final String resourceName) throws IOException {
        return JacksonSource.of(JacksonConfiguration.defaultFactory(), resource(resourceName));
    }
}
