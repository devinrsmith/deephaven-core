//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DeephavenEngineExtension.class)
class JacksonPublisherJobTest {

    public static final TableDefinition TD1 = TableDefinition.of(
            ColumnDefinition.ofString("name"),
            ColumnDefinition.ofInt("age"));

    public static final TableDefinition TD2 = TableDefinition.of(
            ColumnDefinition.ofString("Key"),
            ColumnDefinition.ofInt("Value"));

    private static ObjectValue options1() {
        return ObjectValue.builder()
                .allowUnknownFields(false)
                .putFields("name", StringValue.strict())
                .putFields("age", IntValue.strict())
                .build();
    }

    @Test
    void stream() {
        doTest(
                TD1,
                JacksonValue2.stream(options1()),
                JsonParserProvider.of(resource("/io/deephaven/json/test-newline-objects.json.txt")));
        doTest(
                TD1,
                JacksonValue2.stream(options1()),
                JsonParserProvider.of(resource("/io/deephaven/json/test-compact-objects.json.txt")));
    }

    @Test
    void array() {
        doTest(
                TD1,
                JacksonValue2.array(options1()),
                JsonParserProvider.of(resource("/io/deephaven/json/test-array-objects.json")));
    }

    @Test
    void objectEntries() {
        doTest(
                TD2,
                JacksonValue2.objectEntries(StringValue.strict(), IntValue.strict()),
                JsonParserProvider.of(resource("/io/deephaven/json/test-object-entries.json")));
    }

    void doTest(final TableDefinition td, final JacksonValue2 spec, final JsonParserProvider parserProvider) {
        final Table expected = TableTools.newTable(td,
                TableTools.stringCol(td.getColumnNames().get(0), "foo", "bar"),
                TableTools.intCol(td.getColumnNames().get(1), 42, 43));

        final ControlledUpdateGraph ug = ExecutionContext.getContext().getUpdateGraph().cast();

        final JacksonStreamPublisherJob.State state = JacksonStreamPublisherJob.builder()
                .iteratorSpec(spec)
                .addSerialJobs(parserProvider)
                .executor(Runnable::run)
                .registrar(ug)
                .build()
                .state();
        final Table table = BlinkTableTools.blinkToAppendOnly(state.table());

        assertThat(state.isDone()).isFalse();
        state.start();
        // This was executed on thread, we know it's done now
        assertThat(state.isDone()).isTrue();

        assertThat(table.isEmpty()).isTrue();
        ug.runWithinUnitTestCycle(state::runAdapter);
        TstUtils.assertTableEquals(expected, table);
    }

    private static URL resource(final String resourceName) {
        return JacksonIteratorTest.class.getResource(resourceName);
    }
}
