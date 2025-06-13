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

    private static ObjectValue options() {
        return ObjectValue.builder()
                .allowUnknownFields(false)
                .putFields("name", StringValue.strict())
                .putFields("age", IntValue.strict())
                .build();
    }

    @Test
    void stream() {
        doTest(JacksonIteratorSpec.stream(options()), JsonParserProvider.of(resource("/io/deephaven/json/test-newline-objects.json.txt")));
    }

    void doTest(JacksonIteratorSpec spec, JsonParserProvider parserProvider) {
        final TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofString("name"),
                ColumnDefinition.ofInt("age"));

        final Table expected = TableTools.newTable(td,
                TableTools.stringCol("name", "foo", "bar"),
                TableTools.intCol("age", 42, 43));

        final JacksonStreamPublisherJob.State state = JacksonStreamPublisherJob.builder()
                .iteratorSpec(spec)
                .addSerialJobs(parserProvider)
                .executor(Runnable::run)
                .build()
                .state();
        final Table table = BlinkTableTools.blinkToAppendOnly(state.table());

        state.start();
        assertThat(table.isEmpty()).isTrue();
        final ControlledUpdateGraph ug = ExecutionContext.getContext().getUpdateGraph().cast();
        ug.runWithinUnitTestCycle(state.adapter()::run);
        TstUtils.assertTableEquals(expected, table);
    }

    private static URL resource(final String resourceName) {
        return JacksonIteratorTest.class.getResource(resourceName);
    }
}
