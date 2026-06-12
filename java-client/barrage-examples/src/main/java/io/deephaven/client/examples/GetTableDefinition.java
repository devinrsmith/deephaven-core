//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.base.system.AsyncSystem;
import io.deephaven.client.impl.BarrageSession;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.time.Duration;

@Command(name = "get-table-definition", mixinStandardHelpOptions = true,
        description = "Request a table definition", version = "0.1.0")
class GetTableDefinition extends FlightClientExampleBase {

    @ArgGroup(exclusive = true, multiplicity = "1")
    Path path;

    @CommandLine.Option(names = {"--timeout"}, description = "The timeout", defaultValue = "PT10s")
    Duration timeout;

    @Override
    protected void execute(FlightSession session) throws Exception {
        System.out.println(BarrageUtil.convertArrowSchema(session.schema(path, timeout)).tableDef);
    }

    public static void main(String[] args) {
        Thread.setDefaultUncaughtExceptionHandler(AsyncSystem.uncaughtExceptionHandler(1, System.err));
        int execute = new CommandLine(new GetTableDefinition()).execute(args);
        System.exit(execute);
    }
}
