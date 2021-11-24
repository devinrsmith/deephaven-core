
package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.TicketTable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "bad-example", mixinStandardHelpOptions = true,
        description = "Bad example", version = "0.1.0")
class BadExample extends SingleSessionExampleBase {

    @Parameters(arity = "1", paramLabel = "TICKET", description = "The ticket.")
    String ticket;

    @Override
    protected void execute(Session session) throws Exception {
        try (final TableHandle handle1 = session.execute(TicketTable.of(ticket))) {
            Thread.sleep(1000);
            try (final TableHandle handle2 = handle1.head(25)) {
                System.out.println(handle2);
            }
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new BadExample()).execute(args);
        System.exit(execute);
    }
}
