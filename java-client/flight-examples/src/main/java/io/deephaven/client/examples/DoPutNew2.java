package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.HasTicket;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.qst.array.GenericArray;
import io.deephaven.qst.array.IntArray;
import io.deephaven.qst.array.IntArray.Builder;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.type.Type;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.time.Instant;
import java.util.concurrent.ExecutionException;

@Command(name = "do-put-new", mixinStandardHelpOptions = true,
        description = "Do Put New", version = "0.1.0")
class DoPutNew2 extends FlightExampleBase {

    @Option(names = {"-e", "--efficient"}, description = "Use the more efficient version",
            defaultValue = "false")
    boolean efficient;

    @Option(names = {"-n", "--num-rows"}, description = "The number of rows to doPut",
            defaultValue = "1000")
    int numRows;

    @Parameters(arity = "1", paramLabel = "VAR", description = "Variable name to publish.")
    String variableName;

    @Override
    protected void execute(FlightSession flight) throws Exception {
        if (efficient) {
            moreEfficient(flight);
        } else {
            easy(flight);
        }
    }

    private NewTable newTable() {
        final Builder builder = IntArray.builder(numRows);

        final GenericArray.Builder<Instant> b2 = GenericArray.builder(Type.instantType());
        for (int i = 0; i < numRows; ++i) {
            builder.add(i);
            b2.add(Instant.now().plusSeconds(i));
        }
        return NewTable.of(Column.of("X", builder.build()), Column.of("Y", b2.build()));
    }

    private void publish(FlightSession flight, HasTicket ticket) throws InterruptedException, ExecutionException {
        flight.session().publish(variableName, ticket).get();
    }

    private void easy(FlightSession flight) throws Exception {
        // This version is "prettier", but uses one extra ticket and round trip
        try (final TableHandle destHandle = flight.put(newTable(), bufferAllocator)) {
            publish(flight, destHandle);
        }
    }

    private void moreEfficient(FlightSession flight) throws Exception {
        // This version is more efficient, but requires manual management of a ticket
        final Ticket ticket = flight.putTicket(newTable(), bufferAllocator);
        try {
            publish(flight, () -> ticket);
        } finally {
            flight.release(ticket);
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new DoPutNew2()).execute(args);
        System.exit(execute);
    }
}
