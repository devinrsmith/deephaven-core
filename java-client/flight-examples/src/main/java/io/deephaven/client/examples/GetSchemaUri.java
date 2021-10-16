package io.deephaven.client.examples;

import com.google.protobuf.ByteStringAccess;
import io.deephaven.DeephavenUri;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.proto.backplane.grpc.Ticket;
import org.apache.arrow.vector.types.pojo.Schema;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.time.Duration;

@Command(name = "get-schema-uri", mixinStandardHelpOptions = true,
        description = "Get the schema from a URI", version = "0.1.0")
class GetSchemaUri extends FlightExampleBase {

    @Parameters(arity = "1", paramLabel = "URI", description = "URI to subscribe to.",
            converter = DeephavenUriConverter.class)
    DeephavenUri uri;

    @Override
    protected String target() {
        return uri.host().get() + ":" + uri.port().orElse(10000);
    }

    @Override
    protected void execute(FlightSession flight) throws Exception {
        final long start = System.nanoTime();
        final long end;
        Schema schema = flight
                .schema(() -> Ticket.newBuilder().setTicket(ByteStringAccess.wrap(uri.ticket().ticket())).build());
        end = System.nanoTime();
        System.out.println(schema);
        System.out.printf("%s duration%n", Duration.ofNanos(end - start));
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new GetSchemaUri()).execute(args);
        System.exit(execute);
    }
}
