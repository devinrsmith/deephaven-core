package io.deephaven.client.examples;

import io.deephaven.client.impl.ApplicationFieldId;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.flight.FlightStream;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "query-app-fields", mixinStandardHelpOptions = true, description = "Query application fields",
        version = "0.1.0")
class QueryApplicationFields extends FlightExampleBase {

    @Option(names = {"--app-id"}, required = false, description = "The application id.")
    String applicationId;

    @Option(names = {"--app-field"}, required = false, description = "The field name.")
    String fieldName;

    private TableSpec spec() {
        // todo: make this a proper app dependency for client access
        TableSpec spec = new ApplicationFieldId("io.deephaven.server.appmode.ApplicationApp", "application.fields")
                .ticketId()
                .table();
        if (applicationId != null) {
            spec = spec.where(String.format("Id=`%s`", applicationId));
        }
        if (fieldName != null) {
            spec = spec.where(String.format("Field=`%s`", fieldName));
        }
        return spec;
    }

    @Override
    protected void execute(FlightSession flight) throws Exception {
        try (
                final TableHandle handle = flight.session().batch().execute(spec());
                final FlightStream stream = flight.stream(handle)) {
            while (stream.next()) {
                System.out.println(stream.getRoot().contentToTSVString());
            }
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new QueryApplicationFields()).execute(args);
        System.exit(execute);
    }
}
