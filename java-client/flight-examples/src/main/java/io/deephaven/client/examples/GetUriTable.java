package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.uri.RemoteUri;
import io.deephaven.uri.RemoteUriAdapter;
import io.deephaven.uri.StructuredUri;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.net.URI;

@Command(name = "get-uri-table", mixinStandardHelpOptions = true,
        description = "Get uri table",
        version = "0.1.0")
class GetUriTable extends FlightCannedTableBase {

    @Parameters(arity = "1", paramLabel = "URI", description = "The uri.")
    URI uri;

    private RemoteUri remoteUri() {
        return StructuredUri.of(uri).target(ConnectOptions.target(connectOptions));
    }

    @Override
    protected TableCreationLogic logic() {

        //return TicketTable.fromUri(uri).logic();
        //return EmptyTable.of(100).view("I=i").head(25).logic();
        //return TicketTable.fromQueryScopeField("currentTime").head(25).logic();

        return TicketTable.fromUri(uri).head(25).logic();
        // return UriTable.of(uri).head(25).logic();
        //return RemoteUriAdapter.of(remoteUri()).head(25).logic();
    }

    @Override
    protected void execute(FlightSession flight) throws Exception {
        System.out.printf("Remote URI: '%s'%n", remoteUri());
        super.execute(flight);
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new GetUriTable()).execute(args);
        System.exit(execute);
    }
}
