package io.deephaven.uri;

import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.uri.StructuredUri.Visitor;

import java.net.URI;
import java.util.Objects;

public final class RemoteUriAdapter implements Visitor {

    public static TableSpec of(RemoteUri remoteUri) {
        return remoteUri.uri().walk(new RemoteUriAdapter(remoteUri.target())).out();
    }

    private final DeephavenTarget target;

    private TableSpec out;

    public RemoteUriAdapter(DeephavenTarget target) {
        this.target = Objects.requireNonNull(target);
    }

    public TableSpec out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(FieldUri fieldUri) {
        // out = TicketTable.fromApplicationField(target.host(), fieldUri.fieldName());
        out = TicketTable.of(String.format("u/dh:///app/%s/field/%s", target.host(), fieldUri.fieldName()));
    }

    @Override
    public void visit(ApplicationUri applicationField) {
        // out = TicketTable.fromApplicationField(applicationField.applicationId(), applicationField.fieldName());
        out = TicketTable.of(String.format("u/dh:///app/%s/field/%s", applicationField.applicationId(),
                applicationField.fieldName()));
    }

    @Override
    public void visit(QueryScopeUri queryScope) {
        // out = TicketTable.fromQueryScopeField(queryScope.variableName());

        out = TicketTable.of(String.format("u/dh:///scope/%s", queryScope.variableName()));
    }

    @Override
    public void visit(RemoteUri remoteUri) {
        // todo: consider a stronger type? Target + Uri
        out = TicketTable.fromUri(remoteUri.toURI());
    }

    @Override
    public void visit(URI customUri) {
        out = TicketTable.fromUri(customUri);
    }
}
