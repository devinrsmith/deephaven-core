package io.deephaven.client.impl;

import io.deephaven.DeephavenUriApplicationField;
import io.deephaven.DeephavenUriField;
import io.deephaven.DeephavenUriI;
import io.deephaven.DeephavenUriProxy;
import io.deephaven.DeephavenUriQueryScope;
import io.deephaven.qst.table.TicketTable;

import java.util.Objects;

public class DeephavenUriBarrage implements DeephavenUriI.Visitor {

    public static TicketTable of(DeephavenUriI uri) {
        return uri.walk(new DeephavenUriBarrage()).out();
    }

    private TicketTable out;

    public TicketTable out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(DeephavenUriField field) {
        out = TicketTable.fromApplicationField(field.applicationId(), field.fieldName());
    }

    @Override
    public void visit(DeephavenUriApplicationField applicationField) {
        out = TicketTable.fromApplicationField(applicationField.applicationId(), applicationField.fieldName());
    }

    @Override
    public void visit(DeephavenUriQueryScope queryScope) {
        out = TicketTable.fromQueryScopeField(queryScope.variableName());
    }

    @Override
    public void visit(DeephavenUriProxy proxy) {
        // out = TicketTable.of(proxy.path().toString());
        throw new UnsupportedOperationException("Proxy not supported yet");
    }
}
