package io.deephaven.client.impl;

import io.deephaven.DeephavenUriApplicationField;
import io.deephaven.DeephavenUriField;
import io.deephaven.DeephavenUriI;
import io.deephaven.DeephavenUriProxy;
import io.deephaven.DeephavenUriQueryScope;
import io.deephaven.db.tables.Table;

import java.util.Objects;

public class DeephavenUriLocal implements DeephavenUriI.Visitor {

    public static Table of(BarrageLocalTableResolver resolver, DeephavenUriI uri) {
        if (!uri.isLocal()) {
            throw new IllegalArgumentException("Should only be local URIs");
        }
        return uri.walk(new DeephavenUriLocal(resolver)).out();
    }

    private final BarrageLocalTableResolver resolver;

    private Table out;

    private DeephavenUriLocal(BarrageLocalTableResolver resolver) {
        this.resolver = Objects.requireNonNull(resolver);
    }

    public Table out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(DeephavenUriField field) {
        throw new IllegalStateException();
    }

    @Override
    public void visit(DeephavenUriApplicationField applicationField) {
        out = resolver.resolveApplicationField(applicationField.applicationId(), applicationField.fieldName());
    }

    @Override
    public void visit(DeephavenUriQueryScope queryScope) {
        out = resolver.resolveQueryScopeName(queryScope.variableName());
    }

    @Override
    public void visit(DeephavenUriProxy proxy) {
        throw new IllegalStateException();
    }
}
