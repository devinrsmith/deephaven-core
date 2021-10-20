package io.deephaven.grpc_api.appmode;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.Field;
import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.console.GlobalSessionProvider;
import io.deephaven.uri.DeephavenUri;
import io.deephaven.uri.DeephavenUri.Visitor;
import io.deephaven.uri.DeephavenUriApplicationField;
import io.deephaven.uri.DeephavenUriField;
import io.deephaven.uri.DeephavenUriProxy;
import io.deephaven.uri.DeephavenUriQueryScope;
import io.deephaven.uri.TableResolver;

import javax.inject.Inject;
import java.util.Objects;

public final class LocalTableResolver implements TableResolver {

    private final GlobalSessionProvider globalSessionProvider;
    private final ApplicationStates states;

    @Inject
    public LocalTableResolver(GlobalSessionProvider globalSessionProvider, ApplicationStates states) {
        this.globalSessionProvider = Objects.requireNonNull(globalSessionProvider);
        this.states = Objects.requireNonNull(states);
    }

    @Override
    public boolean canResolve(DeephavenUri uri) {
        return uri.isLocal() && uri.walk(new CanResolve()).out();
    }

    @Override
    public Table resolve(DeephavenUri uri) throws InterruptedException {
        return uri.walk(new Resolver()).out();
    }

    public Table resolve(DeephavenUriQueryScope uri) {
        if (!uri.isLocal()) {
            throw new IllegalArgumentException("Can only resolve local URIs");
        }
        final String variableName = uri.variableName();
        final Object variable = globalSessionProvider.getGlobalSession().getVariable(variableName, null);
        return asTable(variable, "global query scope", variableName);
        // final Field<Object> field = states.getQueryScopeState().getField(queryScopeName);
        // if (field == null) {
        // return null;
        // }
        // return asTable(field, "global query scope", queryScopeName);
    }

    public Table resolve(DeephavenUriField uri) {
        if (!uri.isLocal()) {
            throw new IllegalArgumentException("Can only resolve local URIs");
        }
        return app(uri.applicationId(), uri.fieldName());
    }

    public Table resolve(DeephavenUriApplicationField uri) {
        if (!uri.isLocal()) {
            throw new IllegalArgumentException("Can only resolve local URIs");
        }
        return app(uri.applicationId(), uri.fieldName());
    }

    private Table app(String applicationId, String fieldName) {
        final ApplicationState state = states.getApplicationState(applicationId).orElse(null);
        if (state == null) {
            return null;
        }
        final Field<Object> field = state.getField(fieldName);
        if (field == null) {
            return null;
        }
        return asTable(field.value(), applicationId, fieldName);
    }

    private Table asTable(Object value, String context, String fieldName) {
        if (!(value instanceof Table)) {
            throw new IllegalArgumentException(
                    String.format("Field '%s' in '%s' is not a Table, is %s", fieldName, context, value.getClass()));
        }
        return (Table) value;
    }

    private static class CanResolve implements Visitor {

        private Boolean out;

        public boolean out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(DeephavenUriField field) {
            out = true;
        }

        @Override
        public void visit(DeephavenUriApplicationField applicationField) {
            out = true;
        }

        @Override
        public void visit(DeephavenUriQueryScope queryScope) {
            out = true;
        }

        @Override
        public void visit(DeephavenUriProxy proxy) {
            out = false;
        }
    }

    private class Resolver implements Visitor {

        private Table out;

        public Table out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(DeephavenUriField field) {
            out = resolve(field);
        }

        @Override
        public void visit(DeephavenUriApplicationField applicationField) {
            out = resolve(applicationField);
        }

        @Override
        public void visit(DeephavenUriQueryScope queryScope) {
            out = resolve(queryScope);
        }

        @Override
        public void visit(DeephavenUriProxy proxy) {
            throw new UnsupportedOperationException("Proxy not supported");
        }
    }
}
