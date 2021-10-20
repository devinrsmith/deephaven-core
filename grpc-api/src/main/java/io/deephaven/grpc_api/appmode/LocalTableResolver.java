package io.deephaven.grpc_api.appmode;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.Field;
import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.console.GlobalSessionProvider;
import io.deephaven.uri.LocalApplicationUri;
import io.deephaven.uri.LocalFieldUri;
import io.deephaven.uri.LocalQueryScopeUri;
import io.deephaven.uri.LocalUri;
import io.deephaven.uri.RemoteUri;
import io.deephaven.uri.ResolvableUri;
import io.deephaven.uri.TableResolver;

import javax.inject.Inject;
import java.net.URI;
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
    public boolean canResolve(ResolvableUri uri) {
        return uri.walk(new CanResolve()).out();
    }

    @Override
    public Table resolve(ResolvableUri uri) {
        return uri.walk(new Resolver()).out();
    }

    public Table resolve(LocalQueryScopeUri uri) {
        final String variableName = uri.variableName();
        final Object variable = globalSessionProvider.getGlobalSession().getVariable(variableName, null);
        return asTable(variable, "global query scope", variableName);
        // final Field<Object> field = states.getQueryScopeState().getField(queryScopeName);
        // if (field == null) {
        // return null;
        // }
        // return asTable(field, "global query scope", queryScopeName);
    }

    public Table resolve(LocalApplicationUri uri) {
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

    private static class CanResolve implements ResolvableUri.Visitor, LocalUri.Visitor {

        private Boolean out;

        public boolean out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(LocalUri localUri) {
            localUri.walk((LocalUri.Visitor) this);
        }

        @Override
        public void visit(RemoteUri remoteUri) {
            out = false;
        }

        @Override
        public void visit(URI uri) {
            out = false;
        }

        @Override
        public void visit(LocalFieldUri fieldUri) {
            out = false; // must be paired w/ application id, or new server logic
        }

        @Override
        public void visit(LocalApplicationUri applicationField) {
            out = true;
        }

        @Override
        public void visit(LocalQueryScopeUri queryScope) {
            out = true;
        }
    }

    private class Resolver implements ResolvableUri.Visitor, LocalUri.Visitor {

        private Table out;

        public Table out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(LocalUri localUri) {
            localUri.walk((LocalUri.Visitor) this);
        }

        @Override
        public void visit(RemoteUri remoteUri) {
            throw new UnsupportedOperationException(String.format("Unable to resolve '%s'", remoteUri));
        }

        @Override
        public void visit(URI uri) {
            throw new UnsupportedOperationException(String.format("Unable to resolve '%s'", uri));
        }

        @Override
        public void visit(LocalFieldUri fieldUri) {
            throw new UnsupportedOperationException(String.format("Unable to resolve '%s'", fieldUri));
        }

        @Override
        public void visit(LocalApplicationUri applicationField) {
            out = resolve(applicationField);
        }

        @Override
        public void visit(LocalQueryScopeUri queryScope) {
            out = resolve(queryScope);
        }
    }
}
