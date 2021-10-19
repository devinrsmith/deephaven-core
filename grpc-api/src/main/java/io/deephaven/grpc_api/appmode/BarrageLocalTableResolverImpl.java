package io.deephaven.grpc_api.appmode;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.Field;
import io.deephaven.client.impl.BarrageLocalTableResolver;
import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.console.GlobalSessionProvider;

import javax.inject.Inject;
import java.util.Objects;

public final class BarrageLocalTableResolverImpl implements BarrageLocalTableResolver {

    private final GlobalSessionProvider globalSessionProvider;
    private final ApplicationStates states;

    @Inject
    public BarrageLocalTableResolverImpl(GlobalSessionProvider globalSessionProvider, ApplicationStates states) {
        this.globalSessionProvider = Objects.requireNonNull(globalSessionProvider);
        this.states = Objects.requireNonNull(states);
    }

    @Override
    public Table resolveQueryScopeName(String queryScopeName) {
        final Object variable = globalSessionProvider.getGlobalSession().getVariable(queryScopeName, null);
        return asTable(variable, "global query scope", queryScopeName);
//        final Field<Object> field = states.getQueryScopeState().getField(queryScopeName);
//        if (field == null) {
//            return null;
//        }
//        return asTable(field, "global query scope", queryScopeName);
    }

    @Override
    public Table resolveApplicationField(String applicationId, String fieldName) {
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
}
