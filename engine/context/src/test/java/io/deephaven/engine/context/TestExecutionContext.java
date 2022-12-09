package io.deephaven.engine.context;

import io.deephaven.auth.AuthContext;

public class TestExecutionContext {
    public static ExecutionContext createForUnitTests() {
        return new ExecutionContext.Builder(new AuthContext.SuperUser())
                .markSystemic()
                .newQueryScope()
                .newQueryLibrary()
                .setQueryCompiler(QueryCompiler.createForUnitTests())
                .build();
    }
}
