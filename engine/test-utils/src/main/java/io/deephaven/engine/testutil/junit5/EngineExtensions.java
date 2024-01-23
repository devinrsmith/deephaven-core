/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.testutil.junit5;

import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

/**
 * This is the JUnit 5 equivalent of {@link io.deephaven.engine.testutil.junit4.EngineCleanup}.
 */
public final class EngineExtensions implements BeforeTestExecutionCallback, AfterTestExecutionCallback {
    private static final Object KEY = new Object();

    private static class EngineState extends RefreshingTableTestCase {
    }

    @Override
    public void beforeTestExecution(ExtensionContext context) throws Exception {
        final EngineState state = new EngineState();
        state.setUp();
        store(context).put(KEY, state);
    }

    @Override
    public void afterTestExecution(ExtensionContext context) throws Exception {
        final EngineState state = store(context).remove(KEY, EngineState.class);
        state.tearDown();
    }

    private Store store(ExtensionContext context) {
        return context.getStore(namespace(context));
    }

    private Namespace namespace(ExtensionContext context) {
        return Namespace.create(getClass(), context.getRequiredTestMethod());
    }
}
