//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import io.deephaven.engine.testutil.QueryTableTestBase;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class DeephavenEngineExtension implements BeforeEachCallback, AfterEachCallback {

    private EngineState engineState;

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        engineState = new EngineState();
        engineState.setUp();
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        engineState.tearDown();
    }

    static class EngineState extends QueryTableTestBase {
    }
}
