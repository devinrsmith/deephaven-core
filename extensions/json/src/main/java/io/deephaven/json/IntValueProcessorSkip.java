/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;

final class IntValueProcessorSkip extends ValueProcessorBase {

    IntValueProcessorSkip(String contextPrefix, boolean allowNull, boolean allowMissing) {
        super(contextPrefix, allowNull, allowMissing);
    }

    @Override
    public void handleValueNumberInt(JsonParser parser) {

    }

    @Override
    public void handleNull() {

    }

    @Override
    public void handleMissing() {

    }
}
