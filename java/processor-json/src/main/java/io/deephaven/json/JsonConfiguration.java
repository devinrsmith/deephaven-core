/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;

public interface JsonConfiguration {
    ObjectProcessor.Provider processorProvider(ValueOptions options);

    NamedObjectProcessor.Provider namedProvider(ValueOptions options);
}
