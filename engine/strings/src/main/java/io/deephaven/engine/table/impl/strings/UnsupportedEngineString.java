package io.deephaven.engine.table.impl.strings;

public final class UnsupportedEngineString extends UnsupportedOperationException {
    UnsupportedEngineString(Object object) {
        super("Unsupported engine string from " + object);
    }
}
