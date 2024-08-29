package io.deephaven.processor.appender;

import io.deephaven.qst.type.Type;

public interface Appender {

    Type<?> type();
}
