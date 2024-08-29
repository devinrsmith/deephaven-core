package io.deephaven.processor.appender;

import io.deephaven.qst.type.GenericType;

import java.util.Objects;

public abstract class ObjectAppenderBase<T> extends AppenderBase implements ObjectAppender<T> {

    private final GenericType<T> type;

    public ObjectAppenderBase(GenericType<T> type) {
        this.type = Objects.requireNonNull(type);
    }

    @Override
    public final GenericType<T> type() {
        return type;
    }
}
