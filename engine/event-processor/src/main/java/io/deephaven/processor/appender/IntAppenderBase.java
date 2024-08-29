package io.deephaven.processor.appender;

public abstract class IntAppenderBase extends AppenderBase implements IntAppender {

    @Override
    public final IntAppender intAppender() {
        return this;
    }
}
