package io.deephaven.processor.appender;

public abstract class LongAppenderBase extends AppenderBase implements LongAppender {

    @Override
    public final LongAppender longAppender() {
        return this;
    }
}
