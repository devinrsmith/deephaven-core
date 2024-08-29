package io.deephaven.processor.appender;

public abstract class ShortAppenderBase extends AppenderBase implements ShortAppender {

    @Override
    public final ShortAppender shortAppender() {
        return this;
    }
}
