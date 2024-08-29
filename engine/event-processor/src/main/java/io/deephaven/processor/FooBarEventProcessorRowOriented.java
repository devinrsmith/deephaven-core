package io.deephaven.processor;

import io.deephaven.processor.appender.Appender;
import io.deephaven.processor.appender.InstantAppender;
import io.deephaven.processor.appender.IntAppender;
import io.deephaven.processor.appender.ObjectAppender;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.OptionalLong;

public class FooBarEventProcessorRowOriented extends EventProcessorSimpleSingleStreamBase<String> {

    public FooBarEventProcessorRowOriented() {
        super(new StreamSpec() {
            @Override
            public List<Type<?>> outputTypes() {
                return List.of(Type.instantType(), Type.stringType(), Type.intType());
            }

            @Override
            public OptionalLong expectedSize() {
                return OptionalLong.of(10);
            }

            @Override
            public boolean isRowOriented() {
                return true;
            }
        });
    }

    @Override
    protected void processStream(String event, Stream stream) {
        final List<Appender> appenders = stream.appenders();
        final InstantAppender ts = InstantAppender.get(appenders.get(0));
        final ObjectAppender<String> name = ObjectAppender.get(appenders.get(1), Type.stringType());
        final IntAppender age = IntAppender.get(appenders.get(2));


        stream.advanceAll();
    }
}
