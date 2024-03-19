//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.io.Closeable;
import java.util.List;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;

/**
 * An interface for processing data from one or more input objects into output chunks on a 1-to-1 input record to output
 * row basis.
 *
 * @param <T> the object type
 */
public interface ObjectProcessorToN<T> {

    interface SinkTx extends Closeable {

        DoubleConsumer appendingDoubleConsumer(int columnIx);

        IntConsumer appendingIntConsumer(int columnIx);

        PositionDoubleConsumer dc(int columnIx);

        PositionIntConsumer ic(int columnIx);

        // chunk compatible
        List<WritableChunk<?>> chunks(int size);


        void commit(int size);

        @Override
        void close();
    }

    interface PositionDoubleConsumer {

        void set(int position, double value);
    }

    interface PositionIntConsumer {

        void set(int position, int value);
    }

    interface SinkProvider {

        SinkTx tx();
    }


    default int size() {
        return outputTypes().size();
    }

    List<Type<?>> outputTypes();

    void processAll(ObjectChunk<? extends T, ?> in, SinkProvider provider);
}
