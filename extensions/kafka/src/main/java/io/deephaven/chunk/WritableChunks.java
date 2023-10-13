package io.deephaven.chunk;

import io.deephaven.util.SafeCloseable;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

// todo: should we call this "writable"?
public interface WritableChunks {

    static void closeAll(Collection<? extends WritableChunks> chunks) {
        closeAll(chunks.stream());
    }

    static void closeAll(Stream<? extends WritableChunks> chunks) {
        SafeCloseable.closeAll(chunks.map(WritableChunks::out).flatMap(Collection::stream));
    }

    int pos();

    int size();

    List<WritableChunk<?>> out();

    default int remaining() {
        return size() - pos();
    }
}
