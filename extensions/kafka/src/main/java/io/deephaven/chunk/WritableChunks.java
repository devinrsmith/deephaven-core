package io.deephaven.chunk;

import java.util.List;

// todo: should we call this "writable"?
public interface WritableChunks {

    int pos();

    int size();

    List<WritableChunk<?>> out();

    default int remaining() {
        return size() - pos();
    }
}
