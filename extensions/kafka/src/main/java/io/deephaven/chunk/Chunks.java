package io.deephaven.chunk;

import java.util.List;

public interface Chunks {

    int pos();

    int size();

    List<WritableChunk<?>> out();

    default int remaining() {
        return size() - pos();
    }
}
