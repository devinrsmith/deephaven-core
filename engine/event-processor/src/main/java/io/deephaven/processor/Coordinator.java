package io.deephaven.processor;

public interface Coordinator {

    // implementation marking they are in a consistent state
    void sync();
}
