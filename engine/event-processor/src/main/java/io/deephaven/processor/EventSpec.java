package io.deephaven.processor;

import java.util.List;

public interface EventSpec {
    default int numStreams() {
        return streams().size();
    }

    List<StreamSpec> streams();

    boolean usesCoordinator(); // todo

//    boolean isRowOriented(); // if true, callers must use
}
