package io.deephaven.processor;

import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.OptionalLong;

public interface StreamSpec {
    List<Type<?>> outputTypes();

    OptionalLong expectedSize();

    // if true, caller must use Stream.advanceAll() instead of advance()
    boolean isRowOriented();
}
