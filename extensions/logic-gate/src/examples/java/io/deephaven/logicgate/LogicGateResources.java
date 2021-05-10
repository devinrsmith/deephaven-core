package io.deephaven.logicgate;

import io.deephaven.extensions.Resource;
import io.deephaven.extensions.ResourceProvider;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public final class LogicGateResources implements ResourceProvider {

    // todo: we want to reference these as text files / not include the compilation
    private static final List<String> EXAMPLES = Arrays.asList(
        "/io/deephaven/logicgate/4-bit-adder.class", "/io/deephaven/logicgate/64-bit-adder.class",
        "/io/deephaven/logicgate/nand-single.class");

    @Override
    public final Stream<Resource> getResources() {
        return EXAMPLES.stream().map(Resource::of);
    }
}
